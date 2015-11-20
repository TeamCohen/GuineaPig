import getopt
import sys
import os
import logging
import threading
import collections
import subprocess
import time
import Queue
import shutil
import urllib
import time
import traceback
import cStringIO
import select
import fcntl

#
# still blocking on reads from pipe....maybe I need to go to the
# os.read, os.write model....write the full string by bufsize, and
# read by buf size, and worry about the splitting later..

##############################################################################
# Map Reduce Streaming for GuineaPig (mrs_gp) - very simple
# multi-threading map-reduce, to be used with inputs/outputs which are
# just files in directories.  This combines multi-threading and
# multi-processing.
#
# To do:
#  map-only tasks, multiple map inputs
#  secondary grouping sort key --joinmode
#
# Gotchas/bugs:
#   - threading doesn't work right in jython, and doesn't help
#   much in cpython.  best interpreter is pypy, due to lack
#   of GIL.
#  - if you use /afs/ as file store the server seems to 
#   leave some sort of lock file around which make deletion
#   impossible while the server is running
#
# Usage:
#  
#  1) Start a server:
#
#  pypy mrs_gp.py --serve   #won't return till you shut it down
#
#  2) Run some map-reduce commands.  These are submitted to the
#  server process and run there.
#
#  pypy mrs_gp.py --task --input DIR --output DIR \
#                        --mapper FOO --reducer BAR --numReduceTasks K
#
#  This acts pretty much like a hadoop streaming command: the mapper
#  and reducer use the same API, but the directories are not HDFS
#  locations, just some directory on you local FS.
#
#  Reducers are optional, if they are not present it will be a
#  map-only task.
#
#  DIR could also be "GPFileSystem" directory, specified by the prefix
#  gpfs: These are NOT hierarchical and are just stored in memory by
#  the server.  The files in a directory are always shards of a
#  map-reduce computation.
#  
#  If you want to examine the gpfs: files, you can use a browser on
#  http://localhost:1969/XXX where XXX is a cgi-style query.  For
#  instance, http://localhost:1969/ls will list the directories, and
#  http://localhost:1969/getmerge?dir=foo will return the contents of
#  all shards in the directory foo.  Other commands are ls?dir=X,
#  cat?dir=X&file=Y, and head and tail which are like cat but also
#  have an argument "n".
#
#  Or, you can use 'pypy mrs_gp.py --fs XXX' instead.
#
# 3) Shut down the server, discarding everything held by the
# GPFileSystem.
#
#  pypy mrs_gp.py --shutdown
#
#
##############################################################################

# utility to format large file sizes readably

def fmtchars(n):
    """"Format a large number of characters by also including the
    equivalent size in megabytes."""
    mb = n/(1024*1024.0)
    return "%d(%.1fM)" % (n,mb)

def makePipe(shellCom):
    p = subprocess.Popen(shellCom,shell=True, bufsize=4096,
                         stdin=subprocess.PIPE,stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    # make stderr, stdout no-blocking
    def unblock(fp):
        flags = fcntl.fcntl(fp, fcntl.F_GETFL) # get current p.stdout flags
        fcntl.fcntl(fp, fcntl.F_SETFL, flags | os.O_NONBLOCK)
    unblock(p.stdout)
    unblock(p.stderr)
    return p

##############################################################################
#
# shared "file system"
#
##############################################################################

class GPFileSystem(object):
    """Very simple in-memory file system.  The system is two-level -
    directories and files - not heirarchical.  Files are specified by
    a directory, file pair.  The directory can optionally be prefixed
    by the string 'gpfs:'.
    """
    # these are prefixes for pretty-printed directory listing, used
    # before the number of files in a directory or number of chars in
    # a file.
    FILES_MARKER = ' files: '
    CHARS_MARKER = ' chars: '

    def __init__(self):
        #file names in directory/shards
        self.filesIn = collections.defaultdict(list)
        #content and size of dir/file - indexed by (dir,file)
        self.contentOf = {}
        self.sizeOf = {}

    def rmDir(self,d0):
        """Clear a directory and all files below it."""
        d = self._fixDir(d0)
        if d in self.filesIn:
            for f in self.filesIn[d]:
                del self.contentOf[(d,f)]
            del self.filesIn[d]

    def write(self,d0,f,line):
        """Write to the end of the file d0/f"""
        d = self._fixDir(d0)
        if not f in self.filesIn[d]:
            self.filesIn[d].append(f)
            self.contentOf[(d,f)] = cStringIO.StringIO()
            self.sizeOf[(d,f)] = 0
        self.contentOf[(d,f)].write(line)
        self.sizeOf[(d,f)] += len(line)

    def listDirs(self,pretty=False):
        """Return a list of names of directories in the file system.  If
        pretty=True, give a a list for each directory with a little more
        information, sort of like the output of ls -l.
        """
        result = sorted(self.filesIn.keys())
        if not pretty: 
            return result
        else:
            def fmtdir(d): return '%s%3d  %s' % (GPFileSystem.FILES_MARKER,len(FS.listFiles(d)),d)
            return map(fmtdir,result)

    def listFiles(self,d0,pretty=False):
        """Return a list of names of files in a directory.  If pretty=True,
        give a a list for each directory with a little more
        information, sort of like the output of ls -l.
        """
        d = self._fixDir(d0)
        result = sorted(self.filesIn[d])
        if not pretty: 
            return result
        else:
            def fmtfile(f): return '%s%s  %s/%s' % (GPFileSystem.CHARS_MARKER,fmtchars(self.size(d,f)),d,f)
            return map(fmtfile,result)

    def cat(self,d0,f):
        """Return the contents of a file."""
        d = self._fixDir(d0)
        return self.contentOf[(d,f)].getvalue()

    def size(self,d0,f):
        """Return the size of a file."""
        d = self._fixDir(d0)
        return self.sizeOf[(d,f)]

    def totalSize(self):
        """Return the size of a file."""
        return sum(self.sizeOf.values())

    def head(self,d0,f,n):
        """Return the first n characters of a file."""
        d = self._fixDir(d0)
        return self.contentOf[(d,f)].getvalue()[:n]

    def tail(self,d0,f,n):
        """Return the last n characters of a file."""
        d = self._fixDir(d0)
        return self.contentOf[(d,f)].getvalue()[-n:]

    def __str__(self):
        return "FS("+str(self.filesIn)+";"+str(self.contentOf)+")"

    def _fixDir(self,d):
        """Strip the prefix gpfs: if it is present."""
        return d if not d.startswith("gpfs:") else d[len("gpfs:"):]

# global file system used by map-reduce system

FS = GPFileSystem()

##############################################################################
# main map-reduce algorithm(s)
#
# maponly is very simple: it sets up K independent mapper processes,
# one for each shard, which read from that shard and write to the
# corresponding output shard.  If gpfs is used, the reading and
# writing is from threads which write/read from the appropriate
# subprocess stdin or stdout.
#
# mapreduce is a little more complex.  There are K reducer Queue's,
# each of regulate access to the data that will be fed to a single
# reducer.  Since this data is going to be sorted by key, the data is
# collected in a 'reducerBuffer', which maps keys to values (in
# memory).  The output of every mapper is collected by a thread
# running 'shuffleMapOutputs'.  This buffers ALL the map output by
# shard, and then sends each shard to the approproate reducer queue.
# Each queue is monitored by a queue-specific thread which adds
# map-output sharded data to the reducerBuffer for that shard.
#
# actions are tracked by a global TaskStats object.
#
##############################################################################

class TaskStats(object):

    def __init__(self,opdict):
        self.opts = opdict.copy()
        self.startTime = {}
        self.endTime = {}
        self.inputSize = {}
        self.outputSize = {}
        self.logSize = {}
        self.numStarted = {'mapper':0, 'reducer':0, 'shuffle':0}
        self.numFinished = {'mapper':0, 'reducer':0, 'shuffle':0}

    def start(self,msg):
        """Start timing something."""
        self.startTime[msg] = time.time()
        logging.info('started  '+msg)

    def end(self,msg):
        """End timing something."""
        self.endTime[msg] = time.time()
        logging.info('finished '+msg + ' in %.3f sec' % (self.endTime[msg]-self.startTime[msg]))

    def report(self,includeLogs=True):
        """Return a report on the current/last-finished task, encoded as a list of human-readable strings."""
        global FS
        buf = ['Options:']
        for k in sorted(self.opts.keys()):
            buf.append(' %-40s : %s' % (k,self.opts[k]))
        buf.extend(['Statistics:'])
        for k in sorted(self.numStarted.keys()):
            s = self.numStarted[k]
            f = self.numFinished[k]
            progressBar = '[*]'
            if s>0:
                progressBar = "progress [" + ("#"*f) + ("."*(s-f))+"]"
            buf.extend([' %-7s summary: %d/%d finished/started %s' % (k,self.numFinished[k],self.numStarted[k],progressBar)])
        now = time.time()
        for k in sorted(self.startTime.keys()):
            if k in self.endTime:
                line = ' %-40s: finished in %.3f sec' % (k,self.endTime[k]-self.startTime[k])
                if k in self.inputSize: line += ' chars: input %s' % fmtchars(self.inputSize[k])
                if k in self.outputSize: line += ' output %s' % fmtchars(self.outputSize[k])
                if k in self.logSize: line += ' log %d' % self.logSize[k]
            else:
                line = ' %-40s: running for %.3f sec' % (k,now-self.startTime[k])
                if k in self.inputSize: line += ' input %s' % fmtchars(self.inputSize[k])
            buf.append(line)
        if includeLogs:
            buf.extend(['Subprocess Logs:'])
            buf.extend(FS.listFiles("_logs",pretty=True))
        return buf

# a global object to track the current task
TASK_STATS = TaskStats({'ERROR':'no tasks started yet'})

# prevent multiple tasks from happening at the same time
TASK_LOCK = threading.Lock()

def performTask(optdict):
    """Utility that calls mapreduce or maponly, as appropriate, based on
    the options, and logs a bunch of statistics on this."""

    TASK_LOCK.acquire()  #since the task stats are global
    try:
        # maintain some statistics for this task in TASKS and the FS
        global TASK_STATS
        global FS
        TASK_STATS = TaskStats(optdict)
        
        TASK_STATS.start('__top level task')
        FS.rmDir("gpfs:_logs")

        #start parsing options and performing actions...
        indir = optdict['--input']
        outdir = optdict['--output']
        if '--reducer' in optdict:
            #usage 1: a basic map-reduce has --input, --output, --mapper, --reducer, and --numReduceTasks
            mapper = optdict.get('--mapper','cat')
            reducer = optdict.get('--reducer','cat')
            numReduceTasks = int(optdict.get('--numReduceTasks','1'))
            mapreduce(indir,outdir,mapper,reducer,numReduceTasks)
        else:
            #usage 1: a map-only task has --input, --output, --mapper
            mapper = optdict.get('--mapper','cat')
            maponly(indir,outdir,mapper)        
        TASK_STATS.end('__top level task')
        FS.write("_history",time.strftime("task-%Y.%m.%H.%M.%S"),"\n".join(TASK_STATS.report(includeLogs=False)))

    finally:
        TASK_LOCK.release()

def mapreduce(indir,outdir,mapper,reducer,numReduceTasks):
    """Run a generic streaming map-reduce process.  The mapper and reducer
    are shell commands, as in Hadoop streaming.  Both indir and outdir
    are directories."""

    usingGPFS,infiles = setupFiles(indir,outdir)
    global TASK_STATS

    # Set up a place to save the inputs to K reducers - each of which
    # is a buffer bj, which contains lines for shard K,

    TASK_STATS.start('_init reduce buffer queues')
    reducerQs = []        # task queues to join with later 
    reducerBuffers = []
    for j in range(numReduceTasks):
        qj = Queue.Queue()
        reducerQs.append(qj)
        bj = cStringIO.StringIO()
        reducerBuffers.append(bj)
        tj = threading.Thread(target=acceptReduceInputs, args=(qj,bj))
        tj.daemon = True
        tj.start()
    TASK_STATS.end('_init reduce buffer queues')

    # start the mappers - along with threads to shuffle their outputs
    # to the appropriate reducer task queue

    TASK_STATS.start('_init mappers and shufflers')
    mappers = []
    for fi in infiles:
        # WARNING: it doesn't seem to work well to start the processes
        # inside a thread - this led to bugs with the reducer
        # processes.  This is possibly a python library bug:
        # http://bugs.python.org/issue1404925
        mapPipeI = makePipe(mapper)
        si = threading.Thread(target=shuffleMapOutputs, args=(usingGPFS,numReduceTasks,mapPipeI,indir,fi,reducerQs))
        si.start()                      # si will join the mapPipe process
        mappers.append(si)
    TASK_STATS.end('_init mappers and shufflers')

    #wait for the map tasks, and to empty the queues
    TASK_STATS.start('_join mappers')
    joinAll(mappers,'mappers')        
    TASK_STATS.end('_join mappers')

    TASK_STATS.start('_join reducer queues')
    joinAll(reducerQs,'reduce queues')
    TASK_STATS.end('_join reducer queues')

    # run the reduce processes, each of which is associated with a
    # thread that feeds it inputs from the j's reduce buffer.

    TASK_STATS.start('_init reducers')
    reducers = []
    for j in range(numReduceTasks):    
        reducePipeJ = makePipe("sort -k1 | "+reducer)
        # thread to feed data into the reducer process
        uj = threading.Thread(target=runReducer, 
                              args=(usingGPFS,reducePipeJ,reducerBuffers[j],("part%05d" % j),outdir))
        uj.start()                      # uj will shut down reducePipeJ process on completion
        reducers.append(uj)
    TASK_STATS.end('_init reducers')

    #wait for the reduce tasks
    TASK_STATS.start('_join reducers')
    joinAll(reducers,'reducers')
    TASK_STATS.end('_join reducers')

def maponly(indir,outdir,mapper):
    """Like mapreduce but for a mapper-only process."""

    usingGPFS,infiles = setupFiles(indir,outdir)
    global TASK_STATS

    # start the mappers - each of which is a process that reads from
    # an input file, and outputs to the corresponding output file

    start = time.time()
    numMapTasks = len(infiles)
    mapTags = map(
        lambda i:'mapper-from-%s-%s' % (indir,infiles[i]),
        range(numMapTasks))
    mapPipes = map(
        lambda i:makePipe(mapper), 
        range(numMapTasks))
    mapThreads = map(
        lambda i:threading.Thread(target=runPipe, 
                                  args=(mapTags[i],mapPipes[i],
                                        usingGPFS,indir,infiles[i],outdir,infiles[i])),
        range(numMapTasks))
    TASK_STATS.start('_running mappers')
    for t in mapThreads: t.start()
    for t in mapThreads: t.join()
    TASK_STATS.end('_running mappers')

def runPipe(tag,pipe,usingGPFS,indir,infile,outdir,outfile):

    activeInputs = [pipe.stdin]
    activeOutputFPs = {pipe.stdout:'stdout',pipe.stderr:'stderr'}
    TIMEOUT = 5
    BUFSIZ = 2048
    inbuf = getInput(usingGPFS,indir,infile)
    inbufPtr = 0
    result['stdout'] = cStringIO.StringIO()
    result['stderr'] = cStringIO.StringIO()

    global TASK_STATS
    TASK_STATS.start(tag)
    while True:
#        print 'stdin',inbufPtr,'stdout',len(result['stdout'].getvalue()), \
#             'stderr',len(result['stderr'].getvalue())

        readable,writeable,exceptional = \
            select.select(activeOutputFPs.keys(), activeInputs, activeOutputFPs.keys()+activeInputs, 0)
        assert not exceptional,'exceptional files + %r' % exceptional

        print readable,writeable

        progress = False
        for fp in readable:
            print '-',
            tmp = os.read(fp.fileno(), BUFSIZ)
            if len(tmp) > 0:
                key = activeOutputFPs[fp]
                result[key].write(tmp)                
                progress = True
            else:
                fp.close()
                del activeOutputFPs[fp]

        if writeable:
            #singleton list
            hi = inbufPtr+BUFSIZ
            if hi>len(inbuf): hi=len(inbuf)
            print '+',hi,len(inbuf)
            n = os.write(writeable[0].fileno(), inbuf[inbufPtr:hi])
            #print 'w',n
            if n>0:
                inbufPtr += n
                progress = True
            if n==0 or inbufPtr>=len(inbuf):
                pipe.stdin.close()
                activeInputs = []

        if progress:
            print '.',
            pass
        elif pipe.poll()!=None:
            return
        else:
            print '?..',
            time.sleep(0.001)

    #finished the loop
    TASK_STATS.end(tag)
    putOutput(usingGPFS,outdir,infile,result['stderr'])

#
# routines attached to threads
#

def runMapper(usingGPFS,mapPipe,indir,f,outdir):
    inputString=getInput(usingGPFS,indir,f)
    output = logCommunication(
        mapPipe,inputString,
        'mapper-from-%s/%s to %s/%s' % (indir,f,outdir,f))
    putOutput(usingGPFS,outdir,f,output)

def runReducer(usingGPFS,redPipe,buf,f,outdir):
    output = logCommunication(
        redPipe,buf.getvalue(),
        'reducer-to-%s/%s' % (outdir,f))
    putOutput(usingGPFS,outdir,f,output)

def shuffleMapOutputs(usingGPFS,numReduceTasks,mapPipe,indir,f,reducerQs):
    """Thread that takes outputs of a map pipeline, hashes them, and
    sends them in the appropriate reduce queue."""

    #run the mapper
    output = logCommunication(
        mapPipe, getInput(usingGPFS,indir,f),
        'mapper-from-%s/%s' % (indir,f))

    global TASK_STATS
    TASK_STATS.start('shuffle from %s/%s' % (indir,f))
    #buffer[h] will hold the part of this mapper's output that will be
    #sent to reducer h
    TASK_STATS.numStarted['shuffle'] += 1
    buffers = []
    for h in range(numReduceTasks):
        buffers.append(cStringIO.StringIO())
    #add each output line to the appropriate buffer
    for line in cStringIO.StringIO(output):
        k = key(line)
        h = hash(k) % numReduceTasks    
        buffers[h].write(line)
    #queue up the buffer to send to the appropriate reducer
    for h in range(numReduceTasks):    
        if buffers[h]: reducerQs[h].put(buffers[h].getvalue())
    TASK_STATS.numFinished['shuffle'] += 1
    TASK_STATS.end('shuffle from %s/%s' % (indir,f))

def acceptReduceInputs(reducerQ,reducerBuf):
    """Daemon thread that monitors a queue of items to add to a reducer
    input buffer."""
    while True:
        line = reducerQ.get()
        reducerBuf.write(line)
        reducerQ.task_done()
#
# subroutines for map-reduce
#

def logCommunication(pipe,inputString,pipeTag):
    """Wraps a pipe.communicate() call with a bunch of logging chores."""
    global TASK_STATS
    global FS

    #record start
    TASK_STATS.start(pipeTag)
    for k in TASK_STATS.numStarted.keys():
        if pipeTag.startswith(k): 
            TASK_STATS.numStarted[k] += 1

    #the actual work being done: send the input string to the pipe
    #process, execute it, and return stdout and stderr results
    TASK_STATS.inputSize[pipeTag] = len(inputString)
    #(output,log) = pipe.communicate(input=inputString)    
    result = {}
    t = threading.Thread(target=_communicate, args=(pipe,inputString,result))
    t.start()
    t.join()
    (output,log) = (result['stdout'].getvalue(),result['stderr'].getvalue())

    #record completion
    TASK_STATS.end(pipeTag)
    for k in TASK_STATS.numStarted.keys():
        if pipeTag.startswith(k): 
            TASK_STATS.numFinished[k] += 1

    #save statistics and logs
    TASK_STATS.outputSize[pipeTag] = len(output)
    if not log: log=''
    TASK_STATS.logSize[pipeTag] = len(log)
    FS.write("gpfs:_logs",pipeTag,log)

    #could also have been an error starting up the process
    if pipe.returncode:
        msg = "%s failed to start - return code %d" % (pipeTag,pipe.returncode)
        logging.warn(msg)
        FS.write("gpfs:_logs",pipeTag,msg)

    #finally return stdout from pipe process
    return output

def _communicate(pipe,inbuf,result):

    activeInputs = [pipe.stdin]
    activeOutputFPs = {pipe.stdout:'stdout',pipe.stderr:'stderr'}
    TIMEOUT = 5
    BUFSIZ = 2048
    inbufPtr = 0
    result['stdout'] = cStringIO.StringIO()
    result['stderr'] = cStringIO.StringIO()

    while True:
#        print 'stdin',inbufPtr,'stdout',len(result['stdout'].getvalue()), \
#             'stderr',len(result['stderr'].getvalue())

        readable,writeable,exceptional = \
            select.select(activeOutputFPs.keys(), activeInputs, activeOutputFPs.keys()+activeInputs, 0)
        assert not exceptional,'exceptional files + %r' % exceptional

        print readable,writeable

        progress = False
        for fp in readable:
            print '-',
            tmp = os.read(fp.fileno(), BUFSIZ)
            if len(tmp) > 0:
                key = activeOutputFPs[fp]
                result[key].write(tmp)                
                progress = True
            else:
                fp.close()
                del activeOutputFPs[fp]

        if writeable:
            #singleton list
            hi = inbufPtr+BUFSIZ
            if hi>len(inbuf): hi=len(inbuf)
            print '+',hi,len(inbuf)
            n = os.write(writeable[0].fileno(), inbuf[inbufPtr:hi])
            #print 'w',n
            if n>0:
                inbufPtr += n
                progress = True
            if n==0 or inbufPtr>=len(inbuf):
                pipe.stdin.close()
                activeInputs = []

        if progress:
            print '.',
            pass
        elif pipe.poll()!=None:
            return
        else:
            print '?..',
            time.sleep(0.001)
                    

def setupFiles(indir,outdir):
    """Work out what the input files are, and clear the output directory,
    if needed.  Returns a (usingGPFS,files) where the set usingGPFS
    contains 'input' if the input is on FS, and 'output' if the output
    is on the FS'; and files is a list of input files.
    """
    usingGPFS = set()
    if indir.startswith("gpfs:"):
        usingGPFS.add('input')
        infiles = FS.listFiles(indir)
    else:
        infiles = [f for f in os.listdir(indir)]
    if outdir.startswith("gpfs:"):
        usingGPFS.add('output')
        FS.rmDir(outdir)
    else:
        if os.path.exists(outdir):
            logging.warn('removing %s' % (outdir))
            shutil.rmtree(outdir)
        os.makedirs(outdir)
    logging.info('inputs: %d files from %s' % (len(infiles),indir))
    return usingGPFS,infiles

def getInput(usingGPFS,indir,f):
    """Return the content of the input file at indir/f"""
    if 'input' in usingGPFS:
        return FS.cat(indir,f)
    else:
        logging.debug('loading lines from '+indir+"/"+f)
        inputString = cStringIO.StringIO()
        k = 0
        for line in open(indir+"/"+f):
            inputString.write(line)
            k += 1
            if k%10000==0: logging.debug('reading %d lines from file %s/%s' % (k,indir,f))
        logging.debug('finished transferring from '+indir+"/"+f)
        return inputString.getvalue()

def putOutput(usingGPFS,outdir,f,outputString):
    """Store the output string in outdir/f"""
    if 'output' in usingGPFS:
        FS.write(outdir,f,outputString)
    else:
        fp = open(outdir+"/"+f, 'w')
        fp.write(outputString)
        fp.close()

def key(line):
    """Extract the key for a line containing a tab-separated key,value pair."""
    return line[:line.find("\t")]

def joinAll(xs,msg):
    """Utility to join with all threads/queues in a list."""
    logging.debug('joining ' + str(len(xs))+' '+msg)
    for i,x in enumerate(xs):
        x.join()
    logging.debug('joined all '+msg)


##############################################################################
# server/client stuff
##############################################################################

# server

from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
from SocketServer import ThreadingMixIn
import urlparse

keepRunning = True

class MRSHandler(BaseHTTPRequestHandler):
    
    def _sendList(self,items,html):
        """Send a list of items as a viewable file."""
        if not html:
            self._sendFile("\n".join(items) + "\n", False)
        else:
            # add a whole bunch of annotations if this is for a browser....
            self._sendHtmlHeader()
            self.wfile.write("<pre>\n")
            for it in items:
                self.wfile.write(self._addMarkup(it) + "\n")
            self.wfile.write("</pre>\n")
            self._sendHtmlFooter()

    def _sendFile(self,text,html):
        """Send an entire file."""
        if not html:
            self.send_response(200)
            self.send_header('Content-type','text/plain')
            self.end_headers()
            self.wfile.write(text)
        else:
            self._sendHtmlHeader()
            self.wfile.write("<pre>\n")
            self.wfile.write(text)
            self.wfile.write("</pre>\n")
            self._sendHtmlFooter()
    
    # turn off request logging to stderr
    def log_request(self,code=0,size=0):
        pass

    def do_GET(self):
        """Handle a request."""
        global keepRunning
        global TASK_STATS
        try:
            p = urlparse.urlparse(self.path)
            requestOp = p.path
            requestArgs = urlparse.parse_qs(p.query)
            #convert the dict of lists to a dict of items, since I
            # don't use multiple values for any key
            requestArgs = dict(map(lambda (key,valueList):(key,valueList[0]), requestArgs.items()))
            # indicates if I want a browser-ready output, or plain
            # text
            html = 'html' in requestArgs
            if requestOp=="/shutdown":
                keepRunning = False
                self._sendFile("Shutting down...\n",html)
            elif requestOp=="/ls" and not 'dir' in requestArgs:
                self._sendList(FS.listDirs(pretty=True),html)
            elif requestOp=="/ls" and 'dir' in requestArgs:
                d = requestArgs['dir']
                self._sendList(FS.listFiles(d,pretty=True),html)
            elif requestOp=="/getmerge" and 'dir' in requestArgs:
                d = requestArgs['dir']
                buf = "".join(map(lambda f:FS.cat(d,f),FS.listFiles(d)))
                self._sendFile(buf,html)
            elif requestOp=="/write":
                d = requestArgs['dir']
                f = requestArgs['file']
                line = requestArgs['line']
                FS.write(d,f,line+'\n')
                self._sendFile("Line '%s' written to %s/%s" % (line,d,f), html)
            elif requestOp=="/cat":
                d = requestArgs['dir']
                f = requestArgs['file']
                content = FS.cat(d,f)
                self._sendFile(content,html)
            elif requestOp=="/head":
                d = requestArgs['dir']
                f = requestArgs['file']
                n = int(requestArgs.get('n','2048'))
                self._sendFile(FS.head(d,f,n),html)
            elif requestOp=="/tail":
                d = requestArgs['dir']
                f = requestArgs['file']
                n = int(requestArgs.get('n','2048'))
                self._sendFile(FS.tail(d,f,n),html)
            elif requestOp=="/report":
                self._sendList(TASK_STATS.report(), html)
            elif requestOp=="/":
                self._sendFile("Try browsing http://%s:%d/ls?html=1" % (self.server.server_name,1969),html)
            elif requestOp=="/task":
                try:
                    (clientHost,clientPort) = self.client_address
                    if (clientHost!='127.0.0.1'):
                        raise Exception("externally submitted task: from %s" % clientHost)
                    performTask(requestArgs)
                    self._sendList(TASK_STATS.report(), html)
                except Exception:
                    self._sendFile(traceback.format_exc(),html)
            else:
                self._sendList(["Unknown command '"+requestOp + "' in request '"+self.path+"'"],html)
        except KeyError:
                self._sendList(["Illegal request "+self.path],html)
  
    def _sendHtmlHeader(self):
        self.send_response(200)
        self.send_header('Content-type','text/html')
        self.end_headers()            
        self.wfile.write("<html><head><title>Map-Reduce for GuineaPig</title></head>\n")
        self.wfile.write("<body>\n")
        self.wfile.write('Map-Reduce for GuineaPig: see [<a href="http://curtis.ml.cmu.edu/w/courses/index.php/Guinea_Pig">GuineaPig Wiki</a>]')
        self.wfile.write("<hr/>\n")

    def _sendHtmlFooter(self):
        self.wfile.write("<hr/>\n")
        self.wfile.write("[<a href=\"/ls?html=1\">List directories</a> "
                         "| <a href=\"/ls?html=1&dir=_history\">Task History</a> "
                         "| See <a href=\"/report?html=1\">Report on last task</a>]")
        self.wfile.write(" File system size: %s" % fmtchars(FS.totalSize()))
        self.wfile.write("</body></html>\n")

    def _addMarkup(self,it,colors=False):
        """Add some clickable markup to directory listings."""
        def hasStatus(it, stat):
            return it.find(": "+stat+" ")>=0
        def colorizeStatus(it, stat, color):
            lo = it.find(": "+stat+" ") + len(": ")
            hi = lo+len(stat)
            colorized = '<font color="%s">%s</font>' % (color,stat)
            return it[:lo] + colorized + it[hi:]
        if it.startswith(GPFileSystem.FILES_MARKER):
            keyword,n,f = it.split()
            return it + (' ' * (50-len(f)) + ' ') \
                + '[<a href="/ls?html=1&dir=%s">listing</a>' % (f) \
                + '|<a href="/getmerge?&dir=%s">download</a>]' % (f)
        elif it.startswith(GPFileSystem.CHARS_MARKER):
            keyword,n,df = it.split()
            splitPoint = df.find("/")
            d = df[:splitPoint]
            f = df[splitPoint+1:]
            return it + (' ' * (50-len(df)) + ' ') \
                + '[<a href="/cat?html=1&dir=%s&file=%s">cat</a>' % (d,f) \
                + '|<a href="/head?dir=%s&file=%s">download</a>' % (d,f) \
                + '|<a href="/head?html=1&dir=%s&file=%s">head</a>' % (d,f) \
                + '|<a href="/tail?html=1&dir=%s&file=%s">tail</a>]' % (d,f)
        elif hasStatus(it,"running"):
            return colorizeStatus(it, "running", "red")
        elif hasStatus(it,"finished"):
            return colorizeStatus(it, "finished", "green")
        else:
            return it

class ThreadingServer(ThreadingMixIn, HTTPServer):
    pass

def runServer():
    #allow only access from local machine
    #server_address = ('127.0.0.1', 1969)
    #allow access from anywhere
    server_address = ('0.0.0.0', 1969)    
    httpd = ThreadingServer(server_address, MRSHandler)
    startMsg = 'http server started on http://%s:%d/ls&html=1 at %s' % (httpd.server_name,1969,time.strftime('%X %x'))
    logging.info(startMsg)
    print startMsg
    while keepRunning:
        httpd.handle_request()
    logging.info(startMsg + ' has been shut down')

# client

import httplib
 
def sendRequest(command,quiet=False,timeout=None):
    http_server = "127.0.0.1:1969"
    conn = httplib.HTTPConnection(http_server,timeout=timeout)
    conn.request("GET", command)
    response = conn.getresponse()
    if response.status==200:
        data_received = response.read()
        conn.close()
        if not quiet:
            print data_received,
    else:
        conn.close()        
        raise Exception('%d %s' % (response.status,response.reason))

def serverIsResponsive():
    try:
        sendRequest("/ls",quiet=True,timeout=1)
        return True
    except Exception as e:
        return False
    

##############################################################################
# main
##############################################################################

def usage():
    print "usage: --serve: start server"
    print "usage: --shutdown: shutdown"
    print "usage: --report: print status of running (or last completed) task"
    print "usage: --fs ... "
    print "  where commands are: ls, ls DIR, write DIR FILE LINE, cat DIR FILE, getmerge DIR, head DIR FILE N, tail DIR FILE N"
    print "usage: --task --input DIR1 --output DIR2 --mapper [SHELL_COMMAND]: map-only task"
    print "       --task --input DIR1 --output DIR2 --mapper [SHELL_COMMAND] --reducer [SHELL_COMMAND] --numReduceTasks [K]: map-reduce task"
    print "  where directories DIRi are local file directories OR gpfs:XXX"
    print "  same options w/o --task will run the commands locally, not on the server, which means gpfs:locations are not accessible"
    print "usage: --probe: say if the server is running or down"
    print "usage: --send XXXX: simulate browser request http://server:port/XXXX and print response page"

if __name__ == "__main__":

    argspec = ["serve", "send=", "shutdown", "task", "help", "fs", "report", "probe",
               "input=", "output=", "mapper=", "reducer=", "numReduceTasks=", "joinInputs=", ]
    optlist,args = getopt.getopt(sys.argv[1:], 'x', argspec)
    optdict = dict(optlist)
    #print optdict,args
    
    if "--serve" in optdict:
        # log server to a file, since it runs in the background...
        logging.basicConfig(filename="server.log",level=logging.INFO)
        runServer()
    else:
        logging.basicConfig(level=logging.INFO)        
        if "--send" in optdict:
            sendRequest(optdict['--send'])
        elif "--shutdown" in optdict:
            sendRequest("/shutdown")
            while serverIsResponsive():
                print "waiting for shutdown..."
                time.sleep(1)
            print "shutdown complete."
        elif "--probe" in optdict:
            print "server is",("running" if serverIsResponsive() else "down")
        elif "--report" in optdict:
            sendRequest("/report")
        elif "--task" in optdict:
            del optdict['--task']
            sendRequest("/task?" + urllib.urlencode(optdict))
        elif "--help" in optdict:
            usage()
        elif "--fs" in optdict:
            if not args: 
                usage()
            else:
                request = "/"+args[0]+"?plain"
                if len(args)>1: request += "&dir="+args[1]
                if len(args)>2: request += "&file="+urllib.quote_plus(args[2])
                if len(args)>3 and args[0]=="write": request += "&line="+args[3]
                elif len(args)>3: request += "&n="+args[3]
                #print "request: "+request
                sendRequest(request)
        else:
            if (not '--input' in optdict) or (not '--output' in optdict):
                usage()
            else:
                performTask(optdict)

