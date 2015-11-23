##############################################################################
# (C) Copyright 2014, 2015 William W. Cohen.  All rights reserved.
##############################################################################

import cStringIO
import collections
import getopt
import logging
import os
import Queue
import select
import shutil
import subprocess
import sys
import threading
import time
import time
import traceback
import urllib

##############################################################################
# Map Reduce Streaming for GuineaPig (mrs_gp) - very simple
# multi-threading map-reduce, to be used with inputs/outputs which are
# just files in directories.  This combines multi-threading and
# multi-processing.
#
# TODO:
#  documennt --inputs
#  test/eval on servers and with gp
#
# Gotchas/bugs:
#   - threading doesn't work right in jython, and doesn't help
#   much in cpython.  best interpreter is pypy, due to lack
#   of GIL. TODO: confirm this for v4.
#  - if you use /afs/ as file store the server seems to 
#   leave some sort of lock file around which make deletion
#   impossible while the server is running
#
# Usage:
#  See the wiki at http://curtis.ml.cmu.edu/w/courses/index.php/Guinea_Pig
#  or use the option --help.
# 
# Briefly: 
#  1 - start server: mrs_gp --serve
#  2 - run tasks: mrs_gp --task --input a --output b --mapper cz --reducer d
#  ... or access filesystem: mrs_gp --fs ls [dir], mrs_gp --fs head dir file
#  ... or show last job details: mrs_gp --report
#  
#  n - stop server: mrs_gp --shutdown
#
# Or, you can run map-reduce jobs w/o server by omitting '--task'. 
##############################################################################

class MRS(object):
    VERSION = "1.4.0"
    COPYRIGHT = '(c) William Cohen 2015'

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
    # These are prefixes for pretty-printed directory listing, used
    # before the number of files in a directory or number of chars in
    # a file.  The html server uses these in adding extra links and
    # annotations.
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
            def fmtfile(f): return '%s%s  %s/%s' % (GPFileSystem.CHARS_MARKER,FS.fmtNumChars(self.size(d,f)),d,f)
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
        return d if not GPFileSystem.inGPFS(d) else d[len("gpfs:"):]

    @staticmethod
    def inGPFS(d):
        return d.startswith("gpfs:")

    @staticmethod
    def fmtNumChars(n):
        """"Format a large number of characters readably by also including the
        equivalent size in megabytes."""
        mb = n/(1024*1024.0)
        return "%d(%.1fM)" % (n,mb)


# global file system used by map-reduce system

FS = GPFileSystem()

##############################################################################
#
# Machinery for monitoring tasks
#
##############################################################################


class TaskStats(object):

    def __init__(self,opdict):
        self.opts = opdict.copy()
        self.startTime = {}
        self.endTime = {}
        self.ioSize = {}
        self.numStarted = {'mapper':0, 'reducer':0}
        self.numFinished = {'mapper':0, 'reducer':0}

    def start(self,msg):
        """Start timing something."""
        self.ioSize[msg] = {'stdin':0,'stdout':0,'stderr':0}
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
            else:
                line = ' %-40s: running for %.3f sec' % (k,now-self.startTime[k])
            for f in ['stdin','stdout','stderr']:
                line += ' %s %s' % (f,FS.fmtNumChars(self.ioSize[k][f]))
            buf.append(line)
        if includeLogs:
            buf.extend(['Subprocess Logs:'])
            buf.extend(FS.listFiles("_logs",pretty=True))
        return buf

# a global object to track the current task
TASK_STATS = TaskStats({'ERROR':'no tasks started yet'})

# prevent multiple tasks from happening at the same time
TASK_LOCK = threading.Lock()

##############################################################################
# main map-reduce algorithm(s)
#
# maponly is very simple: it sets up K independent mapper processes,
# one for each shard, which read from that shard and write to the
# corresponding output shard. mapper processes are handled via a
# general purpose 'pipeThread', which has similar functionality to
# popen.communicate(), but is asynchronous, and uses
# 'PipeOutputCollector' objects to save output.
#
# mapreduce sets up K reducers processes, each which has a reducerQ to
# gate its inputs, and a reducerQ thread which monitors the queue.
# mappers are similar but their output is caught by a
# ShufflingCollector object, which feed the reducer queues.  When a
# mapper stops writing to its stdout it will write a 'None' message on
# each queue, which is used to shut the queues down.
#
# Everything is tracked by a global TaskStats object, which records
# when pipeThread's start and end, and tracks the amount of data
# passed in/out via stdin, stdout, and stderr.
#
##############################################################################

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
        if '--input' in optdict:
            indirs = [optdict['--input']]
        elif '--inputs' in optdict:
            indirs = optdict['--inputs'].split(",")
        outdir = optdict['--output']
        if '--reducer' in optdict:
            #usage 1: a basic map-reduce has --input, --output, --mapper, --reducer, and --numReduceTasks
            mapper = optdict.get('--mapper','cat')
            reducer = optdict.get('--reducer','cat')
            numReduceTasks = int(optdict.get('--numReduceTasks','1'))
            mapreduce(indirs,outdir,mapper,reducer,numReduceTasks)
        else:
            #usage 1: a map-only task has --input, --output, --mapper
            mapper = optdict.get('--mapper','cat')
            maponly(indirs,outdir,mapper)        
        TASK_STATS.end('__top level task')
        FS.write("_history",time.strftime("task-%Y.%m.%H.%M.%S"),"\n".join(TASK_STATS.report(includeLogs=False)))

    finally:
        TASK_LOCK.release()

def key(line):
    """Extract the key for a line containing a tab-separated key,value pair."""
    return line[:line.find("\t")]

def mapreduce(indirList,outdir,mapper,reducer,numReduceTasks):
    """Run a generic streaming map-reduce process.  The mapper and reducer
    are shell commands, as in Hadoop streaming.  Both indir and outdir
    are directories."""

    #infiles is a list of input files, and indirs is a parallel list if
    #directories, so the i-th mapper reads from indirs[i],infiles[i]
    indirs,infiles = setupFiles(indirList,outdir)
    numMapTasks = len(infiles)
    global TASK_STATS

    # Set up a place to save the inputs to K reducers - each of which
    # is a buffer bj, which contains lines for shard K,

    TASK_STATS.start('_init reducers and queues')
    reduceOutputs = map(lambda j:'part%04d' % j, range(numReduceTasks))
    reduceTags = map(lambda j:'reducer-to-%s-%s' % (outdir,reduceOutputs[j]), range(numReduceTasks))
    reduceQs = map(lambda j:Queue.Queue(), range(numReduceTasks))
    reducePipes = map(lambda j:makePipe("sort -k1,2 | "+reducer), range(numReduceTasks))
    reduceQThreads = map(
        lambda j:threading.Thread(target=acceptReduceInputs, args=(numMapTasks,reduceQs[j],reducePipes[j])), 
        range(numReduceTasks))
    TASK_STATS.end('_init reducers and queues')

    # start the mappers - along with threads to shuffle their outputs
    # to the appropriate reducer task queue

    TASK_STATS.start('_init mappers and shufflers')
    # names of the mapper tasks
    mapTags = map(lambda i:'mapper-from-%s-%s' % (indirs[i],infiles[i]), range(numMapTasks))
    # subprocesses for each mapper
    mapPipes = map(lambda i:makePipe(mapper), range(numMapTasks))
    # collect stderr of each mapper
    mapErrCollectors = map(lambda i:FileOutputCollector("gpfs:_logs",mapTags[i]),range(numMapTasks))
    shufflerCollectors = map(lambda i:ShufflingCollector(reduceQs), range(numMapTasks))
    mapThreads = map(
        lambda i:threading.Thread(target=pipeThread, 
                                  args=(mapTags[i],mapPipes[i],
                                        getInput(indirs[i],infiles[i]),
                                        shufflerCollectors[i], mapErrCollectors[i])),
        range(numMapTasks))
    TASK_STATS.end('_init mappers and shufflers')

    #run the map tasks, and wait for the queues to empty
    TASK_STATS.start('_run mappers')
    for t in reduceQThreads: t.start()
    for t in mapThreads: t.start()
    #print 'join mapThreads'
    for t in mapThreads: t.join()
    #print 'join reduceQThreads'
    for t in reduceQThreads: t.join()
    #print 'join reduceQs'
    #for q in reduceQs: q.join()
    #print 'joined....'
    TASK_STATS.end('_run mappers')

    reduceErrCollectors = map(
        lambda j:FileOutputCollector("gpfs:_logs",reduceTags[j]),
        range(numReduceTasks))
    outCollectors = map(
        lambda j:FileOutputCollector(outdir,reduceOutputs[j]), 
        range(numReduceTasks))
    reduceThreads = map(
        lambda j:threading.Thread(target=pipeThread,
                                  args=(reduceTags[j],reducePipes[j],
                                        None,outCollectors[j],reduceErrCollectors[j])),
        range(numReduceTasks))

    TASK_STATS.start('_run reducers')
    for t in reduceThreads: t.start()
    for t in reduceThreads: t.join()
    TASK_STATS.end('_run reducers')

def maponly(indirList,outdir,mapper):
    """Like mapreduce but for a mapper-only process."""

    indirs,infiles = setupFiles(indirList,outdir)
    global TASK_STATS

    numMapTasks = len(infiles)
    # names of the mapper tasks
    mapOutputs = map(lambda i:'part%04d' % i, range(numMapTasks))
    mapTags = map(lambda i:'mapper-from-%s-%s' % (indirs[i],infiles[i]), range(numMapTasks))
    # subprocesses for each mapper
    mapPipes = map(lambda i:makePipe(mapper), range(numMapTasks))
    # collect stderr of each mapper
    errCollectors = map(lambda i:FileOutputCollector("gpfs:_logs",mapTags[i]),range(numMapTasks))
    # collect outputs of mappers
    outCollectors = map(lambda i:FileOutputCollector(outdir,mapOutputs[i]), range(numMapTasks))
    # threads for each mapper
    mapThreads = map(
        lambda i:threading.Thread(target=pipeThread, 
                                  args=(mapTags[i],mapPipes[i],
                                        getInput(indirs[i],infiles[i]),
                                        outCollectors[i], errCollectors[i])),
        range(numMapTasks))

    #execute the threads and wait to finish
    TASK_STATS.start('_running mappers')
    for t in mapThreads: t.start()
    for t in mapThreads: t.join()
    TASK_STATS.end('_running mappers')

####################
# pipe threads

BUFFER_SIZE = 32*1024*1024
SLEEP_DURATION = 0.1

def makePipe(shellCom):
    """Create a subprocess that communicates via stdin, stdout, stderr."""
    p = subprocess.Popen(shellCom,shell=True, bufsize=BUFFER_SIZE,
                         stdin=subprocess.PIPE,stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    return p

def pipeThread(tag,pipe,inbuf,outCollector,errCollector):
    """A thread that communicates with a subprocess pipe produced by
    makePipe.  This is an asynchronous version of the
    popen.communicate() method. Tag is a name for the pipe, inbuf is a
    string to be sent as input to the pipe, and the outCollector and
    errCollector are OutputCollector objects that will forward the
    outputs to the appropriate place - the global GPFileSystem FS, an
    actual file, or another thread.

    If inbuf==None it should be that all input has been written to the
    pipe already, and stdin has been closed."""

    #The goal is to read from and write to the process as robustly as
    #possible - so in a loop, do the following:
    #
    # - use select() to see which operations are possible:
    #   write to stdin, or read from stdout/stderr
    #
    # - if you can read, use os.read() - since the amount available to
    # read might not be a full line. pass what was read to the
    # appropriate collector, and on EOF, close the collector and stop
    # monitoring this output stream.
    #
    # - if you can write, write with os.write(), again since the
    # process need not be able to hold a full line. when you hit
    # end of the inbuf, or os.write() returns 0, then close the
    # stdin.
    #
    # - check and see if you made any progress, by reading or writing
    # if you didn't, there are two possibilities: the pipe is
    # computing some more output, or it's finished.  if it's finished,
    # exit the loop.  if not, sleep for a short time to let the
    # process compute.

    #the active inputs and outputs of the pipe
    activeInputs = [pipe.stdin] if inbuf!=None else []
    activeOutputFPs = {pipe.stdout:'stdout',pipe.stderr:'stderr'}
    collectors = {'stdout':outCollector, 'stderr':errCollector}
    #the part of inbuf previously written is always inbuf[:inbufPtr]
    inbufPtr = 0
    # how much to send at a time to the pipe, this is guaranteed by
    # posix, for some reason select.PIPE_BUF doesn't seem to exist
    MIN_PIPE_BUFFER_SIZE = min(512,BUFFER_SIZE)

    global TASK_STATS
    TASK_STATS.start(tag)

    for k in TASK_STATS.numStarted.keys():
        #tag starts with 'mapper' or 'reducer'
        if tag.startswith(k): 
            TASK_STATS.numStarted[k] += 1

    while True:

#        print 'stdin',inbufPtr,'stdout',len(result['stdout'].getvalue()), \
#             'stderr',len(result['stderr'].getvalue())

        readable,writeable,exceptional = \
            select.select(activeOutputFPs.keys(), activeInputs, activeOutputFPs.keys()+activeInputs, 0)
        assert not exceptional,'exceptional files + %r' % exceptional

        #print 'loop r,w',readable,writeable

        progress = False
        for fp in readable:
            # key will be string 'stdout' or 'stdin'
            key = activeOutputFPs[fp]
            #print '-',key
            tmp = os.read(fp.fileno(), BUFFER_SIZE)
            n = len(tmp)
            #print 'r',n
            if n > 0:
                collectors[key].collect(tmp)
                TASK_STATS.ioSize[tag][key] += n
                progress = True
            else:
                #EOF on fp - close the corresponding collector and
                #stop trying to read from this fp
                collectors[key].close()
                del activeOutputFPs[fp]

        if pipe.stdin in writeable:
            # figure out how much I can write...
            hi = min(inbufPtr+MIN_PIPE_BUFFER_SIZE, len(inbuf))
            #print '+','stdin',hi,len(inbuf)
            n = os.write(writeable[0].fileno(), inbuf[inbufPtr:hi])
            #print 'w',n
            if n>0:
                inbufPtr += n
                TASK_STATS.ioSize[tag]['stdin'] += n
                progress = True
            if n==0 or inbufPtr>=len(inbuf):
                #EOF on stdin - close stdin, and stop trying to write
                #to it
                pipe.stdin.close()
                activeInputs = []

        if progress:
            #print '.',
            pass
        elif pipe.poll()!=None:
            #process finished
            break
        else:
            #wait for process to get some output ready
            #print '?..'
            time.sleep(SLEEP_DURATION)

    #finished the loop
    TASK_STATS.end(tag)
    for k in TASK_STATS.numStarted.keys():
        if tag.startswith(k): 
            TASK_STATS.numFinished[k] += 1
    #print 'finished loop'

# used with pipeThread's

class PipeOutputCollector(object):
    """Abstract class used for collected output of a pipe."""
    def collect(str): 
        """Collect another string."""
        pass
    def close(): 
        """Close the file/process/thread we're writing to."""        
        pass

class FileOutputCollector(PipeOutputCollector):
    """Collector writing to a GPFileSystem, or ordinary file."""
    def __init__(self,outdir,outfile):
        """If gpfs is true then the output is on the global GPFileSystem."""
        self.gpfs = GPFileSystem.inGPFS(outdir)
        if self.gpfs:
            self.outdir = outdir
            self.outfile = outfile
        else:
            self.fp = open(outdir+"/"+outfile, 'w')
    def collect(self,str):
        if self.gpfs: 
            global FS
            FS.write(self.outdir,self.outfile,str)
        else:
            self.fp.write(str)
    def close(self):
        if not self.gpfs:
            self.fp.close()

class ShufflingCollector(PipeOutputCollector):
    """Collector writing to reducer queues."""
    def __init__(self,reduceQs):
        self.reduceQs = reduceQs
        self.numReduceTasks = len(reduceQs)
        # leftover is anything that followed the last newline in the
        # most recently collected string
        self.leftover = ''
    def collect(self,str):
        # first break the string into lines
        lines = (self.leftover + str).split("\n")
        self.leftover = lines[-1] #will be '' if str ends with newline
        for line0 in lines[:-1]:
            #send each line to appropriate reducer 
            line = line0 + "\n"
            k = key(line)
            self.reduceQs[hash(k)%self.numReduceTasks].put(line)
    def close(self):
        assert not self.leftover, "collected data wasn't linefeed-terminated"
        # send 'done' signal ("poison") to all queues
        for q in self.reduceQs:
            q.put(None)

####################
# used with reducer queues

def acceptReduceInputs(numMapTasks,reduceQ,reducePipe):
    """Thread that monitors a queue of items to add to send to a reducer process."""
    numPoison = 0 #number of mappers that have finished writing
    while numPoison<numMapTasks:
        task = reduceQ.get()
        if task:
            line = task
            reducePipe.stdin.write(line)
            reduceQ.task_done()
        else:
            #some mapper has indicated that it's finished
            numPoison += 1
    #now all mappers are finished
    reducePipe.stdin.close()

####################
# access input/output files for mapreduce

def setupFiles(indirList,outdir):
    """Returns parallel lists, indirs and infiles, where infiles is a list
    of input files, and indirs is a parallel list if directories, so the
    i-th mapper reads from indirs[i],infiles[i].  clear the output directory,
    if needed. """
    indirs = []
    infiles = []
    for dir in indirList:
        if GPFileSystem.inGPFS(dir):
            files = FS.listFiles(dir)
        else:
            files = [f for f in os.listdir(dir)]
        infiles.extend(files)
        indirs.extend([dir] * len(files))
    if outdir.startswith("gpfs:"):
        FS.rmDir(outdir)
    else:
        if os.path.exists(outdir):
            logging.warn('removing %s' % (outdir))
            shutil.rmtree(outdir)
        os.makedirs(outdir)
    return indirs,infiles
                      
def getInput(indir,f):
    """Return the content of the input file at indir/f"""
    if GPFileSystem.inGPFS(indir):
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
        self.wfile.write(" File system size: %s" % FS.fmtNumChars(FS.totalSize()))
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
    print 'Map-Reduce Streaming for Guinea Pig',MRS.VERSION,MRS.COPYRIGHT
    print ""
    print "Server-control usages:"
    print " --serve: start server"
    print " --shutdown: shutdown"
    print " --probe: say if the server is running or down"
    print ""
    print "File system usages:"
    print " --fs ls [DIR]"
    print " --fs (cat|head|tail) DIR FILE"
    print " --fs getmerge DIR"
    print " --fs write DIR FILE LINE  #for debugging"
    print ""
    print "Running tasks on the server:"
    print " --task --input DIR1 --output DIR2 --mapper SHELL_COMMAND   #map-only task"
    print " --task --input DIR1 --output DIR2 --mapper SHELL_COMMAND1 --reducer SHELL_COMMAND2 [--numReduceTasks K]"
    print " --report #on the last task completed"
    print ""
    print "Note 1: DIRs which start with gpfs: will be stored in-memory on the server"
    print "Note 2: You can run tasks locally, not on the server by omitting --task, if gpfs: is not used"
    print "Note 3: You can replace '--input DIR' with '--inputs DIR1,DIR2,....' ie, a comma-separated list of DIRS"

if __name__ == "__main__":

    argspec = ["serve", "send=", "shutdown", "task", "help", "fs", "report", "probe",
               "input=", "output=", "mapper=", "reducer=", "numReduceTasks=", "inputs=", ]
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
            if (('--inputs' in optdict) or ('--input' in optdict)) and ('--output' in optdict):
                performTask(optdict)
            else:
                usage()


