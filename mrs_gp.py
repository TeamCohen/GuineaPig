##############################################################################
# (C) Copyright 2014, 2015 William W. Cohen.  All rights reserved.
##############################################################################

import cStringIO
import collections
import getopt
import logging
import os
import os.path
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
#  document --inputs
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
#
# Experimental feature: The option "--async 1" will turn on a more
# asynchronous version version of the code that monitors a mapper or
# reducer subprocess, which gives you more ability to monitor
# progress, but maybe? is less stable.
##############################################################################

class MRS(object):
    VERSION = "1.4.1"
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
        self.numStarted = {'mapper':0, 'reducer':0, 'shuffler':0}
        self.numFinished = {'mapper':0, 'reducer':0, 'shuffler':0}

    def start(self,msg):
        """Start timing something."""
        self.ioSize[msg] = {'stdin':0,'stdout':0,'stderr':0}
        self.startTime[msg] = time.time()
        for k in self.numStarted.keys():
            if msg.startswith(k): 
                self.numStarted[k] += 1
        logging.info('started  '+msg)

    def end(self,msg):
        """End timing something."""
        self.endTime[msg] = time.time()
        for k in self.numFinished.keys():
            if msg.startswith(k): 
                self.numFinished[k] += 1
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
            try:
                minTime = min(self.endTime[tag]-self.startTime[tag] for tag in self.endTime.keys() if tag.startswith(k))
                maxTime = max(self.endTime[tag]-self.startTime[tag] for tag in self.endTime.keys() if tag.startswith(k))
            except ValueError:
                #empty sequence
                minTime = maxTime = 0.0
            progressBar = '[*]'
            if s>0:
                progressBar = "progress [" + ("#"*f) + ("."*(s-f))+"]"
            buf.extend([' %-7s summary: %d/%d finished/started %s min/max time %.3f/%.3f' % \
                        (k,self.numFinished[k],self.numStarted[k],progressBar,minTime,maxTime)])
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
        #the log directory is error logs for the most recent top-level
        #task only
        FS.rmDir("gpfs:_logs")

        #two strategies for managing pipes in parallel are implemented
        pipeThread = asyncPipeThread if int(optdict.get('--async',"0")) else simplePipeThread

        #parse input and output directories
        if '--input' in optdict:
            indirs = [optdict['--input']]
        elif '--inputs' in optdict:
            indirs = optdict['--inputs'].split(",")
        outdir = optdict['--output']
        #for security, disallow any access to files above the
        #directory in which the main process (eg server) is running
        for d in indirs+[outdir]: assert d.find("..")<0, "unsafe input directory '"+d+"'"

        if '--reducer' in optdict:
            #usage 1: a basic map-reduce has --input, --output, --mapper, --reducer, and --numReduceTasks
            mapper = optdict.get('--mapper','cat')
            reducer = optdict.get('--reducer','cat')
            numReduceTasks = int(optdict.get('--numReduceTasks','1'))
            mapreduce(indirs,outdir,mapper,reducer,numReduceTasks,pipeThread)
        else:
            #usage 1: a map-only task has --input, --output, --mapper
            mapper = optdict.get('--mapper','cat')
            maponly(indirs,outdir,mapper,pipeThread)        
        TASK_STATS.end('__top level task')
        FS.write("_history",time.strftime("task-%Y.%m.%H.%M.%S"),"\n".join(TASK_STATS.report(includeLogs=False)))

    finally:
        TASK_LOCK.release()

def key(line):
    """Extract the key for a line containing a tab-separated key,value pair."""
    return line[:line.find("\t")]

def mapreduce(indirList,outdir,mapper,reducer,numReduceTasks,pipeThread):
    """Run a generic streaming map-reduce process.  The mapper and reducer
    are shell commands, as in Hadoop streaming.  Both indir and outdir
    are directories."""

    #indirs[i],infiles[i] is the i-th input
    #outdirs[j],outfiles[i] is the i-th output
    indirs,infiles,outdirs,outfiles = setupFiles(indirList,outdir,numReduceTasks)

    numMapTasks = len(infiles)
    global TASK_STATS

    # Set up a place to save the inputs to K reducers - each of which
    # is a buffer bj, which contains lines for shard K,

    TASK_STATS.start('_init reducers and queues')
    #where to output the results
    #names of the reduce tasks
    reduceTags = map(lambda j:'reducer-to-%s-%s' % (outdirs[j],outfiles[j]), range(numReduceTasks))
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
    shuffleTags = map(lambda i:'shuffler-from-%s-%s' % (indirs[i],infiles[i]), range(numMapTasks))
    # subprocesses for each mapper
    mapPipes = map(lambda i:makePipe(mapper), range(numMapTasks))
    # collect stderr of each mapper
    mapErrCollectors = map(lambda i:FileOutputCollector("gpfs:_logs",mapTags[i]),range(numMapTasks))
    shufflerCollectors = map(lambda i:ShufflingCollector(shuffleTags[i],reduceQs), range(numMapTasks))
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
        lambda j:FileOutputCollector(outdirs[j],outfiles[j]),
        range(numReduceTasks))
    reduceThreads = map(
        lambda j:threading.Thread(target=pipeThread,
                                  args=(reduceTags[j],reducePipes[j],
                                        '',outCollectors[j],reduceErrCollectors[j])),
        range(numReduceTasks))

    TASK_STATS.start('_run reducers')
    for t in reduceThreads: t.start()
    for t in reduceThreads: t.join()
    TASK_STATS.end('_run reducers')

def maponly(indirList,outdir,mapper,pipeThread):
    """Like mapreduce but for a mapper-only process."""

    indirs,infiles,outdirs,outfiles = setupFiles(indirList,outdir,-1)
    global TASK_STATS

    numMapTasks = len(infiles)
    # mapper output locations
    mapTags = map(lambda i:'mapper-from-%s-%s' % (indirs[i],infiles[i]), range(numMapTasks))
    # subprocesses for each mapper
    mapPipes = map(lambda i:makePipe(mapper), range(numMapTasks))
    # collect stderr of each mapper
    errCollectors = map(lambda i:FileOutputCollector("gpfs:_logs",mapTags[i]),range(numMapTasks))
    # collect outputs of mappers
    outCollectors = map(lambda i:FileOutputCollector(outdirs[i],outfiles[i]), range(numMapTasks))
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

#params used by makePipe and the asyncPipeThread routine
BUFFER_SIZE = 512*1024
SLEEP_DURATION = 0.01

def makePipe(shellCom):
    """Create a subprocess that communicates via stdin, stdout, stderr."""
    p = subprocess.Popen(shellCom,shell=True, bufsize=BUFFER_SIZE,
                         stdin=subprocess.PIPE,stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    return p

def simplePipeThread(tag,pipe,inbuf,outCollector,errCollector):

    global TASK_STATS
    TASK_STATS.start(tag)
    TASK_STATS.ioSize[tag]['stdin'] = len(inbuf)
    #this actually runs the subprocess
    (outbuf,errbuf) = pipe.communicate(input=inbuf)    
    TASK_STATS.ioSize[tag]['stdout'] = len(outbuf)
    TASK_STATS.ioSize[tag]['stderr'] = len(errbuf)
    errCollector.collect(errbuf)
    errCollector.close()
    #we'll call this the end of this thread - if sending the output
    #along is complex, then the collector should declare another
    #process
    TASK_STATS.end(tag)
    outCollector.collect(outbuf)
    outCollector.close()

def asyncPipeThread(tag,pipe,inbuf,outCollector,errCollector):
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
    if inbuf:
        activeInputs = [pipe.stdin] 
    else:
        pipe.stdin.close()
        activeInputs = []         
    activeOutputFPs = {pipe.stdout:'stdout',pipe.stderr:'stderr'}
    collectors = {'stdout':outCollector, 'stderr':errCollector}
    #the part of inbuf previously written is always inbuf[:inbufPtr]
    inbufPtr = 0
    # how much to send at a time to the pipe, this is guaranteed by
    # posix, for some reason select.PIPE_BUF doesn't seem to exist
    MIN_PIPE_BUFFER_SIZE = min(512,BUFFER_SIZE)
    global TASK_STATS

    TASK_STATS.start(tag)
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
            self.fp = open(os.path.join("./"+outdir,outfile),'w')
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
    """A collector for writing shuffled output to reducer queues."""

    def __init__(self,tag,reduceQs):
        self.firstCollection = True
        self.tag = tag
        self.reduceQs = reduceQs
        self.numReduceTasks = len(reduceQs)
        # buffer what goes into the reducer queues, since they seem
        # very slow with lots of little inputs
        self.buffers = map(lambda i:cStringIO.StringIO(), reduceQs)
        # 'leftover' is anything that followed the last newline in the
        # most recently collected string - only happens when
        # this is called asynchronously
        self.leftover = ''

    def collect(self,str):

        global TASK_STATS
        if self.firstCollection:
            #note that we've started
            TASK_STATS.start(self.tag)
            self.firstCollection = False
        # optimize - don't copy str if we don't need to 
        lines = (self.leftover + str) if self.leftover else str
        #loop through each line and shuffle it to the right location
        lastNewline = 0
        while True:
            nextNewline = lines.find("\n",lastNewline)
            if nextNewline<0: 
                # no more complete lines
                self.leftover = lines[lastNewline:]
                break
            else:
                line = lines[lastNewline:nextNewline+1]
                TASK_STATS.ioSize[self.tag]['stdin'] += len(line)
                k = key(line)
                #self.reduceQs[hash(k)%self.numReduceTasks].put(line)
                self.buffers[hash(k)%self.numReduceTasks].write(line)
                lastNewline = nextNewline+1

    def close(self):
        assert not self.leftover, "collected data wasn't linefeed-terminated"
        global TASK_STATS
        for i in range(len(self.reduceQs)):
            # send the buffered-up data for the i-th queue
            bufi =  self.buffers[i].getvalue()
            self.reduceQs[i].put(bufi)
            TASK_STATS.ioSize[self.tag]['stdout'] += len(bufi)
            # signal we're done with this queue
            self.reduceQs[i].put(None)
        TASK_STATS.end(self.tag)

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
    #now all mappers are finished so we can exit

####################
# access input/output files for mapreduce

def setupFiles(indirList,outdir,numReduceTasks):
    """Return two parallel lists, indirs and infiles and outdirs and
    outfiles, so that indirs[i]/infiles[i] is the i-th input, and
    outdirs[j]/outfiles[j] is the j-th output.  The number of outputs
    is determined by numReduceTasks: if it is -1, then this is
    interpreted as a map-only task, and the number of outputs is the
    same as the number of inputs.

    Generally indirList is a list of directories or GPFS dirs, and the
    input files are the contents of those directories.  But indirList
    could also contains only files.  In this case, if there is a
    single output, it will also be a file.
    """
    indirs = []
    infiles = []
    outputToFile = False
    if all(os.path.isfile(f) for f in indirList): 
        for f in indirList:
            inhead,intail = os.path.split(f)
            indirs.append(inhead)
            infiles.append(intail)
        if (numReduceTasks==1 or (numReduceTasks==-1 and len(indirList)==1)) and not GPFileSystem.inGPFS(outdir):
            #if there's one output, and all inputs are files, then make the output a file also
            outhead,outtail = os.path.split(outdir)
            outdirs=[outhead]
            outfiles=[outtail]
            outputToFile = True
    else:
        for dir in indirList:
            if GPFileSystem.inGPFS(dir):
                files = FS.listFiles(dir)
            elif os.path.isdir(dir):
                files = [f for f in os.listdir(dir)]
            else:
                assert False,'illegal input location %s' % dir
            infiles.extend(files)
            indirs.extend([dir] * len(files))
    #clear space/make directory for output, if necessary
    if os.path.exists(outdir):
        logging.warn('removing %s' % (outdir))
        shutil.rmtree(outdir)
    if not outputToFile:
        os.makedirs(outdir)
    # construct the list of output files
    if numReduceTasks == -1:
        outfiles = infiles
    else:
        outfiles = map(lambda j:'part04%d' % j, range(numReduceTasks))
    outdirs = [outdir]*len(outfiles)
    return indirs,infiles,outdirs,outfiles
                      
def getInput(indir,f):
    """Return the content of the input file at indir/f.  Indir could also
    be on GPFS.
    """
    if GPFileSystem.inGPFS(indir):
        return FS.cat(indir,f)
    else:
        inputString = cStringIO.StringIO()
        k = 0
        for line in open("./"+os.path.join(indir,f)):
            inputString.write(line)
            k += 1
        return inputString.getvalue()

##############################################################################
# server/client stuff
##############################################################################

# server

from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
from SocketServer import ThreadingMixIn
import urlparse

keepRunning = True
serverPort = 8000

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
        global serverPort
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
                self._sendFile("Try browsing http://%s:%d/ls?html=1" % (self.server.server_name,serverPort),html)
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

#incantations for setting up a multi-threaded server
class ThreadingServer(ThreadingMixIn, HTTPServer):
    pass

def runServer():
    global serverPort
    #to allow only access from local machine, use server_address = ('127.0.0.1', serverPort)
    #thid will allow access from anywhere....
    server_address = ('0.0.0.0', serverPort)    
    httpd = ThreadingServer(server_address, MRSHandler)
    startMsg = 'http server started on http://%s:%d/ls&html=1 at %s' % (httpd.server_name,serverPort,time.strftime('%X %x'))
    logging.info(startMsg)
    print startMsg
    while keepRunning:
        httpd.handle_request()
    logging.info(startMsg + ' has been shut down')

# client

import httplib
 
def sendRequest(command,quiet=False,timeout=None):
    """Send a request to the server."""
    global serverPort
    http_server = "127.0.0.1:%d" % serverPort
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
    """Check if the server is up, return True/False"""
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
    print "Note 4: The experimental '--async 1' option is less well-tested but maybe gives better monitoring."

if __name__ == "__main__":

    argspec = ["serve", "send=", "shutdown", "task", "help", "fs", "report", "probe", "async=", "port=",
               "input=", "output=", "mapper=", "reducer=", "numReduceTasks=", "inputs=", ]
    optlist,args = getopt.getopt(sys.argv[1:], 'x', argspec)
    optdict = dict(optlist)
    #print optdict,args
    
    serverPort = int(optdict.get("port",8000))

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


