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

##############################################################################
# Map Reduce Streaming for GuineaPig (mrs_gp) - very simple
# multi-threading map-reduce, to be used with inputs/outputs which are
# just files in directories.  This combines multi-threading and
# multi-processing.
#
# To do:
#  map-only tasks, multiple map inputs
#  secondary grouping sort key --joinmode
#  optimize - keys, IOString?
#  have a no-server compatibility mode (or, for compute-intensive tasks)
#  default n=10 for head, tail
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
#                           #so you might prefer:
#                           # pypy mrs_gp.py --serve >& server.log &
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
#  Reducers are optional, if they are not present it will be map-only
#  task.
#
#  DIR also "GPFileSystem" directory, specified by the prefix gpfs:
#  These are NOT hierarchical and are just stored in memory by the
#  server.  The files in a directory are always shards of a map-reduce
#  computation.
#  
#  If you want to examine the gpfs: files, you can use a browser on
#  http://localhost:1969/XXX where XXX is a cgi-style query.  For
#  instance, http://localhost:1969/ls will list the directories, and
#  http://localhost:1969/getmerge?dir=foo will return the contents of
#  all shards in the directory foo.  Other commands are ls?dir=X,
#  cat?dir=X&file=Y, and head and tail which are like cat but also
#  have an argument "n".
#
#  Or, you can use 'pypy mrs_gp.py --send XXX' instead, which emulates
#  the browser command and prints the text that would be sent to the
#  browser.
#
# 3) Shut down the server, discarding everything held by the
# GPFileSystem.
#
#  pypy mrs_gp.py --shutdown
#
##############################################################################

##############################################################################
#
# shared "files system"
#
##############################################################################

class GPFileSystem(object):

    def __init__(self):
        #file names in directory/shards
        self.filesIn = collections.defaultdict(list)
        #content of (dir,file)
        self.linesOf = {}
    def rmDir(self,d0):
        d = self._fixDir(d0)
        if d in self.filesIn:
            for f in self.filesIn[d]:
                del self.linesOf[(d,f)]
            del self.filesIn[d]
    def append(self,d0,f,line):
        d = self._fixDir(d0)
        if not f in self.filesIn[d]:
            self.filesIn[d].append(f)
            self.linesOf[(d,f)] = list()
        self.linesOf[(d,f)].append(line)
    def listDirs(self):
        return self.filesIn.keys()
    def listFiles(self,d0):
        d = self._fixDir(d0)
        return self.filesIn[d]
    def cat(self,d0,f):
        d = self._fixDir(d0)
        return self.linesOf[(d,f)]
    def head(self,d0,f,n):
        d = self._fixDir(d0)
        return self.linesOf[(d,f)][:n]
    def tail(self,d0,f,n):
        d = self._fixDir(d0)
        return self.linesOf[(d,f)][-n:]
    def __str__(self):
        return "FS("+str(self.filesIn)+";"+str(self.linesOf)+")"
    def _fixDir(self,d):
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
##############################################################################

def performTask(optdict):
    """Utility that calls mapreduce or maponly, as appropriate, based on the options."""
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

def mapreduce(indir,outdir,mapper,reducer,numReduceTasks):
    """Run a generic streaming map-reduce process.  The mapper and reducer
    are shell commands, as in Hadoop streaming.  Both indir and outdir
    are directories."""

    usingGPFS,infiles = setupFiles(indir,outdir)

    # Set up a place to save the inputs to K reducers - each of which
    # is a buffer bj, which maps a key to a list of values associated
    # with that key.  To fill these buffers we also have K threads tj
    # to accumulate inputs, and K Queue's qj for the threads to read
    # from.

    logging.info('starting reduce buffer queues')
    reducerQs = []        # task queues to join with later 
    reducerBuffers = []   # data to send to reduce processes later 
    for j in range(numReduceTasks):
        qj = Queue.Queue()
        bj = collections.defaultdict(list)
        reducerQs.append(qj)
        reducerBuffers.append(bj)
        tj = threading.Thread(target=acceptReduceInputs, args=(qj,bj))
        tj.daemon = True
        tj.start()

    # start the mappers - each of which is a process that reads from
    # an input file or GPFS location, and a thread that passes its
    # outputs to the reducer queues.

    logging.info('starting mapper processes and shuffler threads')
    mappers = []
    mapFeeders = []
    for fi in infiles:
        # WARNING: it doesn't seem to work well to start the processes
        # inside a thread - this led to bugs with the reducer
        # processes.  This is possibly a python library bug:
        # http://bugs.python.org/issue1404925
        if 'input' in usingGPFS:
            mapPipeI = subprocess.Popen(mapper,shell=True,stdin=subprocess.PIPE,stdout=subprocess.PIPE)
            feederI = threading.Thread(target=feedPipeFromGPFS, args=(indir,fi,mapPipeI))
            feederI.start()
            mapFeeders.append(feederI)
        else:
            mapPipeI = subprocess.Popen(mapper,shell=True,stdin=open(indir + "/" + fi),stdout=subprocess.PIPE)
        # a thread to read the mapprocess's output
        si = threading.Thread(target=shuffleMapOutputs, args=(mapper,mapPipeI,reducerQs,numReduceTasks))
        si.start()                      # si will join the mapPipe process
        mappers.append(si)

    #wait for the map tasks, and to empty the queues
    joinAll(mappers,'mappers')        
    if mapFeeders: joinAll(mapFeeders,'map feeders') 

    # run the reduce processes, each of which is associated with a
    # thread that feeds it inputs from the j's reduce buffer.

    logging.info('starting reduce processes and threads to feed these processes')
    reducers = []
    reducerConsumers = []
    for j in range(numReduceTasks):    
        if 'output' in usingGPFS:
            reducePipeJ = subprocess.Popen(reducer,shell=True,stdin=subprocess.PIPE,stdout=subprocess.PIPE)
            consumerJ = threading.Thread(target=writePipeToGPFS, args=(outdir,("part%05d" % j),reducePipeJ))
            consumerJ.start()
            reducerConsumers.append(consumerJ)
        else:
            fpj = open("%s/part%05d" % (outdir,j), 'w')
            reducePipeJ = subprocess.Popen(reducer,shell=True,stdin=subprocess.PIPE,stdout=fpj)
        # thread to feed data into the reducer process
        uj = threading.Thread(target=sendReduceInputs, args=(reducerBuffers[j],reducePipeJ,j))
        uj.start()                      # uj will shut down reducePipeJ process on completion
        reducers.append(uj)

    #wait for the reduce tasks
    joinAll(reducerQs,'reduce queues')  # the queues have been emptied
    joinAll(reducers,'reducers')
    if reducerConsumers: joinAll(reducerConsumers,'reducer consumers')

def maponly(indir,outdir,mapper):
    """Like mapreduce but for a mapper-only process."""

    usingGPFS,infiles = setupFiles(indir,outdir)

    # start the mappers - each of which is a process that reads from
    # an input file, and outputs to the corresponding output file

    logging.info('starting mapper processes')
    activeMappers = set()
    mapFeeders = []
    mapConsumers = []
    for fi in infiles:
        if ('input' in usingGPFS) and not ('output' in usingGPFS):
            print 'case gpfs => file'
            mapPipeI = subprocess.Popen(mapper,shell=True,stdin=subprocess.PIPE,stdout=open(outdir + "/" + fi, 'w'))
            feederI  = threading.Thread(target=feedPipeFromGPFS, args=(indir,fi,mapPipeI))
            feederI.start()
            mapFeeders.append(feederI)
        elif not ('input' in usingGPFS) and ('output' in usingGPFS):
            mapPipeI = subprocess.Popen(mapper,shell=True,stdin=open(indir + "/" + fi),stdout=subprocess.PIPE)
            consumerI  = threading.Thread(target=writePipeToGPFS, args=(outdir,fi,mapPipeI))
            consumerI.start()
            mapConsumers.append(consumerI)
        elif ('input' in usingGPFS) and ('output' in usingGPFS):
            mapPipeI = subprocess.Popen(mapper,shell=True,stdin=subprocess.PIPE,stdout=subprocess.PIPE)
            feederI  = threading.Thread(target=feedPipeFromGPFS, args=(indir,fi,mapPipeI))
            feederI.start()
            mapFeeders.append(feederI)
            consumerI  = threading.Thread(target=writePipeToGPFS, args=(outdir,fi,mapPipeI))
            consumerI.start()
            mapConsumers.append(consumerI)
        else:
            mapPipeI = subprocess.Popen(mapper,shell=True,stdin=open(indir + "/" + fi),stdout=open(outdir + "/" + fi, 'w'))
        activeMappers.add(mapPipeI)

    #wait for the map tasks to finish
    for mapPipe in activeMappers:
        mapPipe.wait()
    if mapFeeders: joinAll(mapFeeders, 'map feeders')
    if mapConsumers: joinAll(mapConsumers, 'map consumers')

#
# subroutines for map-reduce
#

def setupFiles(indir,outdir):
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

def key(line):
    """Extract the key for a line containing a tab-separated key,value pair."""
    return line[:line.find("\t")]

def joinAll(xs,msg):
    """Utility to join with all threads/queues in a list."""
    logging.info('joining ' + str(len(xs))+' '+msg)
    for i,x in enumerate(xs):
        x.join()
    logging.info('joined all '+msg)

#
# routines that are attached to threads
#

def feedPipeFromGPFS(dirName,fileName,pipe):
    """Feed the lines from a gpfs shard into a subprocess,
    by writing them one by one to the stdin for that subprocess."""
    logging.info('started feeder from %s/%s to %s' % (dirName,fileName,str(pipe)))
    for line in FS.cat(dirName, fileName):
        pipe.stdin.write(line+"\n")
    pipe.stdin.close()  #so pipe process will get an EOF signal
    pipe.wait()
    logging.info('exiting feeder from %s/%s to %s' % (dirName,fileName,str(pipe)))

def writePipeToGPFS(dirName,fileName,pipe):
    """Read outputs lines from a subprocess, and
    feed them one by one into a gpfs shard."""
    logging.info('started reader to %s/%s to %s' % (dirName,fileName,str(pipe)))
    for line in pipe.stdout:
        FS.append(dirName,fileName,line.strip())
    logging.info('waiting for writing pipe to finish'+str(pipe))
    pipe.wait()
    logging.info('exiting reader to %s/%s to %s' % (dirName,fileName,str(pipe)))

def shuffleMapOutputs(mapper,mapPipe,reducerQs,numReduceTasks):
    """Thread that takes outputs of a map pipeline, hashes them, and
    sticks them on the appropriate reduce queue."""
    #maps shard index to a key->list defaultdict
    shufbuf = collections.defaultdict(lambda:collections.defaultdict(list))
    for line in mapPipe.stdout:
        k = key(line)
        h = hash(k) % numReduceTasks    # send to reducer buffer h
        shufbuf[h][k].append(line)
    logging.info('shuffleMapOutputs for '+str(mapPipe)+' sending buffer to reducerQs')
    for h in shufbuf:
        reducerQs[h].put(shufbuf[h])
    mapPipe.wait()                      # wait for termination of mapper process
    logging.info('shuffleMapOutputs for pipe '+str(mapPipe)+' done')

def acceptReduceInputs(reducerQ,reducerBuf):
    """Daemon thread that monitors a queue of items to add to a reducer
    input buffer.  Items in the buffer are grouped by key."""
    while True:
        shufbuf = reducerQ.get()
        nLines = 0
        nKeys = 0
        for key,lines in shufbuf.items():
            nLines += len(lines)
            nKeys += 1
            reducerBuf[key].extend(lines)
        logging.info('acceptReduceInputs accepted %d lines for %d keys' % (nLines,nKeys))
        reducerQ.task_done()

def sendReduceInputs(reducerBuf,reducePipe,j):
    """Thread to send contents of a reducer buffer to a reduce process."""
    for (k,lines) in reducerBuf.items():
        for line in lines:
            reducePipe.stdin.write(line)
    reducePipe.stdin.close()
    reducePipe.wait()                   # wait for termination of reducer

##############################################################################
# server/client stuff
##############################################################################

# server

from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
import urlparse

keepRunning = True

class MRSHandler(BaseHTTPRequestHandler):
    
    def _sendList(self,title,items):
        self.send_response(200)
        self.send_header('Content-type','text-html')
        self.end_headers()
        #print "leadin",leadin,"items",items
        itemList = ''
        if items:
            itemList = "\n".join(["<ul>"] + map(lambda it:"<li>%s" % it, items) + ["</ul>"])
        self.wfile.write("<html><head>%s</head>\n<body>\n%s%s\n</body></html>\n" % (title,title,itemList))

    def _sendFile(self,text):
        self.send_response(200)
        self.send_header('Content-type','text-plain')
        self.end_headers()
        self.wfile.write(text)

    def do_GET(self):
        print "GET request "+self.path
        try:
            p = urlparse.urlparse(self.path)
            requestOp = p.path
            requestArgs = urlparse.parse_qs(p.query)
            #convert the dict of lists to a dict of items, since I
            # don't use multiple values for any key
            requestArgs = dict(map(lambda (key,valueList):(key,valueList[0]), requestArgs.items()))
            print "request:",requestOp,requestArgs
            if requestOp=="shutdown":
                global keepRunning
                keepRunning = False
                self._sendFile("shutting down")
            elif requestOp=="ls" and not 'dir' in requestArgs:
                self._sendList("View listing",FS.listDirs())
            elif requestOp=="ls" and 'dir' in requestArgs:
                d = requestArgs['dir']
                self._sendList("Files in "+d,FS.listFiles(d))
            elif requestOp=="getmerge" and 'dir' in requestArgs:
                d = requestArgs['dir']
                buf = []
                for f in FS.listFiles(d):
                    buf += FS.cat(d,f)
                self._sendFile("\n".join(buf))
            elif requestOp=="append":
                d = requestArgs['dir']
                f = requestArgs['file']
                line = requestArgs['line']
                FS.append(d,f,line)
                self._sendList("Appended to "+d+"/"+f,[line])
            elif requestOp=="cat":
                d = requestArgs['dir']
                f = requestArgs['file']
                self._sendFile("\n".join(FS.cat(d,f)))
            elif requestOp=="head":
                d = requestArgs['dir']
                f = requestArgs['file']
                n = requestArgs['n']
                self._sendFile("\n".join(FS.head(d,f,int(n))))
            elif requestOp=="tail":
                d = requestArgs['dir']
                f = requestArgs['file']
                n = requestArgs['n']
                self._sendFile("\n".join(FS.tail(d,f,int(n))))
            elif requestOp=="task":
                try:
                    start = time.time()
                    performTask(requestArgs)
                    end = time.time()
                    stat =  "Task performed in %.2f sec" % (end-start)
                    print stat
                    self._sendList(stat, map(str, requestArgs.items()))
                except Exception:
                    self._sendFile(traceback.format_exc())
            else:
                self._sendList("Error: unknown command "+requestOp,[self.path])
        except KeyError:
                self._sendList("Error: illegal command",[self.path])
  
def runServer():
    server_address = ('127.0.0.1', 1969)
    httpd = HTTPServer(server_address, MRSHandler)
    print('http server is running on port 1969...')
    while keepRunning:
        httpd.handle_request()

# client

import httplib
 
def sendRequest(command):
    http_server = "127.0.0.1:1969"
    conn = httplib.HTTPConnection(http_server)
    conn.request("GET", command)
    response = conn.getresponse()
    print(response.status, response.reason)
    data_received = response.read()
    print(data_received)
    conn.close()

##############################################################################
# main
##############################################################################

def usage():
    print "usage: --serve: start server"
    print "usage: --shutdown: shutdown"
    print "usage: --send XXXX: simulate browser request http://server:port/XXXX and print response page"
    print "  where requests are ls, ls?dir=XXX, append?dir=XXX&file=YYY&line=ZZZ, cat?dir=XXX&file=YYY,"
    print "                     getmerge?dir=XXX, head?dir=XXX&file=YYY&n=NNN, tail?dir=XXX&file=YYY&n=NNN,"
    print "                     shutdown"
    print "usage: --task  --input DIR1 --output DIR2 --mapper [SHELL_COMMAND]: map-only task"
    print "       --task --input DIR1 --output DIR2 --mapper [SHELL_COMMAND] --reducer [SHELL_COMMAND] --numReduceTasks [K]: map-reduce task"
    print "  where directories DIRi are local file directories OR gpfs:XXX"

if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)

    argspec = ["serve", "send=", "shutdown", "task", "help", 
               "input=", "output=", "mapper=", "reducer=", "numReduceTasks=", "joinInputs=", ]
    optlist,args = getopt.getopt(sys.argv[1:], 'x', argspec)
    optdict = dict(optlist)
    
    if "--serve" in optdict:
        runServer()
    elif "--send" in optdict:
        sendRequest(optdict['--send'])
    elif "--shutdown" in optdict:
        sendRequest("shutdown")
    elif "--task" in optdict:
        del optdict['--task']
        sendRequest("task?" + urllib.urlencode(optdict))
    elif "--help" in optdict or (not '--input' in optdict) or (not '--output' in optdict):
        usage()
    else:
        performTask(optdict)
