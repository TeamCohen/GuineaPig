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
    def write(self,d0,f,line):
        d = self._fixDir(d0)
        if not f in self.filesIn[d]:
            self.filesIn[d].append(f)
            self.linesOf[(d,f)] = cStringIO.StringIO()
        self.linesOf[(d,f)].write(line)
    def listDirs(self):
        return self.filesIn.keys()
    def listFiles(self,d0):
        d = self._fixDir(d0)
        return self.filesIn[d]
    def cat(self,d0,f):
        d = self._fixDir(d0)
        return self.linesOf[(d,f)].getvalue()
    def head(self,d0,f,n):
        d = self._fixDir(d0)
        return self.linesOf[(d,f)].getvalue()[:n]
    def tail(self,d0,f,n):
        d = self._fixDir(d0)
        return self.linesOf[(d,f)].getvalue()[-n:]
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
    # is a buffer bj, which contains lines for shard K,

    logging.info('starting reduce buffer queues')
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

    # start the mappers - along with threads to shuffle their outputs
    # to the appropriate reducer task queue

    logging.info('starting mapper processes and shuffler threads')
    mappers = []
    for fi in infiles:
        # WARNING: it doesn't seem to work well to start the processes
        # inside a thread - this led to bugs with the reducer
        # processes.  This is possibly a python library bug:
        # http://bugs.python.org/issue1404925
        mapPipeI = subprocess.Popen(mapper,shell=True,stdin=subprocess.PIPE,stdout=subprocess.PIPE)
        si = threading.Thread(target=shuffleMapOutputs, args=(usingGPFS,numReduceTasks,mapPipeI,indir,fi,reducerQs))
        si.start()                      # si will join the mapPipe process
        mappers.append(si)

    #wait for the map tasks, and to empty the queues
    joinAll(mappers,'mappers')        
    joinAll(reducerQs,'reduce queues')

    # run the reduce processes, each of which is associated with a
    # thread that feeds it inputs from the j's reduce buffer.

    logging.info('starting reduce processes and threads to feed these processes')
    reducers = []
    for j in range(numReduceTasks):    
        reducePipeJ = subprocess.Popen("sort -k1 | "+reducer,shell=True,stdin=subprocess.PIPE,stdout=subprocess.PIPE)
        # thread to feed data into the reducer process
        uj = threading.Thread(target=runReducer, 
                              args=(usingGPFS,reducePipeJ,reducerBuffers[j],("part%05d" % j),outdir))
        uj.start()                      # uj will shut down reducePipeJ process on completion
        reducers.append(uj)

    #wait for the reduce tasks
    joinAll(reducers,'reducers')

def maponly(indir,outdir,mapper):
    """Like mapreduce but for a mapper-only process."""

    usingGPFS,infiles = setupFiles(indir,outdir)

    # start the mappers - each of which is a process that reads from
    # an input file, and outputs to the corresponding output file

    logging.info('starting mappers')
    mapThreads = []
    for fi in infiles:
        mapPipeI = subprocess.Popen(mapper,shell=True,stdin=subprocess.PIPE,stdout=subprocess.PIPE)
        mapThreadI = threading.Thread(target=runMapper, args=(usingGPFS,mapPipeI,indir,fi,outdir))
        mapThreadI.start()
        mapThreads.append(mapThreadI)
    joinAll(mapThreads,'mappers')

#
# routines attached to threads
#

def runMapper(usingGPFS,mapPipe,indir,f,outdir):
    logging.info('runMapper for file '+indir+"/"+f+" > "+outdir+"/"+f)
    inputString=getInput(usingGPFS,indir,f)
    #logging.info('before map inputString is >>'+inputString+"<<")
    (output,logs) = mapPipe.communicate(inputString)
    putOutput(usingGPFS,outdir,f,output)
    #logging.info('after output output is >>'+FS.cat(outdir,f)+"<<")
    #logging.info("output=inputString is "+(output==inputString))
    mapPipe.wait()

def runReducer(usingGPFS,redPipe,buf,f,outdir):
    (output,logs) = redPipe.communicate(input=buf.getvalue())
    putOutput(usingGPFS,outdir,f,output)
    redPipe.wait()

def shuffleMapOutputs(usingGPFS,numReduceTasks,mapPipe,indir,f,reducerQs):
    """Thread that takes outputs of a map pipeline, hashes them, and
    sends them in the appropriate reduce queue."""
    #maps shard index to a key->list defaultdict
    logging.info('shuffleMapOutputs for mapper from %s/%s started' % (indir,f))
    (output,logs) = mapPipe.communicate(input=getInput(usingGPFS,indir,f))
    logging.info('shuffleMapOutputs for mapper from %s/%s communicated' % (indir,f))
    for line in cStringIO.StringIO(output):
        k = key(line)
        h = hash(k) % numReduceTasks    # send to reducer buffer h
        reducerQs[h].put(line)
    mapPipe.wait()                      # wait for termination of mapper process
    logging.info('shuffleMapOutputs for pipe '+str(mapPipe)+' done')

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

def getInput(usingGPFS,indir,f):
    if 'input' in usingGPFS:
        return FS.cat(indir,f)
    else:
        logging.info('loading lines from '+indir+"/"+f)
        inputString = cStringIO.StringIO()
        k = 0
        for line in open(indir+"/"+f):
            inputString.write(line)
            k += 1
            if k%10000==0: logging.info('loaded '+str(k)+' lines')
        logging.info('loaded '+indir+"/"+f)
        return inputString.getvalue()

def putOutput(usingGPFS,outdir,f,outputString):
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
    logging.info('joining ' + str(len(xs))+' '+msg)
    for i,x in enumerate(xs):
        x.join()
    logging.info('joined all '+msg)


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
                buf = ""
                for f in FS.listFiles(d):
                    buf += FS.cat(d,f)
                #logging.info("buf is >>"+buf+"<<")
                self._sendFile(buf)
            elif requestOp=="write":
                d = requestArgs['dir']
                f = requestArgs['file']
                line = requestArgs['line']
                FS.write(d,f,line+'\n')
                self._sendList("Appended to "+d+"/"+f,[line])
            elif requestOp=="cat":
                d = requestArgs['dir']
                f = requestArgs['file']
                self._sendFile(FS.cat(d,f))
            elif requestOp=="head":
                d = requestArgs['dir']
                f = requestArgs['file']
                n = int(requestArgs.get('n','512'))
                self._sendFile(FS.head(d,f,n))
            elif requestOp=="tail":
                d = requestArgs['dir']
                f = requestArgs['file']
                n = int(requestArgs.get('n','512'))
                self._sendFile(FS.tail(d,f,n))
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
    print "  where requests are ls, ls?dir=XXX, write?dir=XXX&file=YYY&line=ZZZ, cat?dir=XXX&file=YYY,"
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
