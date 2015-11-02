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

##############################################################################
# Map Reduce Streaming for GuineaPig (mrs_gp) - very simple
# multi-threading map-reduce, to be used with inputs/outputs which are
# just files in directories.  This combines multi-threading and
# multi-processing.
#
# For i/o bound tasks the inputs should be on ramdisk.  
#
# To do:
#  map-only tasks, multiple map inputs
#  secondary grouping sort key --joinmode
#  optimize - keys
##############################################################################

def mapreduce(indir,outdir,mapper,reducer,numReduceTasks):
    """Run a generic streaming map-reduce process.  The mapper and reducer
    are shell commands, as in Hadoop streaming.  Both indir and outdir
    are directories."""

    infiles = setupFiles(indir,outdir)

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
    # an input file, and a thread that passes its outputs to the
    # reducer queues.

    logging.info('starting mapper processes and shuffler threads')
    mappers = []
    for fi in infiles:
        # WARNING: it doesn't seem to work well to start the processes
        # inside a thread - this led to bugs with the reducer
        # processes.  This is possibly a python library bug:
        # http://bugs.python.org/issue1404925
        mapPipe = subprocess.Popen(mapper,shell=True,stdin=open(indir + "/" + fi),stdout=subprocess.PIPE)
        si = threading.Thread(target=shuffleMapOutputs, args=(mapper,mapPipe,reducerQs,numReduceTasks))
        si.start()                      # si will join the mapPipe process
        mappers.append(si)

    #wait for the map tasks, and to empty the queues
    joinAll(mappers,'mappers')          # no more tasks will be added to the queues

    # run the reduce processes, each of which is associated with a
    # thread that feeds it inputs from the j's reduce buffer.

    logging.info('starting reduce processes and threads to feed these processes')
    reducers = []
    for j in range(numReduceTasks):    
        fpj = open("%s/part%05d" % (outdir,j), 'w')
        reducePipeJ = subprocess.Popen(reducer,shell=True,stdin=subprocess.PIPE,stdout=fpj)
        uj = threading.Thread(target=sendReduceInputs, args=(reducerBuffers[j],reducePipeJ,j))
        uj.start()                      # uj will shut down reducePipeJ process on completion
        reducers.append(uj)

    #wait for the reduce tasks
    joinAll(reducerQs,'reduce queues')  # the queues have been emptied
    joinAll(reducers,'reducers')

def maponly(indir,outdir,mapper):
    """Like mapreduce but for a mapper-only process."""

    infiles = setupFiles(indir,outdir)

    # start the mappers - each of which is a process that reads from
    # an input file, and outputs to the corresponding output file

    logging.info('starting mapper processes')
    activeMappers = set()
    for fi in infiles:
        mapPipe = subprocess.Popen(mapper,shell=True,stdin=open(indir + "/" + fi),stdout=open(outdir + "/" + fi, 'w'))
        activeMappers.add(mapPipe)

    #wait for the map tasks to finish
    for mapPipe in activeMappers:
        mapPipe.wait()

#
# subroutines
#

def setupFiles(indir,outdir):
    infiles = [f for f in os.listdir(indir)]
    if os.path.exists(outdir):
        logging.warn('removing %s' % (outdir))
        shutil.rmtree(outdir)
    os.makedirs(outdir)
    logging.info('inputs: %d files from %s' % (len(infiles),indir))
    return infiles

#
# routines attached to threads
#

def shuffleMapOutputs_v1(mapper,mapPipe,reducerQs,numReduceTasks):
    """Thread that takes outputs of a map pipeline, hashes them, and
    sticks them on the appropriate reduce queue."""
    for line in mapPipe.stdout:
        k = key(line)
        h = hash(k) % numReduceTasks    # send to reducer buffer h
        reducerQs[h].put((k,line))
    logging.info('shuffleMapOutputs for pipe '+str(mapPipe)+' finishes reading, now waiting')
    mapPipe.wait()                      # wait for termination of mapper process
    logging.info('shuffleMapOutputs for pipe '+str(mapPipe)+' done waiting')

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

def accumulateReduceInputs_v1(reducerQ,reducerBuf):
    """Daemon thread that monitors a queue of items to add to a reducer
    input buffer.  Items in the buffer are grouped by key."""
    while True:
        (k,line) = reducerQ.get()
        reducerBuf[k].append(line)
        reducerQ.task_done()

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

#
# utils
#

# TODO make this faster

def key(line):
    """Extract the key for a line containing a key,value pair."""
    parts = line.strip().split("\t")
    return parts[0]

def joinAll(xs,msg):
    """Utility to join with all threads/queues in a list."""
    logging.info('joining ' + str(len(xs))+' '+msg)
    for i,x in enumerate(xs):
        x.join()
    logging.info('joined all '+msg)

##############################################################################
# virtual filesystem
##############################################################################

class TrivialFileSystem(object):

    def __init__(self):
        #file names in directory/shards
        self.filesIn = collections.defaultdict(list)
        #content of (dir,file)
        self.linesOf = {}
    def rmDir(self,d):
        for f in self.filesIn[d]:
            del self.linesOf[(d,f)]
        del self.filesIn[d]
    def append(self,d,f,line):
        if not f in self.filesIn[d]:
            self.filesIn[d].append(f)
            self.linesOf[(d,f)] = list()
        self.linesOf[(d,f)].append(line)
    def listDirs(self):
        return self.filesIn.keys()
    def listFiles(self,d):
        return self.filesIn[d]
    def cat(self,d,f):
        return self.linesOf[(d,f)]
    def head(self,d,f,n):
        return self.linesOf(d,f)[:n]
    def tail(self,d,f,n):
        return self.linesOf(d,f)[-n:]

FS = TrivialFileSystem()

##############################################################################
# server/client stuff
##############################################################################

# server

from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer

class MRSHandler(BaseHTTPRequestHandler):
    
    def _sendList(self,title,leadin,items):
        self.send_response(200)
        self.send_header('Content-type','text-html')
        self.end_headers()
        itemList = ''
        if items:
            itemList = "\n".join(["<ul>"] + map(lambda it:"<li>%s" % it, itemList) + ["</ul>"])
        self.wfile.write("<html><head>%s</head>\n<body>\n%s%s\n</body></html>\n" % (title,leadin,itemList))

    def _sendFile(self,text):
        self.send_response(200)
        self.send_header('Content-type','text-plain')
        self.end_headers()
        self.wfile.write(text)

    def do_GET(self):
        print "GET request "+self.path
        try:
            parts = self.path.split("/")
            print "parts  = ",parts
            operation = parts[0]
            args = parts[1:]
            if operation=="ls" and not args:
                self._sendList("Views defined","Views",FS.listDirs())
            elif operation=="ls" and args:
                d = args[0]
                self._sendList("Files in "+d,"Files in "+d,FS.listFiles(d))
            elif operation=="append":
                d,f,line = args
                self._sendList("Appended to "+d+"/"+f,"Appended to "+d+"/"+f,[])
            elif operation=="cat":
                d,f = args
                self._sendFile("\n".join(FS.cat(d,f)))
            elif operation=="head":
                d,f,n = args
                self._sendFile("\n".join(FS.head(d,f,int(n))))
            elif operation=="tail":
                d,f,n = args
                self._sendFile("\n".join(FS.tail(d,f,int(n))))
            else:
                self._sendList("Error","unknown command "+self.path,[])
        except ValueError:
                self._sendList("Error","illegal command "+self.path,[])
  
def runServer():
    server_address = ('127.0.0.1', 1969)
    httpd = HTTPServer(server_address, MRSHandler)
    print('http server is running on port 1969...')
    httpd.serve_forever()

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
    print "usage: --serve [PORT]"
    print "usage: --send command"
    print "usage: --input DIR1 --output DIR2 --mapper [SHELL_COMMAND]"
    print "       --input DIR1 --output DIR2 --mapper [SHELL_COMMAND] --reducer [SHELL_COMMAND] --numReduceTasks [K]"

if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)

    argspec = ["serve", "send=", "input=", "output=", "mapper=", "reducer=", "numReduceTasks=", "joinInputs=", "help"]
    optlist,args = getopt.getopt(sys.argv[1:], 'x', argspec)
    optdict = dict(optlist)
    
    if "--serve" in optdict:
        runServer()
    if "--send" in optdict:
        sendRequest(optdict['--send'])
    elif "--help" in optdict or (not '--input' in optdict) or (not '--output' in optdict):
        usage()
    else:

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
