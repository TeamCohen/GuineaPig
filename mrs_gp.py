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
    def size(self,d0,f):
        d = self._fixDir(d0)
        return len(self.linesOf[(d,f)].getvalue())
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

    start = time.time()
    logging.debug('starting reduce buffer queues')
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
    logging.info('queues started: time %.3f' % (time.time()-start))

    # start the mappers - along with threads to shuffle their outputs
    # to the appropriate reducer task queue

    logging.debug('starting mapper processes and shuffler threads')
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
    logging.info('mappers started: time %.3f' % (time.time()-start))

    #wait for the map tasks, and to empty the queues
    joinAll(mappers,'mappers')        
    logging.info('mappers joined: time %.3f' % (time.time()-start))
    joinAll(reducerQs,'reduce queues')
    logging.info('reducer queues emptied: time %.3f' % (time.time()-start))

    # run the reduce processes, each of which is associated with a
    # thread that feeds it inputs from the j's reduce buffer.

    reducers = []
    for j in range(numReduceTasks):    
        reducePipeJ = subprocess.Popen("sort -k1 | "+reducer,shell=True,stdin=subprocess.PIPE,stdout=subprocess.PIPE)
        # thread to feed data into the reducer process
        uj = threading.Thread(target=runReducer, 
                              args=(usingGPFS,reducePipeJ,reducerBuffers[j],("part%05d" % j),outdir))
        uj.start()                      # uj will shut down reducePipeJ process on completion
        reducers.append(uj)
    logging.info('reducers started: time %.3f' % (time.time()-start))

    #wait for the reduce tasks
    joinAll(reducers,'reducers')
    logging.info('reducers joined: time %.3f' % (time.time()-start))

def maponly(indir,outdir,mapper):
    """Like mapreduce but for a mapper-only process."""

    usingGPFS,infiles = setupFiles(indir,outdir)

    # start the mappers - each of which is a process that reads from
    # an input file, and outputs to the corresponding output file

    start = time.time()
    logging.debug('starting mappers')
    mapThreads = []
    for fi in infiles:
        mapPipeI = subprocess.Popen(mapper,shell=True,stdin=subprocess.PIPE,stdout=subprocess.PIPE)
        mapThreadI = threading.Thread(target=runMapper, args=(usingGPFS,mapPipeI,indir,fi,outdir))
        mapThreadI.start()
        mapThreads.append(mapThreadI)
    logging.info('mappers started: time %.3f' % (time.time()-start))
    joinAll(mapThreads,'mappers')
    logging.info('mappers joined: time %.3f' % (time.time()-start))

#
# routines attached to threads
#

def runMapper(usingGPFS,mapPipe,indir,f,outdir):
    logging.debug('runMapper for file '+indir+"/"+f+" > "+outdir+"/"+f)
    inputString=getInput(usingGPFS,indir,f)
    (output,logs) = mapPipe.communicate(inputString)
    putOutput(usingGPFS,outdir,f,output)
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
    start = time.time()
    (output,logs) = mapPipe.communicate(input=getInput(usingGPFS,indir,f))
    logging.debug('shuffleMapOutputs for mapper from %s/%s communicated: time %.3f' % (indir,f,(time.time()-start)))
    buffers = []
    for h in range(numReduceTasks):
        buffers.append(cStringIO.StringIO())
    for line in cStringIO.StringIO(output):
        k = key(line)
        h = hash(k) % numReduceTasks    # send to reducer buffer h
        buffers[h].write(line)
    logging.debug('shuffleMapOutputs for mapper from %s/%s buffered: time %.3f' % (indir,f,(time.time()-start)))
    mapPipe.wait()                      # wait for termination of mapper process
    logging.debug('shuffleMapOutputs for mapper from %s/%s wait complete: time %.3f' % (indir,f,(time.time()-start)))
    for h in range(numReduceTasks):    
        if buffers[h]: reducerQs[h].put(buffers[h].getvalue())
    logging.info('shuffleMapOutputs for mapper from  %s/%s done: time %.3f' % (indir,f,(time.time()-start)))

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
import urlparse

keepRunning = True

class MRSHandler(BaseHTTPRequestHandler):
    
    def _sendList(self,items,html):
        if not html:
            self._sendFile("\n".join(items) + "\n")
        else:
            def markup(it):
                if it.startswith(" files:"):
                    keyword,n,f = it.split()
                    return it + (' ' * (30-len(f))) + '[<a href="/ls?html=1&dir=%s">ls %s</a>]' % (f,f)
                elif it.startswith(" chars:"):
                    keyword,n,df = it.split()
                    d,f = df.split("/")
                    return it + (' ' * (30-len(df))) \
                        + '[<a href="/cat?html=1&dir=%s&file=%s">cat</a>' % (d,f) \
                        + '|<a href="/cat?html=1&dir=%s&file=%s">head</a>' % (d,f) \
                        + '|<a href="/cat?html=1&dir=%s&file=%s">tail</a>]' % (d,f)
            self.send_response(200)
            self.send_header('Content-type','text/html')
            self.end_headers()            
            self.wfile.write("<html><head><title>MRS GP Output</title></head>\n")
            self.wfile.write("<body>\n")
            self.wfile.write("<hr/>\n")
            self.wfile.write("<pre>\n")
            for it in items:
                self.wfile.write(markup(it) + "\n")
            self.wfile.write("</pre>\n")
            self.wfile.write("<hr/>\n")
            self.wfile.write('<a href="/ls?html=1">List top level</a>')
            self.wfile.write("</body></html>\n")

    def _sendFile(self,text):
        self.send_response(200)
        self.send_header('Content-type','text/plain')
        self.end_headers()
        self.wfile.write(text)
    
    # turn off request logging
    def log_request(self,code=0,size=0):
        pass

    def do_GET(self):
        #print "GET request "+self.path
        global keepRunning
        try:
            p = urlparse.urlparse(self.path)
            requestOp = p.path
            requestArgs = urlparse.parse_qs(p.query)
            #convert the dict of lists to a dict of items, since I
            # don't use multiple values for any key
            requestArgs = dict(map(lambda (key,valueList):(key,valueList[0]), requestArgs.items()))
            html = 'html' in requestArgs
            #print "request:",requestOp,requestArgs
            if requestOp=="/shutdown":
                global keepRunning
                keepRunning = False
                self._sendFile("Shutting down")
            elif requestOp=="/ls" and not 'dir' in requestArgs:
                def fmtdir(d): return ' files: %3d  %s' % (len(FS.listFiles(d)),d)
                self._sendList(map(fmtdir, FS.listDirs()),html)
            elif requestOp=="/ls" and 'dir' in requestArgs:
                d = requestArgs['dir']
                def fmtfile(f): return ' chars: %6d  %s/%s' % (len(FS.cat(d,f)),d,f)
                self._sendList(map(fmtfile,FS.listFiles(d)),html)
            elif requestOp=="/getmerge" and 'dir' in requestArgs:
                d = requestArgs['dir']
                buf = "".join(map(lambda f:FS.cat(d,f),FS.listFiles(d)))
                self._sendFile(buf)
            elif requestOp=="/write":
                d = requestArgs['dir']
                f = requestArgs['file']
                line = requestArgs['line']
                FS.write(d,f,line+'\n')
                self._sendFile("Line '%s' written to %s/%s" % (line,d,f))
            elif requestOp=="/cat":
                d = requestArgs['dir']
                f = requestArgs['file']
                self._sendFile(FS.cat(d,f))
            elif requestOp=="/head":
                d = requestArgs['dir']
                f = requestArgs['file']
                n = int(requestArgs.get('n','2048'))
                self._sendFile(FS.head(d,f,n))
            elif requestOp=="/tail":
                d = requestArgs['dir']
                f = requestArgs['file']
                n = int(requestArgs.get('n','2048'))
                self._sendFile(FS.tail(d,f,n))
            elif requestOp=="/task":
                try:
                    start = time.time()
                    performTask(requestArgs)
                    end = time.time()
                    stat =  "Task performed in %.2f sec" % (end-start)
                    self._sendList(map(str, requestArgs.items()) + [stat],html)
                except Exception:
                    self._sendFile(traceback.format_exc())
            else:
                self.send_error(400,"Unknown command '"+requestOp + "' in request '"+self.path+"'")
        except KeyError:
                self.send_error(400,"Illegal request "+self.path)
  
def runServer():
    #allow only access from local machine
    server_address = ('127.0.0.1', 1969)
    #allow access from anywhere
    #server_address = ('0.0.0.0', 1969)    
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
    if response.status==200:
        data_received = response.read()
        conn.close()
        print data_received,
    else:
        conn.close()        
        raise Exception('%d %s' % (response.status,response.reason))

##############################################################################
# main
##############################################################################

def usage():
    print "usage: --serve: start server"
    print "usage: --shutdown: shutdown"
    print "usage: --fs ... "
    print "  where commands are: ls, ls DIR, write DIR FILE LINE, cat DIR FILE, getmerge DIR, head DIR FILE N, tail  DIR FILE N",
    print "usage: --send XXXX: simulate browser request http://server:port/XXXX and print response page"
    print "  where requests are ls, ls?dir=XXX, write?dir=XXX&file=YYY&line=ZZZ, cat?dir=XXX&file=YYY,"
    print "                     getmerge?dir=XXX, head?dir=XXX&file=YYY&n=NNN, tail?dir=XXX&file=YYY&n=NNN,"
    print "                     shutdown -- and option 'html' means html output"
    print "  this is mainly for testing"
    print "usage: --task  --input DIR1 --output DIR2 --mapper [SHELL_COMMAND]: map-only task"
    print "       --task --input DIR1 --output DIR2 --mapper [SHELL_COMMAND] --reducer [SHELL_COMMAND] --numReduceTasks [K]: map-reduce task"
    print "  where directories DIRi are local file directories OR gpfs:XXX"
    print "  same options w/o --task will run the commands locally, not on the server"

if __name__ == "__main__":

    argspec = ["serve", "send=", "shutdown", "task", "help", "fs",
               "input=", "output=", "mapper=", "reducer=", "numReduceTasks=", "joinInputs=", ]
    optlist,args = getopt.getopt(sys.argv[1:], 'x', argspec)
    optdict = dict(optlist)
    #print optdict,args
    
    if "--serve" in optdict:
        # log server to a file, since it runs in the background...
        logging.basicConfig(filename="server.log",level=logging.INFO)
        logging.info("server started....")
        runServer()
    else:
        logging.basicConfig(level=logging.INFO)        
        if "--send" in optdict:
            sendRequest(optdict['--send'])
        elif "--shutdown" in optdict:
            sendRequest("/shutdown")
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
                if len(args)>2: request += "&file="+args[2]
                if len(args)>3 and args[0]=="write": request += "&line="+args[3]
                elif len(args)>3: request += "&n="+args[3]
                #print "request: "+request
                sendRequest(request)
        else:
            if (not '--input' in optdict) or (not '--output' in optdict):
                usage()
            else:
                performTask(optdict)

