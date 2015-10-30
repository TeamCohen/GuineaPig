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

# Map Reduce Streaming for GuineaPig (mrs_gp) - very simple
# multi-threading map-reduce, to be used with inputs/outputs which are
# just files in directories.  This combines multi-threading and
# multi-processing.
#
# For i/o bound tasks the inputs should be on ramdisk.  
#
# To do:
#  map-only tasks, multiple map inputs
#  secondary grouping sort key --joinmode - see blist.sortedlist
#  optimize?

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
        tj = threading.Thread(target=accumulateReduceInputs, args=(qj,bj))
        tj.daemon = True                # loops forever, will be killed on exit
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
    joinAll(reducerQs,'reduce queues')  # the queues have been emptied

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

def shuffleMapOutputs(mapper,mapPipe,reducerQs,numReduceTasks):
    """Thread that takes outputs of a map pipeline, hashes them, and
    sticks them on the appropriate reduce queue."""
    for line in mapPipe.stdout:
        k = key(line)
        h = hash(k) % numReduceTasks    # send to reducer buffer h
        reducerQs[h].put((k,line))
    mapPipe.wait()                      # wait for termination of mapper process

def accumulateReduceInputs(reducerQ,reducerBuf):
    """Daemon thread that monitors a queue of items to add to a reducer
    input buffer.  Items in the buffer are grouped by key."""
    while True:
        (k,line) = reducerQ.get()
        reducerBuf[k] += [line]
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

#
# main
#

def usage():
    print "usage: --input DIR1 --output DIR2 --mapper [SHELL_COMMAND]"
    print "       --input DIR1 --output DIR2 --mapper [SHELL_COMMAND] --reducer [SHELL_COMMAND] --numReduceTasks [K]"

if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)

    argspec = ["input=", "output=", "mapper=", "reducer=", "numReduceTasks=", "joinInputs=", "help"]
    optlist,args = getopt.getopt(sys.argv[1:], 'x', argspec)
    optdict = dict(optlist)
    
    if "--help" in optdict or (not '--input' in optdict) or (not '--output' in optdict):
        usage()
        sys.exit(-1)

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

