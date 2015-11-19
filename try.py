import subprocess
import threading
import Queue
import sys
import time
from fcntl import fcntl, F_GETFL, F_SETFL
from os import O_NONBLOCK, read


# testing threads....ok with 2 mappers and one reducer on powerbook, not on sijo
# even 1 1 fails, at read from mpipe, even if the close operation has been performed.  
# I guess the issue is that we never know if the pipe is finished...it could
# still spit stuff out after having its stdin closed....map is not a 1-1 mapping
# 
# I guess we could use select() to see what's readable/writeable....?
#
# maybe I should have an extended thread for communications, which includes
# stream for inputs/outputs
#
# while True:
#    select to see what's readable/writeable    
#    if you can read, read 
#    else if you can, write
#    else poll and if the process is terminated, stop
#    else sleep

def key(line):
    return 1 if (line.find("hello")>=0 or line.find("bye")>=0) else 0

def makePipe(proc):
    p = subprocess.Popen(proc,shell=True, bufsize=-1,
                            stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE)    
    # set the O_NONBLOCK flag of p.stdout file descriptor:
    if False:
        flags = fcntl(p.stdout, F_GETFL) # get current p.stdout flags
        fcntl(p.stdout, F_SETFL, flags | O_NONBLOCK)
    return p

def acceptQInput(q,numMappers,pipe):
    numPoison = 0
    while True:
        try:
            task = q.get_nowait()
        except Queue.Empty:
            pass
        else:
            if task:
                key,line = task
                print 'p dequed',key,line,
                pipe.stdin.write(line)
                q.task_done()
            else:
                numPoison += 1
                print 'poison read: numPoison',numPoison
                if numPoison>=numMappers:
                    pipe.stdin.close()
                    print 'returning from acceptQInput'
                    return

def shuffleOutput(pipe,qs):
    for line in pipe.stdout:
        print "p from mpipe:",line,
        k = key(line)
        qs[k].put((k,line))
    for q in qs:
        q.put(None)

def sendToFile(pipe,filename):
    fp = open(filename,'w')
    for line in pipe.stdout:
        print "p to "+filename,line,
        fp.write(line)
    fp.close()
    print 'file sent'

def getFromFile(pipe,filename):
    for line in open(filename):
        print "p to mpipe:",line,
        pipe.stdin.write(line)
    pipe.stdin.close()
    #pipe.wait()
    print "p closed mpipe input for ",pipe


if __name__ == "__main__":

    try:
        p = int(sys.argv[1])  #how far in pipeline to use threads - 0-3
        z = int(sys.argv[2])  #start threads at once
    except Exception:
        p = 0
        z = 0
    print 'p =',p
    km = 2
    kr = 2

    mpipes = []
    for i in range(km):    
        mpipe = makePipe('cat')
        mpipes.append(mpipe)
    mpipe = None

    if p>=1:
        readers = map(lambda i: threading.Thread(target=getFromFile,args=(mpipes[i],("test%d.txt" %i ))), range(km))
        for r in readers: r.start()
        if z<1:
            print 'joining readers'
            for r in readers: r.join()
            print 'joined readers'
    else:
        for i in range(km):
            for line in open(("test%d.txt" % i )):
                mpipes[i].stdin.write(line)
            mpipes[i].stdin.close()

    rpipes = []
    for j in range(kr):
        rpipe = makePipe('sort | cat -n')
        rpipes.append(rpipe)

    if p>=2:
        qs = map(lambda i:Queue.Queue(), range(kr))
        shufflers = map(lambda i: threading.Thread(target=shuffleOutput,args=(mpipes[i],qs)), range(km))
        acceptors = map(lambda j:threading.Thread(target=acceptQInput,args=(qs[j],km,rpipes[j])), range(kr))
        for s in shufflers: s.start()
        for a in acceptors: a.start()
        if z<2:
            print 'joining acceptor, shufflers'
            for s in shufflers: s.join()
            for a in acceptors: a.join()
            print 'joined acceptor, shufflers'
    else:
        for mpipe in mpipes:
            print 'starting read from',mpipe
            for line in mpipe.stdout:
                k = key(line)
                print "s from mpipe:",k,line,
                rpipes[k].stdin.write(line)
        for j in range(kr):
            rpipes[j].stdin.close()

    if p>=3:
        writers = map(lambda j:threading.Thread(target=sendToFile,args=(rpipes[j],"tmp%d.txt" % j)), range(kr))
        for w in writers: w.start()
        if not z:
            print 'joining writers'
            for w in writers: w.join()
            print 'joined writers'
    else:
        for j in range(kr):
            for line in rpipes[j].stdout:
                print 's from rpipe',j,line,

    if z:
        print 'join readers'
        for r in readers: r.join()
        print 'join shufflers'
        for s in shufflers: s.join()
        print 'join acceptors'
        for a in acceptors: a.join()
        print 'join writers'
        for w in writers: w.start()
        print 'joined everything'


