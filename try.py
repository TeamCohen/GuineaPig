import subprocess
import threading
import Queue
import sys

# testing threads....ok with 2 mappers and one reducer

def key(line):
    return 1 if (line.find("hello")>=0 or line.find("bye")>=0) else 0

def acceptQInput(q,numMappers,pipe):
    numPoison = 0
    while True:
        task = q.get()
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

def getFromFile(pipe,filename):
    for line in open(filename):
        print "p to mpipe:",line,
        pipe.stdin.write(line)
    pipe.stdin.close()
    pipe.wait()
    print "p closed mpipe input"


if __name__ == "__main__":

    try:
        p = int(sys.argv[1])
    except Exception:
        p = 0
    print 'p =',p
    km = 2
    kr = 2

    mpipes = []
    for i in range(km):    
        mpipe = subprocess.Popen('cat',shell=True,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        mpipes.append(mpipe)
    mpipe = None

    if p>=1:
        readers = map(lambda i: threading.Thread(target=getFromFile,args=(mpipes[i],("test%d.txt" %i ))), range(km))
        if p<10:
            for r in readers: r.start()
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
        rpipe = subprocess.Popen('sort | cat -n',shell=True,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        rpipes.append(rpipe)

    if p>=2:
        qs = map(lambda i:Queue.Queue(), range(kr))
        shufflers = map(lambda i: threading.Thread(target=shuffleOutput,args=(mpipes[i],qs)), range(km))
        acceptors = map(lambda j:threading.Thread(target=acceptQInput,args=(qs[j],km,rpipes[j])), range(kr))
        if p<10:
            for s in shufflers: s.start()
            for a in acceptors: a.start()
            print 'joining acceptor, shufflers'
            for s in shufflers: s.join()
            for a in acceptors: a.join()
            print 'joined acceptor, shufflers'
    else:
        for mpipe in mpipes:
            for line in mpipe.stdout:
                k = key(line)
                print "s from mpipe:",k,line,
                rpipes[k].stdin.write(line)
        for j in range(kr):
            rpipes[j].stdin.close()

    if p>=3:
        writers = map(lambda j:threading.Thread(target=sendToFile,args=(rpipes[j],"tmp%d.txt" % j)), range(kr))
        if p<10:
            for w in writers: w.start()
            for w in writers: w.join()
    else:
        for j in range(kr):
            for line in rpipes[j].stdout:
                print 's from rpipe',j,line,

    if p>=10:
        for r in readers: r.start()
        for s in shufflers: s.start()
        for a in acceptors: a.start()
        for w in writers: w.start()

        for r in readers: r.join()
        for s in shufflers: s.join()
        for a in acceptors: a.join()
        for w in writers: w.join()


