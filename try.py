import subprocess
import threading
import Queue

def acceptQInput(q,rpipe):
    while True:
        task = q.get()
        if task:
            key,line = task
            print 'dequed',line,
            rpipe.stdin.write(line)
            q.task_done()
        else:
            rpipe.stdin.close()
            return

def shuffleOutput(pipe,q):
    for line in mpipe.stdout:
        print "from mpipe:",line,
        q.put((0,line))
        q.put(None)

def sendToFile(pipe,filename):
    fp = open(filename,'w')
    for line in pipe.stdout:
        print "to "+filename,line,
        fp.write(line)
    fp.close()

def getFromFile(pipe,filename):
    for line in open(filename):
        print "to mpipe:",line,
        pipe.stdin.write(line)
    pipe.stdin.close()
    print "closed mpipe input"

if __name__ == "__main__":
    mpipe = subprocess.Popen('cat',shell=True,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE)

    r = threading.Thread(target=getFromFile,args=(mpipe,"test.txt"))
    r.start()
    mpipe.wait()
    r.join()
    #mpipe.stdin.write("hello world\n")
    #mpipe.stdin.close()

    rpipe = subprocess.Popen('sort | cat -n',shell=True,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    q = Queue.Queue()
    a = threading.Thread(target=acceptQInput,args=(q,rpipe))
    s = threading.Thread(target=shuffleOutput,args=(mpipe,q))
    w = threading.Thread(target=sendToFile,args=(rpipe,"tmp.txt"))
    for t in [a,s,w]:
        t.start()
    for t in [q,r,a,s,w]:
        t.join()

    #for line in mpipe.stdout:
    #   print "from mpipe:",line,
    #   q.put((0,line))
    #  q.put(None)

    #for line in rpipe.stdout:
    #   print 'from rpipe',line,

