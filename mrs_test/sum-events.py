import sys

if __name__=="__main__":
    prevKey = None
    sum = 0
    def flushBuffer(prevKey,sum): 
        if prevKey: print "%s\t%d" % (prevKey,sum)
    for line in sys.stdin:
        parts = line.strip().split("\t")
        if parts[0]==prevKey:
            sum += int(parts[1])
        else:
            flushBuffer(prevKey,sum)
            prevKey = parts[0]
            sum = int(parts[1])
    flushBuffer(prevKey,sum)

