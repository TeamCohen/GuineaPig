import sys
import logging

if __name__=="__main__":
    prevKey = None
    sum = 0
    def flushBuffer(prevKey,sum): 
        if prevKey: print "%s\t%d" % (prevKey,sum)
    for line in sys.stdin:
        try:
            parts = line.strip().split("\t")
            if parts[0]==prevKey:
                sum += int(parts[1])
            else:
                flushBuffer(prevKey,sum)
                prevKey = parts[0]
                sum = int(parts[1])
        except ValueError:
            logging.warn('sum-events: error on line: '+line.strip())
    flushBuffer(prevKey,sum)

