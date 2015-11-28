from guineapig import *
import sys

# In a standard ReplaceEach, the 'by' function is stateless - it
# performs the same operation on each row.  ReplaceEachPartition has a
# 'by' function which iterates over the rows, and yields a series of
# replacements. If the by function is a closure, then it can have
# local state.  This file has a couple of examples of that.

# MakeIdAdder will replaces every line x in a file with a pair (n,x)
# where n is the line number for x, starting at 1.

def MakeIdAdder(startId):
    #create a closure that keeps the local state
    def idAdder(lines):
        nextId = startId
        for line in lines:
            yield (nextId,line)
            nextId += 1
    #return the closure
    return idAdder
    

# LineCounter will yield a single item, which is the number of lines
# in the partition.

def LineCounter():
    def counter(lines):
        n = 0
        for line in lines:
            n += 1
        yield n
    return counter

class AddIds(Planner):

    result = ReadLines('corpus.txt') | ReplaceEachPartition(by=MakeIdAdder(1))
    count = ReadLines('corpus.txt') | ReplaceEachPartition(by=LineCounter())

# always end like this
if __name__ == "__main__":
    AddIds().main(sys.argv)
