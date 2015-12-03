from guineapig import *
import sys
import gpextras

def partitionCounter(rows):
    n = 0
    for r in rows: n+= 1
    yield n

class NaiveBayes(Planner):

    D = GPig.getArgvParams(required=['train'])
    def sumEventCounts(v):
        return Group(v, by=lambda (e,n):e, retaining=lambda (e,n):n, reducingTo=ReduceToSum(), combiningTo=ReduceToSum())
    def count(v,tag):
        return ReplaceEachPartition(v, by=partitionCounter) | Group(by=lambda n:tag, reducingTo=ReduceToSum())

    #format: (id,classLabelList,tokenList)
    data = ReadLines(D['train']) \
        | ReplaceEach(by=lambda line:line.strip().split("\t")) \
        | ReplaceEach(by=lambda parts:(parts[0],parts[1].split(","),parts[2:]))
    labelEvents = sumEventCounts(Flatten(data, by=lambda (docid,ys,ws): [(y,1) for y in ys]))
    wordLabelEvents = sumEventCounts(Flatten(data, by=lambda (docid,ys,ws): [(y+'/'+w,1) for y in ys for w in ws]))
    totalLines = count(data,'#lines')
    totalWords = count(Flatten(data, lambda (docid,ys,ws): ws), '#words')

# always end like this
if __name__ == "__main__":
    p = NaiveBayes()
    p.registerCompiler('mrs',gpextras.MRSCompiler)
    p.main(sys.argv)

