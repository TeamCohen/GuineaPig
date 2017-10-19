from guineapig import *
import sys
import os

EDGEFILE = 'citeseer-graph.txt'
TMPFILE = 'tmp-pr-graph.txt'
RESET = 0.15
N = 10

def pageRankPlanner():

    p = Planner()

    def serialize(graphView):
        return \
            Format(graphView,
                   by=lambda(url,pagerank,outlinks):'\t'.join([url,'%g'%pagerank]+outlinks))

    # read in and create initial ranked graph
    p.edges = ReadLines(EDGEFILE) | Map(by=lambda line:line.strip().split(' '))
    p.initGraph = Group(p.edges, by=lambda (src,dst):src, retaining=lambda(src,dst):dst)
    p.initRankedGraph = Map(p.initGraph, by=lambda (url,outlinks):(url,1.0,outlinks))
    p.serializedInitRankedGraph = serialize(p.initRankedGraph)

    # one step of the update, reading the last iteration from a temp file
    p.prevGraph = \
        ReadLines(TMPFILE) \
        | Map(by=lambda line:line.strip().split("\t")) \
        | Map(by=lambda parts:(parts[0],float(parts[1]),parts[2:]))
    p.outboundPageRankMessages =  \
        FlatMap(p.prevGraph, 
                by=lambda (url,pagerank,outlinks): 
                map(lambda dst:(dst,pagerank/len(outlinks)), outlinks))
    p.newPageRank = \
        Group(p.outboundPageRankMessages,
              by=lambda (dst,deltaPageRank):dst, 
              retaining=lambda (dst,deltaPageRank):deltaPageRank,
              reducingTo=ReduceTo(lambda:(RESET), lambda accum,delta:accum + (1-RESET)*delta))
    p.newRankedGraph = \
        Join( Jin(p.prevGraph, by=lambda (url,pagerank,outlinks):url), 
              Jin(p.newPageRank, by=lambda (dst,newPageRank):dst)) \
        | Map(by=lambda((url,oldPageRank,outlinks),(url_,newPageRank)):(url,newPageRank,outlinks))
    p.serializedRankedGraph = serialize(p.newRankedGraph)

    p.setup()
    return p

if __name__ == "__main__":

    #run subplan step, if called recursively
    if Planner.partOfPlan(sys.argv) or len(sys.argv)!=1:
        pageRankPlanner().main(sys.argv)  

    else: 

        #create the initial graph w pagerank
        planner = pageRankPlanner()
        v0 = planner.getView('serializedInitRankedGraph')
        v0.storagePlan().execute(planner)
        print '>>> moving initial pageranks to',TMPFILE
        os.rename(v0.storedFile(), TMPFILE)

        for i in range(N):
            print '>>> pagerank iteration',i
            vi = planner.getView('serializedRankedGraph')
            vi.storagePlan().execute(planner)
            print '>>> moving round',i,'pageranks to',TMPFILE
            os.rename(vi.storedFile(), TMPFILE)
