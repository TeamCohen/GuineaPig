from guineapig import *

# compute TFIDF in Guineapig
#
# Optimized to use sideviews for the relations that
# are only the size of the vocabulary
# 
# sample invocation:
# % python smallvoc-tfidf-simplified.py --params input:dbpedia/withIds,output:dbpedia/docvec.gp --store docvec
#

import sys
import math

def loadAsDict(view):
    result = {}
    for (key,val) in GPig.rowsOf(view):
        result[key] = val
    return result

class TFIDF(Planner):
    
    data = ReadLines('idcorpus.txt') \
        | Map(by=lambda line:line.strip().split("\t")) \
        | Map(by=lambda (docid,doc): (docid,doc.lower().split())) \
        | FlatMap(by=lambda (docid,words): map(lambda w:(docid,w),words))

    #compute document frequency and inverse doc freq
    docFreq = Distinct(data) \
        | Group(by=lambda (docid,term):term, \
                retaining=lambda x:1, \
                reducingTo=ReduceToSum())

    # definitely use combiners when you aggregate
    ndoc = Map(data, by=lambda (docid,term):docid) \
        | Distinct() \
        | Group(by=lambda row:'ndoc', retaining=lambda x:1, combiningTo=ReduceToSum(), reducingTo=ReduceToSum())

    # convert raw docFreq to idf
    inverseDocFreq = Augment(docFreq, sideview=ndoc, loadedBy=lambda v:GPig.onlyRowOf(v)) \
        | Map(by=lambda((term,df),(dummy,ndoc)):(term,math.log(ndoc/df)))

    #compute unweighted document vectors with a map-side join
    udocvec = Augment(data, sideview=inverseDocFreq, loadedBy=loadAsDict) \
        | Map(by=lambda ((docid,term),idfDict):(docid,term,idfDict[term]))

    #normalize
    norm = Group(udocvec, 
                 by=lambda(docid,term,weight):docid, 
                 retaining=lambda(docid,term,weight):weight*weight,
                 reducingTo=ReduceToSum() )

    docvec = Augment(udocvec, sideview=norm, loadedBy=loadAsDict) \
        | Map( by=lambda ((docid,term,weight),normDict): (docid,term,weight/math.sqrt(normDict[docid])))

## always end like this
if __name__ == "__main__":
    p = TFIDF()
    p.main(sys.argv)  
