from guineapig import *

# compute TFIDF in Guineapig
#
# Optimized to use sideviews for the normalized weights and
# docFrequencies, combiners, and compression.  Note that sideviews
# can't be compressed if you want to use the standard GPig.rowsOf()
# loading methods, so this is also of a test case for the hadoop
# options. Also includes the new storeAt option, to export the final
# weighted terms.
# 
# sample invocation:
# % python smallvoc-tfidf.py --opts target:hadoop,parallel:100,echo:1 \
#          --params input:dbpedia/withIds,output:dbpedia/docvec.gp --store docvecExport
#


import sys
import math

COMPRESSED=['-jobconf','mapred.output.compress=true', '-jobconf','mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec']
UNCOMPRESSED=['-jobconf','mapred.output.compress=false']

def loadDictView(view):
    result = {}
    for (key,val) in GPig.rowsOf(view):
        result[key] = val
    return result

class TFIDF(Planner):
    
    D = GPig.getArgvParams(required=['input','output'])
    data = ReadLines(D['input']) \
        | Map(by=lambda line:line.strip().split("\t")) \
        | Map(by=lambda (docid,doc): (docid,doc.lower().split())) \
        | FlatMap(by=lambda (docid,words): map(lambda w:(docid,w),words))

    #compute document frequency and inverse doc freq
    docFreq = Distinct(data) \
        | Group(by=lambda (docid,term):term, \
                retaining=lambda x:1, \
                combiningTo=ReduceToSum(), \
                reducingTo=ReduceToSum())
    docFreq.opts(hopts=UNCOMPRESSED,parallel=20)

    ndoc = Map(data, by=lambda (docid,term):docid) \
        | Distinct() \
        | Group(by=lambda row:'ndoc', retaining=lambda x:1, reducingTo=ReduceToSum(), combiningTo=ReduceToSum())
    ndoc.opts(hopts=UNCOMPRESSED,parallel=20)

    inverseDocFreq = Augment(docFreq, sideview=ndoc, loadedBy=lambda v:GPig.onlyRowOf(v)) \
        | Map(by=lambda((term,df),(dummy,ndoc)):(term,math.log(ndoc/df)))
    inverseDocFreq.opts(hopts=UNCOMPRESSED)

    #compute unweighted document vectors
    udocvec = Augment(data, sideview=inverseDocFreq, loadedBy=loadDictView) \
        | Map(by=lambda ((docid,term),idfDict):(docid,term,idfDict[term]))

    #normalize
    norm = Group(udocvec, 
                 by=lambda(docid,term,weight):docid, 
                 retaining=lambda(docid,term,weight):weight*weight,
                 reducingTo=ReduceToSum() )
    norm.opts(hopts=UNCOMPRESSED,parallel=20)

    docvec = Augment(udocvec, sideview=norm, loadedBy=loadDictView) \
        | Map( by=lambda ((docid,term,weight),normDict): (docid,term,weight/math.sqrt(normDict[docid])))

    docvecExport = Format(docvec, by=lambda(docid,term,weight):'%s\t%s\t%.3f' % (docid,term,weight)) \
        .opts(storedAt=D['output'],hopts=UNCOMPRESSED)

## always end like this
if __name__ == "__main__":
    p = TFIDF()
    p.hopts(COMPRESSED)
    p.main(sys.argv)  
