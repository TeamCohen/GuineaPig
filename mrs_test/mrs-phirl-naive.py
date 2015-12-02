from guineapig import *
import gpextras

# a non-trivial GineaPig program

import sys
import math
import logging

def termWeight(relCount,df): 
    return math.log(2.0)*math.log(relCount/float(df))

def eachTerm((rel,docid,id,doc)):
    for term in doc: yield (rel,docid,term)

class Phirl(Planner):
    
    #parse the data
    fields = ReadLines('id-parks.txt') | ReplaceEach(by=lambda line:line.strip().split("\t"))

    data = ReplaceEach(fields, by=lambda(rel,docid,id,str): (rel,docid,id,str.lower().split(" "))) | Flatten(by=eachTerm)

    #compute document frequency
    docFreq = Group(data, by=lambda(rel,docid,term):(rel,term), reducingTo=ReduceToCount()) \
     | ReplaceEach(by=lambda((rel,term),df):(rel,term,df))

    #find total number of docs per relation
    ndoc = ReplaceEach(data, by=lambda(rel,docid,term):(rel,docid)) | Distinct() | Group(by=lambda(rel,docid):rel, reducingTo=ReduceToCount())

    #unweighted document vectors
    
    udocvec = Join( Jin(data,by=lambda(rel,docid,term):(rel,term)), Jin(docFreq,by=lambda(rel,term,df):(rel,term)) ) \
        | ReplaceEach(by=lambda((rel,doc,term),(rel_,term_,df)):(rel,doc,term,df)) \
        | JoinTo( Jin(ndoc,by=lambda(rel,relCount):rel), by=lambda(rel,doc,term,df):rel ) \
        | ReplaceEach(by=lambda((rel,doc,term,df),(rel_,relCount)):(rel,doc,term,df,relCount)) \
        | ReplaceEach(by=lambda(rel,doc,term,df,relCount):(rel,doc,term,termWeight(relCount,df)))

    sumSquareWeights = ReduceTo(float, lambda accum,(rel,doc,term,weight): accum+weight*weight)

    norm = Group( udocvec, by=lambda(rel,doc,term,weight):(rel,doc), reducingTo=sumSquareWeights) \
        | ReplaceEach( by=lambda((rel,doc),z):(rel,doc,z))

    #normalized document vector
    docvec = Join( Jin(norm,by=lambda(rel,doc,z):(rel,doc)), Jin(udocvec,by=lambda(rel,doc,term,weight):(rel,doc)) ) \
        | ReplaceEach( by=lambda((rel,doc,z),(rel_,doc_,term,weight)): (rel,doc,term,weight/math.sqrt(z)) )

    # grab only the p component and reduce it
    sumOfP = ReduceTo(float,lambda accum,(doc1,doc2,p): accum+p)

    # naive algorithm: use all pairs for finding matches
    rel1Docs = Filter(docvec, by=lambda(rel,doc,term,weight):rel=='icepark')
    rel2Docs = Filter(docvec, by=lambda(rel,doc,term,weight):rel=='npspark')
    softjoin = Join( Jin(rel1Docs,by=lambda(rel,doc,term,weight):term), Jin(rel2Docs,by=lambda(rel,doc,term,weight):term)) \
        | ReplaceEach(by=lambda((rel1,doc1,term,weight1),(rel2,doc2,term_,weight2)): (doc1,doc2,weight1*weight2)) \
        | Group(by=lambda(doc1,doc2,p):(doc1,doc2), reducingTo=sumOfP) \
        | ReplaceEach(by=lambda((doc1,doc2),sim):(doc1,doc2,sim))

    # get the top few similar pairs
    simpairs = Filter(softjoin, by=lambda(doc1,doc,sim):sim>0.75)

    # diagnostic output
    look = Join( Jin(simpairs,by=lambda(doc1,doc2,sim):doc1), Jin(fields,by=lambda(rel,doc,id,str):doc) ) \
        | ReplaceEach(by=lambda((doc1,doc2,sim),(rel,doc1_,id1,str)):(doc1,doc2,id1,str,sim)) \
        | JoinTo( Jin(fields,by=lambda(rel,doc,id,str):doc), by=lambda(doc1,doc2,id1,str1,sim):doc2) \
        | ReplaceEach( by=lambda((doc1,doc2,id1,str1,sim),(rel,doc2_,id2,str2)):(doc1,doc2,(1 if id1==id2 else 0),sim,str1,str2) ) \
        | Format( by=lambda(doc1,doc2,correct,sim,str1,str2):'%5.3f\t%d\t%30s:%s\t%30s:%s' % (sim,correct,str1,doc1,str2,doc2) )

# always end like this
if __name__ == "__main__":
    p = Phirl()
    p.registerCompiler('mrs',gpextras.MRSCompiler)
    p.main(sys.argv)
