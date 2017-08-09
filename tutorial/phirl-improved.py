from guineapig import *

# a non-trivial GineaPig program - phirl-naive.py but with some added
# heuristics to decrease the number of candidate pairs

import sys
import math
import logging

# parameters for some heuristics to reduce the set of terms you use as
# indices in the soft join

NUM_TOP_TERMS = 2       # only join on the K top-scoring terms from each doc
MIN_TERM_WEIGHT = 0.2   # only terms with weight >= T in normalized doc vector 
MAX_TERM_DF = 5         # only join terms with MAX_TERM_DF <= K
TARGET_REL = 'icepark'  # should be the smaller of the two relations

# some useful subroutines

def termWeight(relCount,df): 
    return math.log(2.0)*math.log(relCount/float(df))
def eachTerm((rel,docid,id,doc)):
    for term in doc: yield (rel,docid,term)

class Phirl(Planner):
    
    # parse the data
    fields = ReadLines('id-parks.txt') | ReplaceEach(by=lambda line:line.strip().split("\t"))

    data = ReplaceEach(fields, by=lambda(rel,docid,id,str): (rel,docid,id,str.lower().split(" "))) | Flatten(by=eachTerm)

    # compute document frequency
    docFreq = Group(data, by=lambda(rel,docid,term):(rel,term), reducingTo=ReduceToCount()) \
     | ReplaceEach(by=lambda((rel,term),df):(rel,term,df))

    # find total number of docs per relation,stored as (rel,numdocs)
    ndoc = ReplaceEach(data, by=lambda(rel,docid,term):(rel,docid)) | Distinct() | Group(by=lambda(rel,docid):rel, reducingTo=ReduceToCount())

    # computed unweighted document vectors, stored as (rel,doc,term,weight)
    udocvec = Join( Jin(data,by=lambda(rel,docid,term):(rel,term)), Jin(docFreq,by=lambda(rel,term,df):(rel,term)) ) \
        | ReplaceEach(by=lambda((rel,doc,term),(rel_,term_,df)):(rel,doc,term,df)) \
        | JoinTo( Jin(ndoc,by=lambda(rel,relCount):rel), by=lambda(rel,doc,term,df):rel ) \
        | ReplaceEach(by=lambda((rel,doc,term,df),(rel_,relCount)):(rel,doc,term,df,relCount)) \
        | ReplaceEach(by=lambda(rel,doc,term,df,relCount):(rel,doc,term,termWeight(relCount,df)))

    # compute squared L2 norm for each document, stored as (rel,doc,Z)
    sumSquareWeights = ReduceTo(float, lambda accum,(rel,doc,term,weight): accum+weight*weight)
    norm = Group( udocvec, by=lambda(rel,doc,term,weight):(rel,doc), reducingTo=sumSquareWeights) \
        | ReplaceEach( by=lambda((rel,doc),z):(rel,doc,z))

    # compute normalized document vector, stored in same format as unweighted doc vectors
    docvec = Join( Jin(norm,by=lambda(rel,doc,z):(rel,doc)), Jin(udocvec,by=lambda(rel,doc,term,weight):(rel,doc)) ) \
        | ReplaceEach( by=lambda((rel,doc,z),(rel_,doc_,term,weight)): (rel,doc,term,weight/math.sqrt(z)) )

    # split the document vectors by relation
    rel1Docs = Filter(docvec, by=lambda(rel,doc,term,weight):rel==TARGET_REL)
    rel2Docs = Filter(docvec, by=lambda(rel,doc,term,weight):rel!=TARGET_REL)

    # Some heuristics for choosing which terms to join on.  these are
    # applied only to terms in the 'target' relation

    # 1) pick only top terms in each document
    topTermsInEachDocForRel1 = Group(rel1Docs, by=lambda(rel,doc,term,weight):doc, retaining=lambda(rel,doc,term,weight):(weight,term)) \
        | ReplaceEach(by=lambda(doc,termList):sorted(termList,reverse=True)[0:NUM_TOP_TERMS]) \
        | Flatten(by=lambda x:x) | ReplaceEach(by=lambda(weight,term):term)

    # 2) pick terms that have some minimal weight in their documents
    highWeightTermsForRel1 = Filter(rel1Docs, by=lambda(rel,doc,term,weight):weight>=MIN_TERM_WEIGHT) \
        | ReplaceEach(by=lambda(rel,doc,term,weight):term)

    # 3) pick terms with some maximal DF
    lowDocFreqTerms = Filter(docFreq,by=lambda(rel,term,df):df<=MAX_TERM_DF) | ReplaceEach(by=lambda(rel,term,df):term)

    # terms we will join on should pass all of the tests above
    usefulTerms = Join( Jin(topTermsInEachDocForRel1), Jin(highWeightTermsForRel1)) | ReplaceEach(by=lambda(term1,term2):term1) \
        | JoinTo( Jin(lowDocFreqTerms)) | ReplaceEach(by=lambda(term1,term2):term1) | Distinct()

    # this is now an approximate soft join, because
    # a) since we're not considering all possible index terms, some pairs with non-zero similarity could be missed
    # b) since we're not adding up weight products for all terms, the score for a pair can be under-counted

    softjoin = Join( Jin(rel1Docs,by=lambda(rel,doc,term,weight):term), Jin(usefulTerms)) | ReplaceEach(by=lambda(rel1doc,term):rel1doc) \
        | JoinTo( Jin(rel2Docs,by=lambda(rel,doc,term,weight):term), by=lambda(rel,doc,term,weight):term)\
        | ReplaceEach(by=lambda((rel1,doc1,term,weight1),(rel2,doc2,term_,weight2)): (doc1,doc2,weight1*weight2)) \
        | Group(by=lambda(doc1,doc2,p):(doc1,doc2), reducingTo=ReduceTo(float,lambda accum,(doc1,doc2,p): accum+p)) \
        | ReplaceEach(by=lambda((doc1,doc2),sim):(doc1,doc2,sim))

    # get the strongly similar pairs
    simpairs = Filter(softjoin, by=lambda(doc1,doc,sim):sim>0.65)

    # diagnostic output
    look = Join( Jin(simpairs,by=lambda(doc1,doc2,sim):doc1), Jin(fields,by=lambda(rel,doc,id,str):doc) ) \
        | ReplaceEach(by=lambda((doc1,doc2,sim),(rel,doc1_,id1,str)):(doc1,doc2,id1,str,sim)) \
        | JoinTo( Jin(fields,by=lambda(rel,doc,id,str):doc), by=lambda(doc1,doc2,id1,str1,sim):doc2) \
        | ReplaceEach( by=lambda((doc1,doc2,id1,str1,sim),(rel,doc2_,id2,str2)):(doc1,doc2,(1 if id1==id2 else 0),sim,str1,str2) ) \
        | Format( by=lambda(doc1,doc2,correct,sim,str1,str2):'%5.3f\t%d\t%30s:%s\t%30s:%s' % (sim,correct,str1,doc1,str2,doc2) )

# always end like this
if __name__ == "__main__":
    p = Phirl()
    p.main(sys.argv)
