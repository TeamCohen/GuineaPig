# tests for spyk

import spyk

if __name__ == "__main__" :
    
    sc = spyk.SpykContext()
    xA = sc.textFile('data/xA.txt').map(lambda line:int(line.strip()))
    xHiA = xA.filter(lambda r:r>=3)
    xLoA = xA.filter(lambda r:r<=7)
    xMidA = xHiA.intersection(xLoA)
    xHiAPairs = xHiA.map(lambda r:(r,2*r))
    xLoAPairs = xLoA.map(lambda r:(r,3*r))
    xMidAPairs = xHiAPairs.join(xLoAPairs)
    xUnionPairs = xHiAPairs.union(xLoAPairs)
    xGroupPairs = xUnionPairs.groupByKey()
    xGroupSizes = xUnionPairs.countByKey()
    xSumPairs = xUnionPairs.reduceByKey(0,lambda accum,x:accum+x)
    xDistinctInputs = xUnionPairs.map(lambda (a,b):a)
    xDistinctPairs = xUnionPairs.map(lambda (a,b):a).distinct()
    xSample = xA.sample(False,0.5)
    yA = sc.textFile('data/xA.txt').map(lambda line:int(line.strip()))
    yB = yA.filter(lambda n:n%2==1)
    yC = yB.map(lambda n:(n,n+1,2*n))
    yB.cache()
    # triples (id, classList, words)
    corpus = sc.textFile('data/mytest.txt') \
        .map(lambda line:line.strip().split("\t")) \
        .map(lambda parts:(parts[0],parts[1].split(","),parts[2:]))
    docterm = corpus.flatMap(lambda (docid,classes,words):[(docid,w) for w in words])
    sc.finalize()

    if sc.usermain():

        print '= xA',list(xA.collect())
        print '= xHiA',list(xHiA.collect())
        print '= xLoA',list(xLoA.collect())
        print '= xMidA',list(xMidA.collect())
        print '= xMidAPairs',list(xMidAPairs.collect())
        print '= xUnionPairs',list(xUnionPairs.collect())
        print '= xGroupPairs',list(xGroupPairs.collect())
        print '= xGroupSizes',list(xGroupSizes.collect())
        print '= xSumPairs',list(xSumPairs.collect())
        print '= xDistinctPairs',list(xDistinctPairs.collect())
        print '= xDistinctInputs',list(xDistinctInputs.collect())
        print '= count xDistinctInputs',xDistinctInputs.count()
        print '= count xDistinctPairs',xDistinctPairs.count()
        print '= count xSample',list(xSample.collect())
        print '= xA reduce to sum', xA.reduce(lambda a,b:a+b)
        print '= docterm',list(docterm.take(10))
        aCopy = []
        xA.foreach(lambda a:aCopy.append(a))
        print '= aCopy',aCopy
        xMidAPairs.save('tmp.txt')
        midAPairCopy = []
        for line in open('tmp.txt'):
            midAPairCopy.append(line.strip())
        print '= midAPairCopy',midAPairCopy
        
