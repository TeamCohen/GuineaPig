import unittest
import sys

#these tests need to be run somewhere where gpig_views can be created
#and where data/xA.txt and other sample input views are accessible.

from guineapig import *

class Wrap(View):
    """ Wraps any iterable python object - for testing"""
    def __init__(self,dataSource):
        View.__init__(self)
        self.dataSource = dataSource
    def checkpoint(self):
        return '/dev/null'
    def checkpointPlan(self):
        plan = Plan()
        plan.append( TransformStep(view=self,whatToDo='doStoreRows',srcs=['/dev/null'],dst=self.storedFile(),why=self.explanation()) )
        return plan
    def rowGenerator(self):
        for row in self.dataSource:
            yield row
    def explanation(self):
        return [ 'wrap data as %s' % self.tag ]
    def __str__(self):
        s = str(self.dataSource)        
        if len(s)>30: s = s[0:30]+'...'
        return 'Wrap("%s")' % s + self.showExtras()

someInts = list(range(10))
somePairs = [(i/3, i) for i in range(15)]
leftd = [('a1','a1v'),('a2','a2v1'),('a2','a2v2'),('w','aw'),('x','ax'),('y','ay1'),('y','ay2'),('z','az1'),('z','az2')]
rightd = [('b1','b1v'),('b2','b2v1'),('b2','b2v2'),('w','bw1'),('w','bw2'),('x','bx'),('y','by'),('z','bz1'),('z','bz2')]

def aPlanner():
    # a program to test
    p = Planner()
    p.a = Wrap(someInts)
    p.b = Wrap([[1,2,3],[4,5,6,7],[6,7,8]])
    p.doubleA = ReplaceEach(p.a, by=lambda r:2*r)
    p.oddA = Filter(p.a, lambda r:r%2==1)
    p.flatB = Flatten(p.b, by=lambda r:r[0:-1])
    p.distinctFlatB = Distinct(p.flatB)
    p.c = Wrap(somePairs)
    p.groupedC = Group(p.c, by=lambda r:r[0]) | ReplaceEach(by=lambda(k,xs):(k,sorted(xs)))  #need to sort to canonicalize the groups...
    p.reducedC = Group(p.c, by=lambda r:r[0], retaining=lambda r:r[1], reducingTo=ReduceToSum())

    #tests for joins
    p.leftd = Wrap(leftd)
    p.rightd = Wrap(rightd)
    p.joinii = Join( Jin(p.leftd,by=lambda r:r[0]), Jin(p.rightd,by=lambda r:r[0]) )
    p.joiniiCounts = Group(p.joinii, by=lambda((ak,av),(bk,bv)):ak, reducingTo=ReduceToCount())

    p.joinio = Join( Jin(p.leftd,by=lambda r:r[0]), Jin(p.rightd,by=lambda r:r[0],outer=True) ) 
    p.joinioNonNullCounts = Filter(p.joinio, by=lambda (a,b):a and b) | Group(by=lambda((ak,av),(bk,bv)):ak, reducingTo=ReduceToCount())
    p.joinioNullCounts = Filter(p.joinio, by=lambda (a,b):not a) | Group(by=lambda(a,(bk,bv)):bk, reducingTo=ReduceToCount())

    p.joinoi = Join( Jin(p.leftd,by=lambda r:r[0],outer=True), Jin(p.rightd,by=lambda r:r[0]) )
    p.joinoiNonNullCounts = Filter(p.joinoi, by=lambda (a,b):a and b) | Group(by=lambda((ak,av),(bk,bv)):ak, reducingTo=ReduceToCount())
    p.joinoiNullCounts = Filter(p.joinoi, by=lambda (a,b):not b) | Group(by=lambda((ak,av),b):ak, reducingTo=ReduceToCount())

    p.joinoo = Join( Jin(p.leftd,by=lambda r:r[0],outer=True), Jin(p.rightd,by=lambda r:r[0],outer=True) ) 
    p.joinooNonNullCounts = Filter(p.joinoo, by=lambda (a,b):a and b) \
        | Group(by=lambda((ak,av),(bk,bv)):ak, reducingTo=ReduceToCount())
    p.joinooNullCounts = Filter(p.joinoo, by=lambda (a,b):(not (a and b))) \
        | ReplaceEach(by=lambda (a,b):a if a else b) | Group(by=lambda(k,v):k, reducingTo=ReduceToCount())

    #tests for planner
    p.xA = ReadLines('data/xA.txt') | ReplaceEach(by=lambda line:int(line.strip()))
    p.xHiA = Filter(p.xA, lambda r:r>=3)
    p.xLoA = Filter(p.xA, lambda r:r<=7)
    p.xMidA = Join(Jin(p.xLoA), Jin(p.xHiA)) | ReplaceEach(by=lambda (a,b):a)

    #augment
    p.yA = ReadLines('data/xA.txt') | ReplaceEach(by=lambda line:int(line.strip()))
    p.yD = ReadLines('data/d.txt') | ReplaceEach(by=lambda line:int(line.strip()))
    p.augA = Augment(p.yA, sideview=p.yD, loadedBy=lambda v:GPig.onlyRowOf(v))
    p.hiLo = p.augA | ReplaceEach(by=lambda(a,d):1 if a>d else -1)

    p.uab = Union(p.yA,p.yD)

    p.setup()
    return p

class Test(unittest.TestCase):

    def setUp(self):
        self.p = aPlanner()

    def testReplaceEach(self):
        print 'TEST: ReplaceEach'
        self.checkEquiv(self.p,'doubleA',map(lambda r:2*r, someInts))

    def testFilter(self):
        print 'TEST: Filter'
        self.checkEquiv(self.p,'oddA',filter(lambda r:r%2==1, someInts))

    def testFlatten(self):
        print 'TEST: Flatten'
        self.checkEquiv(self.p,'flatB',[1,2,4,5,6,7])
        self.checkExact(self.p,'flatB',[1,2,4,5,6,6,7])

    def testDistinct(self):
        print 'TEST: Distinct'
        self.checkExact(self.p,'distinctFlatB',[1,2,4,5,6,7])

    def testGroup(self):
        print 'TEST: Group'
        expectedGroupedC = [(i,[(i,i*3+j) for j in range(3)]) for i in range(5)]
        self.checkExact(self.p, 'groupedC',expectedGroupedC)
        expectedReducedC = [(i,sum(map(lambda (x,y):y,xs))) for (i,xs) in expectedGroupedC]
        self.checkExact(self.p, 'reducedC',expectedReducedC)

    def testJoin(self):
        print 'TEST: Join'
        expectedInnerJoinCounts = [('w',2),('x',1),('y',2),('z',4)]
        print 'TEST: Joinii'
        self.checkEquiv(self.p, 'joiniiCounts', expectedInnerJoinCounts)
        print 'TEST: Joinio'
        self.checkEquiv(self.p, 'joinioNonNullCounts', expectedInnerJoinCounts)
        self.checkEquiv(self.p, 'joinioNullCounts', [('b1',1),('b2',2)])
        print 'TEST: Joinoi'
        self.checkEquiv(self.p, 'joinoiNonNullCounts', expectedInnerJoinCounts)
        self.checkEquiv(self.p, 'joinoiNullCounts', [('a1',1),('a2',2)])
        print 'TEST: Joinoo'
        self.checkEquiv(self.p, 'joinooNonNullCounts', expectedInnerJoinCounts)
        self.checkEquiv(self.p, 'joinooNullCounts', [('a1',1),('a2',2)] + [('b1',1),('b2',2)])

    def testPlanning(self):
        print 'TEST: Planner'
        v = self.p.getView('xMidA')
        #check inferred storage
        plan = v.storagePlan()
        self.assertTrue( self.p.getView('xA').storeMe )
        print 'midA plan:\n',"\n".join(map(str,plan.steps))
        #self.assertTrue(len(plan.steps)==5)
        self.checkEquiv(self.p, 'xMidA', [3,4,6,7])

    def testAugment(self):
        print 'TEST: Augment'
        self.checkExact(self.p, 'hiLo', [-1]*5 + [+1]*4)

    def testUnion(self):
        print 'TEST: Union'
        self.checkExact(self.p, 'uab', list(range(10)))

    def checkEquiv(self,p,viewName,expected):
        v = p.getView(viewName)
        v.storagePlan().execute(p)
        actualSet = set(self.rowsOf(v))
        print 'expected:',expected
        print 'actual:  ',actualSet
        expectedSet = set(expected)
        for r in actualSet:
            self.assertTrue(r in expectedSet)
        for r in expected:
            self.assertTrue(r in actualSet)

    def checkExact(self,p,viewName,expected):
        v = p.getView(viewName)
        v.storagePlan().execute(p)
        actual = list(self.rowsOf(v))
        print 'exact expected:',expected,'len',len(expected)
        print 'exact actual:  ',actual,'len',len(actual)
        self.assertTrue(len(actual)==len(expected))
        for i in range(len(actual)):
            self.assertTrue(actual[i]==expected[i])

    def rowsOf(self,v):
        for line in open(v.storedFile()):
            yield v.planner._serializer.fromString(line.strip())


if __name__=="__main__":
    if len(sys.argv)==1:
        unittest.main()
    else:
        aPlanner().main(sys.argv)
