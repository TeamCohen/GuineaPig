##############################################################################
# (C) Copyright 2014, 2015 William W. Cohen.  All rights reserved.
##############################################################################

import guineapig
import sys
import random

class SpykContext(object):

    def __init__(self,**kw):
        self.planner = guineapig.Planner(**kw)
        self.tagCodeIndex = 0
    
    #TODO setSerializer, setEvaluator, ship

    #returns a SpykRDD
    def textFile(self,fileName):
        rdd = SpykRDD('textFile', self, guineapig.ReadLines(fileName))
        return rdd

    def wholeTextFiles(self,dirName):
        #TODO find this in royals, and make it a gpextra
        pass

    def finalize(self):
        """Declare the SpykRDD and all RDD definitions complete.  This must be
        called in the __name__=="__main__" part of the code, because
        it also executes substeps when called recursively."""
        self.planner.setup()        
        if guineapig.Planner.partOfPlan(sys.argv):
            self.planner.main(sys.argv)

    def usermain(self):
        """Use this in an if statement before any Spyk actions."""
        return not guineapig.Planner.partOfPlan(sys.argv)

class SpykRDD(object):

    def __init__(self,tag,context,view):
        self.view = view
        self.context = context
        self.view.planner = context.planner
        self.context.tagCodeIndex += 1
        self.context.planner._setView("%s__%d" % (tag,self.context.tagCodeIndex), view)

    #TODO this doesn't work, need to use a different mechanism,
    #maybe with a wrapper around plan/execute
    def cache(self):
        self.view = self.view.opts(stored=True)
        return self

    #transformations, which return new SpykRDD's

    #TODO
    #union
    #intersection - gpextra?
    # ... and for keyed views only
    #cogroup

    def map(self,mapfun):
        return SpykRDD('map',self.context, guineapig.ReplaceEach(self.view,by=mapfun))

    def flatMap(self,mapfun):
        return SpykRDD('flatMap',self.context, guineapig.Flatten(self.view,by=mapfun))

    def groupByKey(self):
        return SpykRDD('groupByKey', 
                       self.context, 
                       guineapig.Group(self.view, 
                                       by=lambda (key,val):key, 
                                       retaining=lambda (key,val):val))

    def reduceByKey(self,initValue,reduceOp):
        return SpykRDD('reduceByKey',
                       self.context, 
                       guineapig.Group(self.view, 
                                       by=lambda (key,val):key, 
                                       retaining=lambda (key,val):val,
                                       reducingTo=guineapig.ReduceTo(initValue,reduceOp)))
    def filter(self,filterfun):
        return SpykRDD('filter',self.context, guineapig.Filter(self.view,by=filterfun))

    def sample(self,withReplacement,fraction):
        assert not withReplacement, 'sampling with replacement is not implemented'
        return SpykRDD('sample',self.context, guineapig.Filter(self.view,by=lambda x:1 if random.random()<fraction else 0))

    def join(self,rdd):
        return SpykRDD('join', 
                       self.context, 
                       (guineapig.Join(guineapig.Jin(self.view,by=lambda (k,v):k),
                                      guineapig.Jin(rdd.view,by=lambda (k,v):k)) \
                        | guineapig.ReplaceEach(by=lambda ((k1,v1),(k2,v2)):(k1,(v1,v2)))))

    def distinct(self):
        return SpykRDD('distinct', self.context, guineapig.Distinct(self.view))


    #TODO
    #actions, which setup(), store and return a python data structure
    #can setup() be called more than once? note we need to initialize
    #the argument view as part of sc.planner, and I guess mark it as
    #re-useable.

    #reduce
    #save(path)
    #countByKey
    #foreach

    def collect(self):
        """Returns a generator."""
        for x in self._take(-1): yield x

    def take(self,n):
        return list(self._take(n))

    def first(self):
        return list(self.take(1))[0]

    def _take(self,n):
        #subroutine, like take(n) but take(-1) collects all
        plan = self.view.storagePlan()
        plan.execute(self.context.planner, echo=self.context.planner.opts['echo'])
        k = 0
        for line in open(self.view.storedFile()):
            k += 1
            if n<0 or k<=n:
                yield self.view.planner._serializer.fromString(line.strip())
        
    def count(self):
        plan = self.view.storagePlan()
        plan.execute(self.context.planner, echo=self.context.planner.opts['echo'])
        n = 0
        for line in open(self.view.storedFile()):
            n += 1
        return n

    #debug - not in spark
    #pprint, tasks, plan, list

