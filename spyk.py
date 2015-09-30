##############################################################################
# (C) Copyright 2014, 2015 William W. Cohen.  All rights reserved.
##############################################################################

import guineapig
import sys
import random

class SpykContext(object):
    """Analogous to a SparkContext, this allows a Spark-like syntax for
    GuineaPig programs.  A sample program would be:

    import spyk
    if __name__ == "__main__" :
        sc = spyk.SpykContext()
        #view definitions, using sc.textFile(), rdd.map(...) and other transformations
        ...
        sc.finalize()
        if sc.usermain():
           #actions, eg collect(), take(n), first(), count(), ...
           ...
    
    You cannot interleave actions and transformations - all
    transformations need to be defined before finalize() is
    called, and all actions afterward.

    Corrently, actions can only be executed locally,
    i.e., with target=shell.
    """

    def __init__(self,**kw):
        self.planner = guineapig.Planner(**kw)
        self.tagCodeIndex = 0
        self.cachedViews = set()

    def setSerializer(self,setSerializer):
        """Delegate to the SpykContext's planner."""
        self.planner.setSerializer(setSerializer)

    def setEvaluator(self,setEvaluator):
        """Delegate to the SpykContext's planner."""
        self.planner.setEvaluator(setEvaluator)

    def ship(self,fileName):
        """Delegate to the SpykContext's planner."""
        self.planner.ship(fileName)

    #returns a SpykRDD
    def textFile(self,fileName):
        """Return a SpykRDD that contains the lines in the textfile."""
        rdd = SpykRDD('textFile', self, guineapig.ReadLines(fileName))
        return rdd

    def wholeTextFiles(self,dirName):
        #TODO find this in royals, and make it a gpextra
        assert False,'not implemented!'

    #not in spark

    def list(self):
        """Return a list of the names of all defined views."""
        return self.planner.listViewNames()

    def finalize(self):
        """Declare the SpykRDD and all RDD definitions complete.  This must be
        called in the __name__=="__main__" part of the code, after all
        transformations have been defined, because it also executes
        substeps when called as part of a plan."""
        self.planner.setup()        
        for rdd in self.cachedViews:
            rdd.view.opts(stored=True)
        if guineapig.Planner.partOfPlan(sys.argv):
            self.planner.main(sys.argv)

    def usermain(self):
        """Use this in an if statement in the __main__ of a code,
        before any Spyk actions, but after all transformations have been
        defined.."""
        return not guineapig.Planner.partOfPlan(sys.argv)

class SpykRDD(object):

    def __init__(self,tag,context,view):
        """Should not be called directly by users."""
        self.view = view
        self.context = context
        self.view.planner = context.planner
        self.context.tagCodeIndex += 1
        self.context.planner._setView("%s__%d" % (tag,self.context.tagCodeIndex), view)

    def cache(self):
        """Mark this as to-be-cached on disk."""
        self.context.cachedViews.add(self)
        return self

    #transformations, which return new SpykRDD's

    def map(self,mapfun):
        """Analogous to the corresponding Spark transformation."""
        return SpykRDD('map', self.context, guineapig.ReplaceEach(self.view,by=mapfun))

    def flatMap(self,mapfun):
        """Analogous to the corresponding Spark transformation."""
        return SpykRDD('flatMap', self.context, guineapig.Flatten(self.view,by=mapfun))

    def groupByKey(self):
        """Analogous to the corresponding Spark transformation."""
        return SpykRDD('groupByKey', 
                       self.context, 
                       guineapig.Group(self.view, 
                                       by=lambda (key,val):key, 
                                       retaining=lambda (key,val):val))

    def reduceByKey(self,initValue,reduceOp):
        """Analogous to the corresponding Spark transformation."""
        return SpykRDD('reduceByKey',
                       self.context, 
                       guineapig.Group(self.view, 
                                       by=lambda (key,val):key, 
                                       retaining=lambda (key,val):val,
                                       reducingTo=guineapig.ReduceTo(lambda:initValue,reduceOp)))
    def filter(self,filterfun):
        """Analogous to the corresponding Spark transformation."""
        return SpykRDD('filter',self.context, guineapig.Filter(self.view,by=filterfun))

    def sample(self,withReplacement,fraction):
        """Analogous to the corresponding Spark transformation, but defined only when withReplacement==False"""
        assert not withReplacement, 'sampling with replacement is not implemented'
        return SpykRDD('sample',self.context, guineapig.Filter(self.view,by=lambda x:1 if random.random()<fraction else 0))

    def union(self,rdd):
        """Analogous to the corresponding Spark transformation."""
        return SpykRDD('union', self.context, guineapig.Union(self.view,rdd.view))

    def intersection(self,rdd):
        """Analogous to the corresponding Spark transformation."""
        return SpykRDD('intersection',
                       self.context,
                       (guineapig.Join(guineapig.Jin(self.view,by=lambda row:row),
                                       guineapig.Jin(rdd.view,by=lambda row:row))
                        | guineapig.ReplaceEach(by=lambda (row1,row2):row1)))

    def join(self,rdd):
        """Analogous to the corresponding Spark transformation."""
        return SpykRDD('join', 
                       self.context, 
                       (guineapig.Join(guineapig.Jin(self.view,by=lambda (k,v):k),
                                      guineapig.Jin(rdd.view,by=lambda (k,v):k)) \
                        | guineapig.ReplaceEach(by=lambda ((k1,v1),(k2,v2)):(k1,(v1,v2)))))

    def distinct(self):
        """Analogous to the corresponding Spark transformation."""
        return SpykRDD('distinct', self.context, guineapig.Distinct(self.view))


    def countByKey(self):
        """Analogous to the corresponding Spark transformation."""
        return SpykRDD('countByKey', 
                       self.context, 
                       guineapig.Group(self.view,
                                       by=lambda (k,v):k, 
                                       reducingTo = guineapig.ReduceToCount()))

    ###############################################################################
    # actions, which setup(), execute a plan, and retrieve the results.
    # TODO: include steps to download HDFS output
    ###############################################################################

    def collect(self):
        """Action which returns a generator of the rows in this transformation."""
        for x in self._take(-1): yield x

    def take(self,n):
        """Action which returns the first n things produced by collect()."""
        return list(self._take(n))

    def first(self):
        """Action which returns the first thing produced by collect()."""
        return list(self.take(1))[0]

    def _take(self,n):
        #subroutine, like take(n) but take(-1) collects all
        plan = self.view.storagePlan()
        plan.execute(self.context.planner, echo=self.context.planner.opts['echo'])
        k = 0
        #TODO: download from hdfs if needed
        for line in open(self.view.storedFile()):
            k += 1
            if n<0 or k<=n:
                yield self.view.planner._serializer.fromString(line.strip())
        
    def foreach(self,function):
        """Action which applies function(row) to each row produced by collect()."""
        for row in self.collect():
            function(row)

    def reduce(self,reduceFunction):
        """Action which applies the pairwise reduction to each row produced by collect()."""
        accum = None
        for row in self.collect():
            accum = reduceFunction(accum,row) if accum else row
        return accum

    def count(self):
        """Action which counts the number of rows produced by collect()."""
        plan = self.view.storagePlan()
        plan.execute(self.context.planner, echo=self.context.planner.opts['echo'])
        n = 0
        for line in open(self.view.storedFile()):
            n += 1
        return n

    def save(self,path):
        """Action which saves the rows produced by collect() in a local file."""
        fp = open(path,'w')
        for row in self.collect():
            str = self.context.planner.serialize(row)
            fp.write(str + '\n')            
        fp.close()

    #not in spark

    def store(self):
        """Execute the storage plan for this view, like Guineapig's --store option."""
        self.view.storagePlan().execute(self.context.planner)

    def pprint(self):
        """Print the underlying Guineapig view."""
        self.view.pprint()

    def steps(self):
        """Return list of steps in the storage plan for this view, like Guineapig's --steps option."""
        return self.view.storagePlan().steps

    def tasks(self):
        """Return list of tasks in the storage plan for this view, like Guineapig's --tasks option."""
        p = self.view.storagePlan()
        p.buildTasks()
        return p.tasks

    def plan(self):
        """Return list of shell commands in the storage plan for this view, like Guineapig's --plan option."""
        return self.view.storagePlan().compile(self.context.planner)

