##############################################################################
# (C) Copyright 2014, 2015 William W. Cohen.  All rights reserved.
##############################################################################

import sys
import logging
import copy
import subprocess
import collections
import os
import os.path
import urlparse
import urllib
import getopt
import csv

###############################################################################
# helpers functions and data structures
###############################################################################

class GPig(object):
    """Collection of utilities for Guinea Pig."""

    SORT_COMMAND = 'LC_COLLATE=C sort'  # use standard ascii ordering, not locale-specific one
    HADOOP_LOC = 'hadoop'               # assume hadoop is on the path at planning time
    MY_LOC = 'guineapig.py'             # the name of this file
    VERSION = '1.3.4'
    COPYRIGHT = '(c) William Cohen 2014,2015'

    #Global options for Guinea Pig can be passed in with the --opts
    #command-line option, and these are the default values
    #The location of the streaming jar is a special case,
    #in that it's also settable via an environment variable.
    #defaultJar = '/home/hadoop/contrib/streaming/hadoop-streaming.jar'
    defaultJar = '/opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar'
    envjar = os.environ.get('GP_STREAMJAR', defaultJar)
    defaultViewDir = 'gpig_views'
    envViewDir = os.environ.get('GP_VIEWDIR',defaultViewDir )
    DEFAULT_OPTS = {'streamJar': envjar,
                    'parallel':5,
                    'target':'shell',
                    'echo':0,
                    'viewdir': envViewDir,
                    }
    #These are the types of each option that has a non-string value
    DEFAULT_OPT_TYPES = {'parallel':int,'echo':int}
    #We need to pass non-default options in to mappers and reducers,
    #but since the remote worker's environment can be different that
    #the environment of this script, we also need to pass in options
    #computed from the environment
    COMPUTED_OPTION_DEFAULTS = {'streamJar':defaultJar, 'viewdir':defaultViewDir}

    @staticmethod
    def getArgvParams(required=[]): 
        """Return a dictionary holding the argument of the --params option in
        sys.argv.  The argument requiredParams, if present, is a list
        of dictionary keys that must be present.  Otherwise an error will
        be thrown."""
        result =  GPig.getArgvDict('--params')
        for p in required:
            assert p in result, '--params must contain a value for "'+p+"', e.g., --params "+p+":FOO"
        return result

    @staticmethod
    def getArgvOpts(): 
        """Return a dictionary holding the argument of the --opts option in
        sys.argv."""
        return GPig.getArgvDict('--opts')
    
    @staticmethod
    def getArgvDict(optname):
        """Return a dictionary of parameter values that were defined on the command line
        view an option like '--params filename:foo.txt,basedir:/tmp/glob/'.
        """
        assert optname.startswith('--')
        for i,a in enumerate(sys.argv):
            if a==optname:
                paramString = sys.argv[i+1]
                result = dict(pair.split(":") for pair in paramString.split(","))
                for key in result:
                    result[key] = urllib.unquote(result[key])
                return result
        return {}

    @staticmethod
    def rowsOf(view):
        """Return a generator that iterates over the rows in a view."""
        for line in open(view.distributableFile()):
            yield view.planner._serializer.fromString(line.strip())

    @staticmethod
    def onlyRowOf(view):
        """Return the first row in a side view, and raise an error if it
        is not the only row of the view."""
        result = None
        logging.info('loading '+view.distributableFile())
        for line in open(view.distributableFile()):
            assert not result,'multiple rows in stored file for '+view.tag
            result = view.planner._serializer.fromString(line.strip())
        return result

    @staticmethod
    class SafeEvaluator(object):
        """Evaluates expressions that correzpond to serialized guinea pig rows."""
        def __init__(self,restrictedBindings={}):
            self.restrictedBindings = restrictedBindings
        def eval(self,s):
            code = compile(s,'<gpig row>','eval')
            return eval(code,self.restrictedBindings)

class Jin(object):
    """"Object to hold description of a single join input."""

    def __init__(self,view,by=(lambda x:x),outer=False):
        self.view = view
        self.joinBy = by
        self.outer = outer
        #To implement the semantics for outer joins, if one Jin input
        #for a join is outer, then the other inputs will be marked as
        #_padWithNulls==True
        self._padWithNulls = False

    def __str__(self):
        viewStr = View.asTag(self.view) if self.view else '_'
        outerStr = ',outer=True' if self.outer else ''
        padStr = ',_padWithNulls=True' if self._padWithNulls else ''
        return "Jin(%s,by=%s%s%s)" % (viewStr,self.joinBy,outerStr,padStr)

class ReduceTo(object):
    """An object x that can be the argument of a reducingTo=x parameter in
    a Group view.  Basetype is a function f such that f() returns the
    initial value of the accumuator, and 'by' is a function that
    maps one accumulator and a single new value to the next accumulator.  
    """
    def __init__(self,baseType,by=lambda accum,val:accum+val):
        self.baseType = baseType
        self.reduceBy = by

class ReduceToCount(ReduceTo):
    """Produce the count of the number of objects that would be placed in a group."""
    def __init__(self):
        ReduceTo.__init__(self,int,by=lambda accum,val:accum+1)
    def __str__(self):
        return '<ReduceToCount>'

class ReduceToSum(ReduceTo):
    """Produce the sum of the objects - which must be legal arguments of
    the '+' function - that would be placed in a group."""
    def __init__(self):
        ReduceTo.__init__(self,int,by=lambda accum,val:accum+val)
    def __str__(self):
        return '<ReduceToSum>'

class ReduceToList(ReduceTo):
    """Produce a list of the objects that would be placed in a group."""
    def __init__(self):
        ReduceTo.__init__(self,list,by=lambda accum,val:accum+[val])
    def __str__(self):
        return '<ReduceToList>'

###############################################################################
# abstract views
##############################################################################

class View(object):
    """A definition of a relation for Guinea Pig.  A View object can be
    produce a storagePlan(), which can then be executed to produce the
    contents of the relation.  Intutitively, a relation is and
    unordered bag of rows, and a row an almost-arbitrary python data
    structure. (It must be something that can be stored and retrieved
    by the RowSerializer.)

    Steps in the storagePlan are executed by delegation, thru the
    planner, to methods of a View class named doFoo.
    """

    def __init__(self):
        """The planner and tag must be set before this is used."""
        self.planner = None       #pointer to planner object
        self.tag = None           #for naming storedFile and checkpoints
        self.storeMe = None       #try and store this view if true
        self.storeMeAt = None     #file name to store the file
        self.parallelForMe = None #level of parallelism, locally to this view
        self.hDefsForMe = []      # hadoop defs, -D param.name=value
        self.hOptsForMe = []      # hadoop opts, like -jobconf param.name=value
        self.retainedPart = None  #used in map-reduce views only
        self.sideviews = []       #non-empty for Augment views only
        self.inners = []          #always used

    #self.inner is shortcut for inners[0]
    def _getInner(self): return self.inners[0]
    def _setInner(self,val): self.inners = [val]
    inner = property(_getInner,_setInner)

    #
    # ways to modify a view
    # 

    def opts(self,stored=None,storedAt=None,parallel=None,hdefs=[],hopts=[]):
        """Return the same view with options set appropriately.  Possible
        options include:

          - stored=True - Explicitly store this view on disk whenever
            it is used in another view's definition.  This might be set
            by the user for debugging purposes, or by the planner,
            to prevent incorrect optimizations.  Generally "inner"
            views are not explicitly stored.
            
          - stored='distributedCache' - Store this view in the working
            directory and/or the Hadoop distributed cache.
            
          - storedAt=path/to/file - Store this view in this location,
            not in the viewdir'

          - parallel=N - suggestion to use N reducer tasks for this view. 

          - hdefs=['-D','param.name=param.value', ...] - Hadoop param
            definitions to pass in to the hadoop streaming tasks that
            implement this view.  Each param.name=value pair should
            be preceded by a '-D' string.

          - hopts=['-jobconf','param.name=param.value', ...] - Hadoop 
            options to pass in to the hadoop streaming tasks that
            implement this view.
            """

        self.storeMe = stored
        if storedAt:
            if not self.storeMe: self.storeMe=True
            self.storeMeAt = storedAt
        if parallel: self.parallelForMe = int(parallel)
        self.hDefsForMe = hdefs
        self.hOptsForMe = hopts
        return self

    def showExtras(self):
        """Printable representation of the options for a view."""
        result = ''
        flagPairs = []
        if self.storeMe: flagPairs += ['stored=%s' % repr(self.storeMe)]
        if self.parallelForMe: flagPairs += ['parallel=%d' % self.parallelForMe]
        if flagPairs: result += '.opts(' + ",".join(flagPairs) + ')'
        return result

    #
    # how the view is saved on disk
    #

    def storedFile(self):
        """The file that will hold the materialized relation."""
        if self.storeMeAt: return self.storeMeAt
        else: return self.planner.opts['viewdir'] + '/' + self.tag + '.gp'

    def distributableFile(self):
        """The file that will hold the materialized relation in the working directory
        in preparation to be uploaded to the distributed cache."""
        return self.tag + '.gp'

    @staticmethod
    def viewNameFor(fileName):
        """The view associated with the given file name"""
        vname = os.path.basename(fileName)
        if vname.endswith(".gp"): vname = vname[0:-len(".gp")]
        return vname

    #
    # semantics of the view
    #

    def checkpoint(self):
        """A checkpoint is an intermediate computation for the view, which is
        saved on disk.  The rowGenerator() for the view will assume
        that the checkpoint is available.
        """
        assert False, 'abstract method called'

    def checkpointPlan(self):
        """A plan to produce checkpoint()."""
        assert False, 'abstract method called'

    def rowGenerator(self):
        """A generator for the rows in this relation, which assumes existence
        of the checkpoint."""
        assert False, 'abstract method called'

    def explanation(self):
        """Return an explanation of how rows are generated."""
        assert False, 'abstract method called'

    def storagePlan(self):
        """A plan to store the view."""
        return self.planner._buildRecursiveStoragePlan(self)

    def nonrecursiveStoragePlan(self):
        """Materialize the relation, assuming that there are no descendent
        inner views that need to be materialized first."""
        plan = Plan()
        plan.includeStepsOf(self.checkpointPlan())
        plan.append(TransformStep(view=self,whatToDo='doStoreRows',srcs=[self.checkpoint()],dst=self.storedFile(),why=self.explanation()))
        if self.storeMe=='distributedCache':
            plan.append(DistributeStep(self))
        return plan
            
    def applyDict(self,mapping,innerviewsOnly=False):
        """Given a mapping from view tags to views, replace every inner view with
        the appropriate value from the mapping, and return the result."""
        if self.tag in mapping and not innerviewsOnly:
            return mapping[self.tag]
        elif not self.inners:
            return self
        else:
            result = copy.copy(self)
            result.inners = map(lambda v:v.applyDict(mapping), self.inners)
            return result

    def sideviewsNeeded(self):
        """Sideviews needed by this view."""
        result = []
        for sv in self.sideviews:
            result += [sv]
        for v in self.inners:
            result += list(v._sideviewsOfDescendants())
        return result
        
    def _sideviewsOfDescendants(self):
        if not self.storeMe:
            for sv in self.sideviews:
                yield sv
            for v in self.inners:
                for sv in v._sideviewsOfDescendants():
                    yield sv

    def enforceStorageConstraints(self):
        """Subclass this, if there are constraints on when one must explicitly
        store inner views."""
        pass

    def doStoreRows(self):
        """Called by planner at execution time to store the rows of the view."""
        for row in self.rowGenerator():
            print self.planner._serializer.toString(row)

    def supportsCombiners(self):
        """Special test to see if combiners can be used with this view."""
        return False

    #
    # support the "pipe" syntax: view1 | view2
    #

    def __or__(self,otherView):
        """Overload the pipe operator x | y to return with y, with x as its inner view."""
        otherView.acceptInnerView(self)
        return otherView

    def acceptInnerView(self,otherView):
        """Replace an appropriate input view with otherView. This is subclassed to 
        implement the the pipe operator."""
        assert not self.inner,'An inner view is defined for '+self.tag+' so you cannot use it as RHS of a pipe'
        self.inner = otherView  #subclass if needed

    #
    # printing views
    #

    def pprint(self,depth=0,alreadyPrinted=None,sideview=False):
        """Print a readable representation of the view."""
        if alreadyPrinted==None: alreadyPrinted = set()
        tabStr = '| ' * depth
        tagStr = str(self.tag)
        sideviewIndicator = '*' if sideview else ''
        if self.tag in alreadyPrinted:
            print tabStr + sideviewIndicator + tagStr + ' = ' + '...'
        else:
            sideviewInfo = "  sideviews: {"+",".join(map(lambda x:x.tag, self.sideviews))+"}" if self.sideviews else ""
            sideviewInfo += "  *sideviews: {"+",".join(map(lambda x:x.tag, self.sideviewsNeeded()))+"}" if self.sideviewsNeeded() else ""
            print tabStr + sideviewIndicator + tagStr + ' = ' + str(self) + sideviewInfo
            alreadyPrinted.add(self.tag)
            for inner in self.inners:
                inner.pprint(depth+1,alreadyPrinted)
            for inner in self.sideviews:
                inner.pprint(depth+1,alreadyPrinted,sideview=True)

    @staticmethod
    def asTag(view):
        """Helper for printing views."""
        if not view: return '(null view)'
        elif view.tag: return view.tag 
        else: return str(view)

#
# abstract view types
#

class Reader(View):
    """Read data stored on the file system and make it look like a View."""

    def __init__(self,src):
        View.__init__(self)
        assert src,'argument to Reader should not be null' 
        self.src = src
        self.inners = []

    def checkpoint(self): 
        return self.src

    def checkpointPlan(self):
        return Plan()  #empty plan

    def explanation(self):
        return [ 'read %s with %s' % (str(self.src),self.tag) ]

    def acceptInnerView(self,otherView):
        assert False, "Reader views cannot be used as RHS of a pipe"

class Transformation(View):
    """Streaming transformation on a single inner view."""

    def __init__(self,inner=None):
        View.__init__(self)
        self.inner = inner
    
    # A transformation will stream on-the-fly through the inner
    # relation, and produce a new version, so the checkpoint and plan
    # to produce it are delegated to the inner View.

    def checkpoint(self):
        return self.inner.checkpoint()

    def checkpointPlan(self):
        return self.inner.checkpointPlan()

    def explanation(self):
        return self.inner.explanation() + [ 'transform to %s' % self.tag ]

class MapReduce(View):
    """A view that takes an inner relation and processes in a
    map-reduce-like way."""

    def __init__(self,inners,retaining):
        View.__init__(self)
        self.inners = inners
        self.retainedPart = retaining
    
    def _isReduceInputFile(self,fileName):
        return fileName.endswith('.gpri')

    #use of combiners is dependent on the checkpoint
    #being the reducer input file

    def convertReduceCommandToCombineCommand(self,reduceCom):
        assert False,'abstract method called'

    def checkpoint(self):
        ## the checkpoint is the reducer input file
        return self.planner.opts['viewdir'] + '/'  + self.tag + '.gpri'

    def checkpointPlan(self):
        plan = Plan()
        for inner in self.inners:
            plan.includeStepsOf(inner.checkpointPlan())
        plan.includeStepsOf(self.mapPlan())
        return plan

    def enforceStorageConstraints(self):
        for inner in self.inners:
            innerChkpt = inner.checkpoint()
            #optimizations break if you chain two map-reduces together
            if innerChkpt and innerChkpt.endswith(".gpri"):
                if not inner.storeMe:
                    logging.info('making %s stored, to make possible a downstream map-reduce view' % inner.tag)
                    inner.storeMe = True

    def mapPlan(self):
        logging.error("abstract method not implemented")
        
    def doStoreKeyedRows(self,subview,key,index):
        """Utility method used by concrete map-reduce classes to compute keys
        and store key-value pairs.  Usually used as the main step in a
        mapPlan. """
        for row in subview.rowGenerator():
            keyStr = self.planner._serializer.toString(key(row))
            rrow = self.retainedPart(row) if self.retainedPart else row
            valStr = self.planner._serializer.toString(rrow)
            if index<0:
                print "%s\t%s" % (keyStr,valStr)
            else:
                print "%s\t%d\t%s" % (keyStr,index,valStr)
            
##############################################################################
#
# concrete View classes
#
##############################################################################

class ReuseView(Reader):
    """Returns the objects in a previously stored view."""

    def __init__(self,view):
        if isinstance(view,View):
            Reader.__init__(self,view.storedFile())
            self.tag = "reuse_"+view.tag
            self.reusedViewTag = view.tag
            self.planner = view.planner
        else:
            assert False,'user-defined ReuseView not supported (yet)'

    def rowGenerator(self):
        for line in sys.stdin:
            yield self.planner._serializer.fromString(line.strip())

    def explanation(self):
        return [ 'reuse view %s stored in %s' % (self.reusedViewTag,self.src)]

    def __str__(self):
        return 'ReuseView("%s")' % self.src + self.showExtras()


class ReadLines(Reader):
    """ Returns the lines in a file, as python strings."""

    def __init__(self,src):
        Reader.__init__(self,src)

    def rowGenerator(self):
        for line in sys.stdin:
            yield line

    def __str__(self):
        return 'ReadLines("%s")' % self.src + self.showExtras()

class ReplaceEach(Transformation):
    """ In 'by=f'' f is a python function that takes a row and produces
    its replacement."""
    
    def __init__(self,inner=None,by=lambda x:x):
        Transformation.__init__(self,inner)
        self.replaceBy = by

    def rowGenerator(self):
        for row in self.inner.rowGenerator():
            yield self.replaceBy(row)

    def explanation(self):
        return self.inner.explanation() + [ 'replace to %s' % self.tag ]

    def __str__(self):
        return 'ReplaceEach(%s, by=%s)' % (View.asTag(self.inner),str(self.replaceBy)) + self.showExtras()

class Map(ReplaceEach):
    """ Alternate name for ReplaceEach"""

class Augment(Transformation):

    def __init__(self,inner=None,sideviews=None,sideview=None,loadedBy=lambda v:list(GPig.rowsOf(v))):
        Transformation.__init__(self,inner)
        assert not (sideviews and sideview), 'cannot specify both "sideview" and "sideviews"'
        self.sideviews = list(sideviews) if sideviews else [sideview]
        self.loader = loadedBy
        assert self.loader,'must specify a "loadedBy" function for Augment'

    def enforceStorageConstraints(self):
        for sv in self.sideviews:
            logging.info('marking '+sv.tag+' to be placed in the distributedCache')
            sv.storeMe = 'distributedCache'

    def rowGenerator(self):
        augend = self.loader(*self.sideviews)
        for row in self.inner.rowGenerator():
            yield (row,augend)

    def checkpointPlan(self):
        plan = Plan()
        plan.includeStepsOf(self.inner.checkpointPlan())
        #the sideviews should have been stored by the top-level
        #planner already, and distributed, if marked as storeMe==distributedCache
        #for sv in self.sideviews:
        #    plan.append(DistributeStep(sv))
        return plan

    def explanation(self):
        return self.inner.explanation() + [ 'augmented to %s' % self.tag ]

    def __str__(self):
        sideviewTags = loaderTag = '*UNSPECIFIED*'
        if self.sideviews!=None: sideviewTags = ",".join(map(View.asTag,self.sideviews))
        if self.loader!=None: loaderTag = str(self.loader)
        return 'Augment(%s,sideviews=%s,loadedBy=s%s)' % (View.asTag(self.inner),sideviewTags,loaderTag) + self.showExtras()


class Format(ReplaceEach):
    """ Like ReplaceEach, but output should be a string, and it will be be
    stored as strings, ie without using the serializer."""

    def __init__(self,inner=None,by=lambda x:str(x)):
        ReplaceEach.__init__(self,inner,by)

    def __str__(self):
        return 'Format(%s, by=%s)' % (View.asTag(self.inner),str(self.replaceBy)) + self.showExtras()

    def doStoreRows(self):
        for row in self.rowGenerator():
            print row

class Flatten(Transformation):
    """ Like ReplaceEach, but output of 'by' is an iterable, and all
    results will be returned. """

    def __init__(self,inner=None,by=None):
        Transformation.__init__(self,inner)
        self.flattenBy = by

    def rowGenerator(self):
        for row in self.inner.rowGenerator():
            for flatrow in self.flattenBy(row):
                yield flatrow

    def explanation(self):
        return self.inner.explanation() + [ 'flatten to %s' % self.tag ]

    def __str__(self):
        return 'Flatten(%s, by=%s)' % (View.asTag(self.inner),str(self.flattenBy)) + self.showExtras()

class FlatMap(Flatten):
    """ Alternate name for Flatten"""

class Filter(Transformation):
    """Filter out a subset of rows that match some predicate."""
    
    def __init__(self,inner=None,by=lambda x:x):
        Transformation.__init__(self,inner)
        self.filterBy = by

    def rowGenerator(self):
        for row in self.inner.rowGenerator():
            if self.filterBy(row):
                yield row

    def explanation(self):
        return self.inner.explanation() + [ 'filtered to %s' % self.tag ]

    def __str__(self):
        return 'Filter(%s, by=%s)' % (View.asTag(self.inner),str(self.filterBy)) + self.showExtras()

class Distinct(MapReduce):
    """Remove duplicate rows."""

    def __init__(self,inner=None,retaining=None):
        MapReduce.__init__(self,[inner],retaining)

    def mapPlan(self):
        plan = Plan()
        plan.append(PrereduceStep(view=self,whatToDo='doDistinctMap',srcs=[self.inner.checkpoint()],dst=self.checkpoint(),why=self.explanation()))
        return plan

    def rowGenerator(self):
        """Extract distinct elements from a sorted list."""
        lastval = None
        for line in sys.stdin:
            valStr = line.strip()
            val = self.planner._serializer.fromString(valStr)
            if val != lastval and lastval: 
                yield lastval
            lastval = val
        if lastval: 
            yield lastval

    def explanation(self):
        return self.inner.explanation() + [ 'make distinct to %s' % self.tag]

    def __str__(self):
        return 'Distinct(%s)' % (View.asTag(self.inner)) + self.showExtras()

    def doDistinctMap(self):
        self.inner.doStoreRows()


class Group(MapReduce):
    """Group by some property of a row, defined with the 'by' option.
    Default outputs are tuples (x,[r1,...,rk]) where the ri's are rows
    that have 'by' values of x."""

    def __init__(self,inner=None,by=lambda x:x,reducingTo=ReduceToList(),combiningTo=None,retaining=None):
        MapReduce.__init__(self,[inner],retaining)
        self.groupBy = by
        self.reducingTo = reducingTo
        self.combiningTo = combiningTo
        # flag which determines the behavior of the rowGenerator
        self.reducerOrCombiner = self.reducingTo
    
    def mapPlan(self):
        plan = Plan()
        plan.append(PrereduceStep(view=self,whatToDo='doGroupMap',srcs=[self.inner.checkpoint()],dst=self.checkpoint(),why=self.explanation()))
        return plan

    def supportsCombiners(self):
        return self.combiningTo

    def convertReduceCommandToCombineCommand(self,reduceCom):
        k = reduceCom.find('doStoreRows')
        return reduceCom[:k] + 'doCombineRows' + reduceCom[k+len('doStoreRows'):]

    def doCombineRows(self):
        self.reducerOrCombiner = self.combiningTo
        for key,val in self.rowGenerator():
            keyStr = self.planner._serializer.toString(key)
            valStr = self.planner._serializer.toString(val)
            print "%s\t%s" % (keyStr,valStr)

    def rowGenerator(self):
        """Group objects from stdin by key, yielding tuples (key,[g1,..,gn]), or appropriate reduction of that list.."""
        lastkey = key = None
        #we re-use this algorithm for both combiners and reduces
        accum = self.reducerOrCombiner.baseType()
        for line in sys.stdin:
            keyStr,valStr = line.strip().split("\t")
            key = self.planner._serializer.fromString(keyStr)
            val = self.planner._serializer.fromString(valStr)
            if key != lastkey and lastkey!=None: 
                yield (lastkey,accum)
                accum = self.reducerOrCombiner.baseType()
            accum = self.reducerOrCombiner.reduceBy(accum, val)
            lastkey = key
        if key: 
            yield (key,accum)

    def explanation(self):
        return self.inner.explanation() + ['group to %s' % self.tag]

    def __str__(self):
        insert = ',combiningTo=%s' % str(self.combiningTo) if self.combiningTo else ''
        return 'Group(%s,by=%s,reducingTo=%s%s)' % (View.asTag(self.inner),str(self.groupBy),str(self.reducingTo),insert) + self.showExtras()

    def doGroupMap(self):
        self.doStoreKeyedRows(self.inner,self.groupBy,-1)

class Join(MapReduce):
    """Outputs tuples of the form (row1,row2,...rowk) where
    rowi is from the i-th join input, and the rowi's have the same
    value of the property being joined on."""

    def __init__(self,*joinInputs):
        #sets self.inners
        MapReduce.__init__(self,map(lambda x:x.view, joinInputs),None)
        self.joinInputs = joinInputs
        #re-interpret the 'outer' join parameters - semantically
        #if jin[i] is outer, then all other inputs must be marked as _padWithNulls
        if any(map(lambda jin:jin.outer, self.joinInputs)):
            assert len(self.joinInputs)==2,'outer joins are only supported on two-way joins '+str(self.joinInputs)
            for i in range(len(self.joinInputs)):
                if self.joinInputs[i].outer:
                    j = 1-i  #the other index
                    self.joinInputs[j]._padWithNulls = True
    
    def acceptInnerView(self,otherView):
        assert False, 'join cannot be RHS of a pipe - use JoinTo instead'

    def mapPlan(self):
        plan = Plan()
        innerCheckpoints = map(lambda v:v.checkpoint(), self.inners)
        step = PrereduceStep(view=self, whatToDo='doJoinMap',srcs=innerCheckpoints,dst=self.checkpoint(),why=self.explanation())
        plan.append(step)
        return plan

    def applyDict(self,mapping,innerviewsOnly=False):
        result = MapReduce.applyDict(self,mapping,innerviewsOnly=innerviewsOnly)
        #also need to map over the join inputs
        if isinstance(result,Join):
            for i in range(len(result.joinInputs)):
                result.joinInputs[i].view = result.inners[i]
        return result

    def rowGenerator(self):
        """Group objects from stdin by key, yielding tuples (row1,row2,...)."""
        lastkey = None
        lastIndex = len(self.joinInputs)-1
        somethingProducedForLastKey = False
        #accumulate a list of lists of all non-final inputs
        accumList = [ [] for i in range(lastIndex) ]
        for line in sys.stdin:
            keyStr,indexStr,valStr = line.strip().split("\t")
            key = self.planner._serializer.fromString(keyStr)
            index = int(indexStr)
            val = self.planner._serializer.fromString(valStr)
            if key != lastkey and lastkey!=None: 
                #if the final join is marked as _padWithNulls, clear
                #the accumulators, since we're doing an outer join
                #with the last view
                if self.joinInputs[lastIndex]._padWithNulls and not somethingProducedForLastKey:
                    for tup in self._joinAccumulatedValuesTo(accumList,lastIndex,None):
                        yield tup
                #reset the accumulators, since they pertain to the 
                accumList = [ [] for i in range(lastIndex) ]
                somethingProducedForLastKey = False
            if index!=lastIndex:
                #accumulate values to use in the join
                accumList[index] = accumList[index] + [val]
            else:
                #produce tuples that match the key for the last view
                for tup in self._joinAccumulatedValuesTo(accumList,lastIndex,val):
                    somethingProducedForLastKey = True
                    yield tup
            lastkey = key

    def _joinAccumulatedValuesTo(self,accumList,lastIndex,finalVal):
        # _padWithNulls as needed
        for i in range(lastIndex):
            if self.joinInputs[i]._padWithNulls and not accumList[i]:
                accumList[i] = [None]
        tupbuf = [ None for i in range(lastIndex+1) ]  #holds output
        tupbuf[lastIndex] = finalVal
        for i in range(lastIndex):
            for a in accumList[i]:
                tupbuf[i] = a
                if i==lastIndex-1 and any(tupbuf):
                        yield tuple(tupbuf)

    def explanation(self):
        innerEx = []
        for inner in self.inners:
            if innerEx: innerEx += ['THEN']
            innerEx += inner.explanation()
        return innerEx + [ 'FINALLY join to %s' % self.tag ]

    def __str__(self):
        return "Join(%s)" % ",".join(map(str,self.joinInputs)) + self.showExtras()

    def doJoinMap(self,i):
        # called by joinMapPlan with argument index, and stdin pointing to innerCheckpoints[index]
        self.doStoreKeyedRows(self.joinInputs[i].view,self.joinInputs[i].joinBy,i)

class JoinTo(Join):
    """Special case of Join which can be used as the RHS of a pipe operator."""

    def __init__(self,joinInput,by=lambda x:x):
        Join.__init__(self,Jin(None,by),joinInput)
        
    def acceptInnerView(self,otherView):
        self.joinInputs[0].view = otherView
        self.inners[0] = otherView

class Union(MapReduce):
    """Combine two or more relations, also removing duplicates."""

    def __init__(self,*inners):
        #sets self.inners
        MapReduce.__init__(self,list(inners),None)

    def acceptInnerView(self,otherView):
        assert False, 'Union cannot be RHS of a pipe - use UnionTo instead'

    def mapPlan(self):
        plan = Plan()
        innerCheckpoints = map(lambda v:v.checkpoint(), self.inners)
        step = PrereduceStep(view=self, whatToDo='doUnionMap',srcs=innerCheckpoints,dst=self.checkpoint(),why=self.explanation())
        plan.append(step)
        return plan

    def explanation(self):
        innerEx = []
        for inner in self.inners:
            if innerEx: innerEx += ['CONCAT TO']
            innerEx += inner.explanation()
        return innerEx

    def __str__(self):
        return "Union(%s)" % ",".join(map(str,self.inners)) + self.showExtras()

    def rowGenerator(self):
        lastLine = None
        for line in sys.stdin:
            if line!=lastLine:
                yield self.planner._serializer.fromString(line.strip())
            lastLine = line

    def doUnionMap(self,i):
        # called with argument index, and stdin pointing to innerCheckpoints[index]
        for row in self.inners[i].rowGenerator():
            print self.planner._serializer.toString(row)


class UnionTo(Union):
    """Special case of Union which can be used as RHS of a pipe operator."""
    
    def __init__(self,*moreInners):
        allInners = [None]+list(moreInners)
        Union.__init__(self,*allInners)
        
    def acceptInnerView(self,otherView):
        self.inners[0] = otherView

##############################################################################
#
# the top-level planner, and its supporting classes
#
##############################################################################

class Plan(object):
    """A plan constructed by a GuineaPig."""

    def __init__(self): 
        self.steps = []
        self.tasks = []

    def append(self,step): 
        self.steps.append(step)

    def includeStepsOf(self,subplan):
        self.steps += subplan.steps

    def execute(self,gp,echo=False):
        script = self.compile(gp)
        for shellcom in script:
            if echo: print 'calling:',shellcom
            subprocess.check_call(shellcom,shell=True)

    def buildTasks(self):
        """Group the steps into AbstractMapReduceTask's"""
        self.tasks = [AbstractMapReduceTask()]
        for step in self.steps:
            if not self.tasks[-1].insert(step):
                self.tasks.append(AbstractMapReduceTask())
                status = self.tasks[-1].insert(step)
                assert status, 'failure to insert '+str(step)+' in fresh AbstractMapReduceTask'

    def compile(self,gp):
        """Return a list of strings that can be run as shell commands."""
        self.buildTasks()
        logging.info("%d steps converted to %d abstract map-reduce tasks" % (len(self.steps),len(self.tasks)))
        script = []
        taskCompiler = gp._getCompiler(gp.opts['target']) 
        for task in self.tasks:
            #print 'compiling',task
            script += taskCompiler.compile(task,gp)
        return script

class Step(object):
    """A single 'step' of the plans produced by the planner."""

    def __init__(self,view):
        self.view = view
        self.reused = []  # list of views reused at this point
        self.why = []

    def setReusedViews(self,views):
        self.reused = list(views)

    def explain(self):
        """Convert an explanation - which is a list of strings - into a string"""
        return "...".join(self.why)

#
# a single step in a plan produced by the planner
#

class DistributeStep(Step):
    """Prepare a stored view for the distributed cache."""

    def __init__(self,view):
        Step.__init__(self,view)

    def __str__(self):
        return "DistributeStep(%s,reused=%s)" % (repr(self.view.tag),repr(self.reused))

class TransformStep(Step):
    """Tranform input to output."""
    def __init__(self,view,whatToDo,srcs,dst,why):
        Step.__init__(self,view)
        self.whatToDo = whatToDo
        self.srcs = srcs
        self.dst = dst
        self.why = why

    def __str__(self):
        return "TransformStep("+",".join(map(repr, [self.view.tag,self.whatToDo,self.srcs,self.dst,self.reused]))+")"

class PrereduceStep(Step):
    """A step that can be followed by a reduce step."""
    def __init__(self,view,whatToDo,srcs,dst,why):
        Step.__init__(self,view)
        self.whatToDo = whatToDo
        self.srcs = srcs
        self.dst = dst
        self.why = why

    def __str__(self):
        return "PrereduceStep("+",".join(map(repr, [self.view.tag,self.whatToDo,self.srcs,self.dst,self.reused]))+")"

class AbstractMapReduceTask(object):
    """A collection of steps that can be executed as a single map-reduce
    operation, possibly with some file managements steps to set up the
    task.  More specifically, this consists of 

    1a) distribute: a maybe-empty sequence of DistributeStep's
    2a) map: a PrereduceStep or a TransformStep 

    or else

    1b) distribute: a maybe-empty sequence of DistributeStep's
    2b) map: a PrereduceStep
    3b) reduce: a TransformStep

    Sequence 1a-2a is a map-only task, and sequence 1b-3b is a
    map-reduce task.
    """

    def __init__(self):
        self.distributeSteps = []
        self.mapStep = None
        self.reduceStep = None

    def insert(self,step):
        """Treating the AbstractMapReduceTask as a buffer, add this step to it if possible."""
        if isinstance(step,DistributeStep):
            #we can accept any number of distribute steps
            self.distributeSteps.append(step)
            return True
        elif self.mapStep==None and (isinstance(step,TransformStep) or isinstance(step,PrereduceStep)):
            #we can only have one map step, so fill up an empty mapstep slot if possible
            self.mapStep = step
            return True
        elif self.mapStep and isinstance(self.mapStep,PrereduceStep) and isinstance(step,TransformStep) and not self.reduceStep:
            #if the mapstep is a prereduce, then we can also allow any TransformStep to be used as a reduceStep
            self.reduceStep = step
            return True
        else:
            return False
            
    def reduceParallel(self,gp):
        if self.reduceStep:
            return self.reduceStep.view.parallelForMe or gp.opts['parallel']
        else:
            return self.mapStep.view.parallelForMe or gp.opts['parallel']

    def explanation(self):
        """Concatenate together the explanations for the different steps of
        2this task."""
        buf = []
        for step in self.distributeSteps:
            buf += step.why
        #reduce explanation copies the map explanation so we don't need both
        if self.reduceStep:
            buf += self.reduceStep.why
        else:
            buf += self.mapStep.why
        return buf

    def inputsAndOutputs(self):
        """Return a string summarizing the source files used as inputs, and
        the view ultimately created by this task."""
        buf = ' + '.join(self.mapStep.srcs)
        if self.reduceStep:
            buf += ' => ' + self.reduceStep.view.tag
        else:
            buf += ' => ' + self.mapStep.view.tag            
        return buf

    def __str__(self):
        buf = "mapreduce task:"
        for step in self.distributeSteps:
            buf += "\n - d "+str(step)
        buf += "\n - m " + str(self.mapStep)
        if self.reduceStep:
            buf += "\n - r " + str(self.reduceStep)
        return buf

class MRCompiler(object):

    """Abstract compiler class to convert a task to a list of commands that can be executed by the shell."""

    def compile(self,task,gp):
        script = []
        # an explanation/header
        if not task.reduceStep: 
            script += ['echo create '+task.mapStep.view.tag + ' via map: ' + task.mapStep.explain()]        
        else: 
            script += ['echo create '+task.reduceStep.view.tag +' via map/reduce: '+task.reduceStep.explain()]
        if not task.reduceStep and len(task.mapStep.srcs)==1:   #a map-only step
            mapCom = self._coreCommand(task.mapStep,gp)
            script += self.simpleMapCommands(task, gp, mapCom, task.mapStep.srcs[0], task.mapStep.dst)
        elif task.reduceStep and len(task.mapStep.srcs)==1:     #a map-reduce step
            mapCom = self._coreCommand(task.mapStep,gp)
            reduceCom = self._coreCommand(task.reduceStep,gp)
            script += self.simpleMapReduceCommands(task, gp, mapCom, reduceCom, task.mapStep.srcs[0], task.reduceStep.dst)
        elif task.reduceStep and len(task.mapStep.srcs)>1:      #multiple mappers and one reduce 
            mapComs = [self._ithCoreCommand(task.mapStep,gp,i) for i in range(len(task.mapStep.srcs))]
            reduceCom = self._coreCommand(task.reduceStep,gp)
            midpoint = gp.opts['viewdir']+'/'+task.mapStep.view.tag+'.gpmo'
            script += self.joinCommands(task, gp, mapComs, reduceCom, task.mapStep.srcs, midpoint, task.reduceStep.dst)
        else:
            assert False,'cannot compile task '+str(task)
        for step in task.distributeSteps:                       #distribute the results, if necessary
            localCopy = step.view.distributableFile()                
            maybeRemoteCopy = step.view.storedFile()
            echoCom = 'echo distribute %s: making a local copy of %s in %s' % (step.view.tag,maybeRemoteCopy,localCopy)
            script += [echoCom] + self.distributeCommands(task, gp, maybeRemoteCopy,localCopy)
        return script

    # abstract routines

    def distributeCommands(self,task,gp,maybeRemoteCopy,localCopy):
        """Distribute the remote copy to the local directory."""
        assert False, 'abstract method called'

    def simpleMapCommands(self,task,gp,mapCom,src,dst):
        """A map-only task with zero or one inputs."""
        assert False, 'abstract method called'

    def simpleMapReduceCommands(self,task,gp,mapCom,reduceCom,src,dst):
        """A map-reduce task with one inputs."""
        assert False, 'abstract method called'

    def joinCommands(self,task,gp,mapComs,reduceCom,srcs,midpoint,dst):
        """A map-reduce task with several inputs."""
        assert False, 'abstract method called'

    # utilities

    def _stepSideviewFiles(self,step):
        files = []
        for sv in step.view.sideviewsNeeded():
            files += [sv.distributableFile()]

    def _coreCommand(self,step,gp):
        """Python command to call an individual plan step."""
        return 'python %s --view=%s --do=%s' % (gp._gpigSourceFile,step.view.tag,step.whatToDo) + self.__coreCommandOptions(step,gp)

    def _ithCoreCommand(self,step,gp,i):
        """Like _coreCommand but allows index parameter to 'do' option"""
        return 'python %s --view=%s --do=%s.%d' % (gp._gpigSourceFile,step.view.tag,step.whatToDo,i) + self.__coreCommandOptions(step,gp)

    def __coreCommandOptions(self,step,gp):
        if not gp.param:
            paramOpts = '' 
        else: 
            paramOpts = " --params " + ",".join(map(lambda(k,v):k+':'+urllib.quote(v), gp.param.items()))
        nonDefaults = []
        for (k,v) in gp.opts.items():
            #pass in non-default options, or options computed from the environment
            if (gp.opts[k] != GPig.DEFAULT_OPTS[k]) or ((k in GPig.COMPUTED_OPTION_DEFAULTS) and (gp.opts[k] != GPig.COMPUTED_OPTION_DEFAULTS[k])):
                nonDefaults += ["%s:%s" % (k,urllib.quote(str(v)))]
        optsOpts = '' if not nonDefaults else " --opts " + ",".join(nonDefaults)
        reuseOpts = '' if not step.reused else " --reuse "+ " ".join(step.reused)
        return paramOpts  + optsOpts + reuseOpts


class ShellCompiler(MRCompiler):
    """Compile tasks to commands that are executable to most Unix shells."""

    def distributeCommands(self,task,gp,maybeRemoteCopy,localCopy):
        """Distribute the remote copy to the local directory."""
        return ['cp -f %s %s || echo warning: the copy failed!' % (maybeRemoteCopy,localCopy)]

    def simpleMapCommands(self,task,gp,mapCom,src,dst):
        """A map-only job with zero or one inputs."""
        if src: return [mapCom + ' < %s > %s' % (src,dst)]
        else: return [self.mapCommand(gp) + (' > %s' % (dst))]
        
    def simpleMapReduceCommands(self,task,gp,mapCom,reduceCom,src,dst):
        """A map-reduce job with one input."""
        return [mapCom + ' < ' + src + (' | %s -k1 | ' % GPig.SORT_COMMAND) +reduceCom + ' > ' + dst]

    def joinCommands(self,task,gp,mapComs,reduceCom,srcs,midpoint,dst):
        """A map-reduce job with several inputs."""
        subplan = ['rm -f %s' % midpoint]
        for i,ithMapCom in enumerate(mapComs):
            subplan += [ithMapCom + ' < ' + srcs[i] + ' >> ' + midpoint]
        subplan += [ ('%s -k1,2  < '% GPig.SORT_COMMAND) + midpoint + ' | ' + reduceCom + ' > ' + dst]
        return subplan

class HadoopCompiler(MRCompiler):
    """Compile tasks to commands that are executable to most Unix shells
    after hadoop has been installed."""

    def distributeCommands(self,task,gp,maybeRemoteCopy,localCopy):
        return ['rm -f %s' % localCopy, '%s fs -getmerge %s %s' % (GPig.HADOOP_LOC,maybeRemoteCopy,localCopy)]

    def simpleMapCommands(self,task,gp,mapCom,src,dst):
        assert src,'undefined src for this view? you may be using Wrap with target:hadoop'
        hcom = self.HadoopCommandBuf(gp,task)
        hcom.extendDef('-D','mapred.reduce.tasks=0')
        hcom.extend('-cmdenv','PYTHONPATH=.')
        hcom.extend('-input',src,'-output',dst)
        hcom.extend("-mapper '%s'" % mapCom)
        self._extendWithHadoopOptions(hcom,gp,task)
        return [ self._hadoopCleanCommand(gp,dst), hcom.asEcho(), hcom.asString() ]

    def simpleMapReduceCommands(self,task,gp,mapCom,reduceCom,src,dst):
        p = task.reduceParallel(gp)
        hcom = self.HadoopCommandBuf(gp,task)
        hcom.extendDef('-D','mapred.reduce.tasks=%d' % p)
        hcom.extend('-cmdenv','PYTHONPATH=.')
        hcom.extend('-input',src,'-output',dst)
        hcom.extend("-mapper '%s'" % mapCom)
        hcom.extend("-reducer '%s'" % reduceCom)
        if task.reduceStep.view.supportsCombiners():
            hcom.extend("-combiner '%s'" % task.reduceStep.view.convertReduceCommandToCombineCommand(reduceCom))
        self._extendWithHadoopOptions(hcom,gp,task)
        return [ self._hadoopCleanCommand(gp,dst), hcom.asEcho(), hcom.asString() ]
        
    def joinCommands(self,task,gp,mapComs,reduceCom,srcs,midpoint,dst):
        def midi(i): return midpoint + '-' + str(i)
        p = task.reduceParallel(gp)
        subplan = []
        for i in range(len(srcs)):
            hcom = self.HadoopCommandBuf(gp,task)
            hcom.extendDef('-D','mapred.reduce.tasks=%d' % p)
            hcom.extend('-cmdenv','PYTHONPATH=.')
            hcom.extend('-input',srcs[i], '-output',midi(i))
            hcom.extend("-mapper","'%s'" % mapComs[i])
            subplan += [ self._hadoopCleanCommand(gp,midi(i)), hcom.asEcho(), hcom.asString() ]
        hcombineCom = self.HadoopCommandBuf(gp,task)
        hcombineCom.extendDef('-D','mapred.reduce.tasks=%d' % p)
        hcombineCom.extendDef('-jobconf','stream.num.map.output.key.fields=3')
        hcombineCom.extendDef('-jobconf','num.key.fields.for.partition=1')
        for i in range(len(srcs)):
            hcombineCom.extend('-input',midi(i))
        hcombineCom.extend('-cmdenv','PYTHONPATH=.')
        hcombineCom.extend('-output',dst)
        hcombineCom.extend('-mapper','cat')
        hcombineCom.extend('-reducer',"'%s'" % reduceCom)
        hcombineCom.extend('-partitioner','org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner')
        self._extendWithHadoopOptions(hcom,gp,task)
        subplan += [ self._hadoopCleanCommand(gp,dst),  hcombineCom.asEcho(), hcombineCom.asString() ]
        return subplan

    def _extendWithHadoopOptions(self,hcombuf,gp,task):
        hcombuf.extendDef(*gp.hDefsForPlanner)
        hcombuf.extend(*gp.hOptsForPlanner)
        hcombuf.extendDef(*task.mapStep.view.hDefsForMe)
        hcombuf.extend(*task.mapStep.view.hOptsForMe)
        if task.reduceStep and task.reduceStep.view!=task.mapStep.view:
            hcombuf.extendDef(*task.reduceStep.view.hDefsForMe)
            hcombuf.extend(*task.reduceStep.view.hOptsForMe)

    class HadoopCommandBuf(object):
        """Utility to hold the various pieces of a hadoop command."""
        def __init__(self,gp,task):
            logging.debug('building hadoop command for '+str(task.mapStep.view.tag))
            self.invocation = [GPig.HADOOP_LOC,'jar',gp.opts['streamJar']]
            self.defs = []
            self.args = []
            self.files = []
            for f in gp._shippedFiles:
                self.files += ['-file',f]
            for sv in task.mapStep.view.sideviewsNeeded():
                self.files += ['-file',sv.distributableFile()]
            if task.reduceStep:
                for sv in task.reduceStep.view.sideviewsNeeded():
                    self.files += ['-file',sv.distributableFile()]
            logging.debug('files: '+str(self.files))
        def extend(self,*toks):
            self.args += list(toks)
        def extendDef(self,*toks):
            self.defs += list(toks)
        def asEcho(self):
            return " ".join(['echo','hadoop'] + self.args + ['...'])
        def asString(self):
            return " ".join(self.invocation+self.defs+self.files+self.args)

    def _hadoopCleanCommand(self,gp,fileName):
        """A command to remove a hdfs directory if it exists."""
        return '(%s fs -test -e %s && %s fs -rmr %s) || echo no need to remove %s' % (GPig.HADOOP_LOC,fileName, GPig.HADOOP_LOC,fileName, fileName)

#
# replacable object to save objects to disk and retrieve them
#

class RowSerializer(object):
    """Saves row objects to disk and retrieves them.  A RowSerializer is
    used internally in a Planner, and by default the one used by a
    Planner will be an instance of RowSerializer().  A user can
    override this with planner.setSerializer(). """
    def __init__(self):
        self.evaluator = GPig.SafeEvaluator()
    def toString(self,x): 
        return repr(x)
    def fromString(self,s): 
        return self.evaluator.eval(s)

#
# the planner
#

class Planner(object):
    """Can create storage plans for views that are defined as parts of it."""

    def __init__(self,**kw):

        # these are Hadoop-specific options

        self.hDefsForPlanner = []
        self.hOptsForPlanner = []

        #Parameters are used for programmatically giving user-defined
        #config information to a planner, or they can be specified in
        #the command-line.  These are usually accessed in user-defined
        #views.

        self.param = kw
        for (key,val) in GPig.getArgvParams().items():
                # don't override non-null values specified in the constructor
                if self.param.get(key)==None:
                    self.param[key] = val

        #opts are used for giving options to the planner from the
        #shell, and are used in code in this file.

        self.opts = GPig.getArgvOpts()
        for (key,val) in GPig.DEFAULT_OPTS.items():
            if (not key in self.opts): self.opts[key] = val
        for (key,type) in GPig.DEFAULT_OPT_TYPES.items():
            self.opts[key] = type(self.opts[key])

        #Provide a default serializer
        self._serializer = RowSerializer()

        #Provide a default mapping from 'target' strings to Python
        #classes which implement compilers for that target
        self._compilerRegistry = {'shell':ShellCompiler, 'hadoop':HadoopCompiler}

        #These are views that aren't associated with class variable,
        #but are instead named automatically - ie, inner views with no
        #user-provided names.
        self._autoNamedViews = {}

        #By default, use info-level logging at planning time only, not
        #at view execution time.
        if not Planner.partOfPlan(sys.argv): 
            logging.basicConfig(level=logging.INFO)
        logging.info('GuineaPig v%s %s' % (GPig.VERSION,GPig.COPYRIGHT))

        #Hadoop needs to know where to give the main script file, as
        #well as the guineapig.py file used here
        self._shippedFiles = []
        self._gpigSourceFile = sys.argv[0]
        self.ship(GPig.MY_LOC)
        self.ship(self._gpigSourceFile)

    #
    # utils
    # 

    def _setView(self,str,view):
        """Internal use only: allow the view to be retreived by name later."""
        view.tag = str
        self._autoNamedViews[str] = view

    def _buildRecursiveStoragePlan(self,view):
        """Called by view.storagePlan.""" 
        #figure out what to reuse - starting with what the user specified
        storedViews = dict(self.reusableViews)
        #also mark for eager storage anything that's used twice in the
        #plan---i.e., anything that is consumed by two or more views
        numParents = collections.defaultdict(int)
        for dv in self._descendants(view):
            for inner in dv.inners + dv.sideviews:
                numParents[inner] += 1
        for (dv,n) in numParents.items():
            if n>1 and dv.storeMe==None:
                logging.info('making %s stored because it is used %d times in creating %s' % (dv,n,view.tag))
                dv.storeMe = True

        #traverse view in pre-order and find a linear sequence of
        #views to store, each of which requires only views earlier in
        #the sequence
        storageSeq = self._storageSeq(view,storedViews) + [view.tag]
        logging.info('storage sequence is: ' + ",".join(storageSeq))

        #splice together plans for each view in the sequence,
        #after first modifying the view so that nothing is called
        #directly, but only through the ReuseView proxies
        plan = Plan()
        for tag in storageSeq:
            v = self.getView(tag,mustExist=True)
            vm = v.applyDict(storedViews,innerviewsOnly=True)
            subplan = vm.nonrecursiveStoragePlan()
            #add the correct context of reused views to the subplan,
            #so that that the actual definition of the view will be
            #rewritten appropriately to include the new ReuseView
            #proxy for it
            viewsLocallyReused = self._reuseViewDescendants(vm)
            for s in subplan.steps:
                s.setReusedViews(viewsLocallyReused)
            plan.includeStepsOf(subplan)
        return plan

    def _reuseViewDescendants(self,view):
        """Descendent views that are ReuseView's"""
        result = set()
        for dv in self._descendants(view):
            if isinstance(dv,ReuseView):
                result.add(dv.reusedViewTag)
        return result

    def _descendants(self,view):
        """Descendents of a view."""
        result = set()
        result.add(view)
        for inner in view.inners + view.sideviews:
            result = result.union(self._descendants(inner))
        return result

    def _storageSeq(self,view,storedViews):
        """Linear sequence of storage actions to take - as view tags."""
        seq = []
        for inner in view.inners + view.sideviews:
            if not inner.tag in storedViews:
                seq += self._storageSeq(inner,storedViews)
                if inner.storeMe:
                    seq += [inner.tag]
                    storedViews[inner.tag] = ReuseView(inner)
        return seq

    ##############################################################################
    # external interface to Planner
    ##############################################################################

    def setup(self):
        """Initialize planner, and views used by the planner.  This has to be
        done after the planner is fully configured by adding views."""

        self.reusableViews = {}
        # make sure view directory is valid
        if self.opts['target']=='shell' and not os.path.exists(self.opts['viewdir']):
            logging.info('creating view directory ' + self.opts['viewdir'])
            os.makedirs(self.opts['viewdir'])
        elif self.opts['target']=='hadoop':
            p = urlparse.urlparse(self.opts['viewdir'])
            if not p.path.startswith("/"):
                logging.warn('hadoop viewdir should be absolute path: will try prefixing /user/$LOGNAME')
                username = os.environ.get('LOGNAME','me')
                self.opts['viewdir'] = '/user/'+username+'/'+self.opts['viewdir']
                logging.warn('viewdir is set to '+self.opts['viewdir'])

        # Add 'tag' and planner fields to each view
        for vname in self.listViewNames():
            v = self.getView(vname)
            v.tag = vname
            v.planner = self
        def tagUnnamedViews(v,basename,index,depth):
            assert v,'null inner view for '+basename
            if not v.planner:
                v.planner = self
                autoname = '%s_%d_%s' % (basename,depth,index)
                self._setView(autoname,v)
                for i,inner in enumerate(v.inners + v.sideviews):
                    tagUnnamedViews(inner,vname,i,depth+1)
        for vname in self.listViewNames():
            v = self.getView(vname)
            for i,inner in enumerate(v.inners + v.sideviews):
                tagUnnamedViews(inner,vname,i,1)

        # Add caching options as needed
        for vname in self.listViewNames():
            v = self.getView(vname)
            v.enforceStorageConstraints()



    #
    # accessing views
    # 

    def getView(self,str,mustExist=False):
        """Find the defined relation named str, and if necessary bind its
        planner and tag appropriately."""
        v = self.__class__.__dict__.get(str) or self.__dict__.get(str) or self._autoNamedViews.get(str)
        if mustExist: assert v,'cannot find a view named '+str
        return v

    def listViewNames(self):
        def namedViews(d): return [vname for vname in d.keys() if isinstance(self.getView(vname),View)]
        userNamedViews =  namedViews(self.__class__.__dict__) + namedViews(self.__dict__)
        return userNamedViews + self._autoNamedViews.keys()

    #
    # dealing with the file storage system and related stuff
    #

    def hdefs(self,hdefList):
        """Similar to view.opts(hdefs=....) but provides defaults for all Hadoop jobs launched by a planner."""
        self.hDefsForPlanner += hdefList

    def hopts(self,hoptList):
        """Similar to view.opts(hopts=....) but provides defaults for all Hadoop jobs launched by a planner."""
        self.hOptsForPlanner += hoptList

    def ship(self,fileName):
        """Declare a set of inputs to be 'shipped' to the hadoop cluster."""
        for d in sys.path:
            location = os.path.join(d,fileName)
            if os.path.isfile(location):
                logging.debug('located %s at %s' % (fileName,location))
                self._shippedFiles.append(location)
                return
        logging.error("didn't locate %s on sys.path: path is %r" % (fileName,sys.path))
        logging.warn("note that the working directory . should always be on your PYTHONPATH")

    def setSerializer(self,serializer):
        """Replace the default serializer another RowSerializer object."""
        self._serializer = serializer
        return self

    def setEvaluator(self,rowEvaluator):
        """Specify a function which will deserialize a string that was produced
        by Python's 'repr' function."""
        self._serializer.evaluator = rowEvaluator
        return self

    def registerCompiler(self,compilerName,compilerClass):
        self._compilerRegistry[compilerName] = compilerClass

    def _getCompiler(self,target):
        """Return the compiler object used to convert AbstractMapReduceTasks
        to executable commands."""
        assert target in self._compilerRegistry, 'illegal compilation target '+target
        return self._compilerRegistry[target]()

    #
    # rest of the API for the planner
    # 

    def serialize(self,row):
        return self._serializer.toString(row)

    @staticmethod
    def partOfPlan(argv):
        """True if the command line was generated as part of a storage plan."""
        return any(s.startswith("--do") for s in argv)

    def main(self,argv):
        """Run a main that lets you --store a view, as well as doing a few other things."""
        self.setup()
        self.runMain(argv)

    def runMain(self,argv):
        """Called by main() after setup()"""

        # parse the options and dispatch appropriately
        argspec = ["store=", "cat=", "reuse", "help", "hopts=", "hdefs=",
                   "list", "pprint=", "steps=", "tasks=", "plan=", 
                   "params=", "opts=", "do=", "view="]
        try:
            optlist,args = getopt.getopt(argv[1:], 'x', argspec)
        except getopt.GetoptError:
            logging.fatal('bad option: use "--help" to get help')
            sys.exit(-1)
        optdict = dict(optlist)
        
        #import any planner-level hadoop options from the command line
        if '--hopts' in optdict:
            self.hopts(optdict['--hopts'].split())
        if '--hdefs' in optdict:
            self.hdefs(optdict['--hdefs'].split())

        # decide what views can be re-used, vs which need fresh plans
        if '--reuse' in optdict:  #reuse the views listed in the arguments
            for a in args:
                vname = View.viewNameFor(a)
                v = self.getView(vname)
                if v:
                    self.reusableViews[v.tag] = ReuseView(v)
                    logging.info("re-using data stored for view "+vname+": "+str(v))
                else:
                    logging.warn("cannot re-use view "+vname+" since it's not used in this script")

        #choose the main action to take
        if '--store' in optdict:  #store a view
            rel = self.getView(optdict['--store'],mustExist=True)            
            plan = rel.storagePlan()
            plan.execute(self, echo=self.opts['echo'])
            return
        elif '--pprint' in optdict: #print a view
            rel = self.getView(optdict['--pprint'],mustExist=True)
            rel.applyDict(self.reusableViews).pprint()
            return
        elif '--steps' in optdict: #print steps to produce a view 
            rel = self.getView(optdict['--steps'],mustExist=True)
            plan = rel.storagePlan()
            for s in plan.steps:
                print ' -',s
            return
        elif '--tasks' in optdict: #print AbstractMapReduceTasks 
            rel = self.getView(optdict['--tasks'],mustExist=True)
            plan = rel.storagePlan()
            plan.buildTasks()
            for k,task in enumerate(plan.tasks):
                print '=' * 70
                taskType = 'map-reduce' if task.reduceStep else 'map-only'
                print '%s task %d: %s' % (taskType,(k+1),task.inputsAndOutputs())
                print ' - +' + '-' * 20, 'explanation', '-' * 20
                for w in task.explanation():
                    print ' - | ',w
                print ' - +' + '-' * 20, 'commands', '-' * 20
                script = self._getCompiler(self.opts['target']).compile(task,self)
                for c in script:
                    if not c.startswith("echo"):
                        print ' - | ',c
            return
        elif '--plan' in optdict:    #print a storage plan
            rel = self.getView(optdict['--plan'],mustExist=True)
            plan = rel.storagePlan()
            script = plan.compile(self)
            print "\n".join(script)
            return
        elif '--cat' in optdict:    #store and then print a view
            assert self.opts['target']=='shell','cannot do --cat except in shell mode'
            rel = self.getView(optdict['--cat'],mustExist=True)
            plan = rel.storagePlan()
            plan.execute(self, self.opts['echo'])
            for line in open(rel.storedFile(),'r'):
                print line,
            return
        elif '--list' in optdict:   #list named views
            for vname in self.listViewNames():
                print '     ',vname,'\t',self.getView(vname)
            return
        elif '--do' in optdict:     #run an internally-generated action
            #recover what should be stored when this action is performed
            #work out what view to use and what routine to call
            rel = self.getView(optdict['--view'],mustExist=True)
            rel = rel.applyDict(self.reusableViews)
            whatToDo = optdict['--do']
            #work out the method given by 'do' and call it - note it
            #may have a single integer argument, eg doJoinMap.1
            k = whatToDo.find(".")
            if k<0:
                whatToDoMethod = getattr(rel,whatToDo)
                whatToDoMethod()                
            else:
                arg = int(whatToDo[k+1:])
                whatToDo = whatToDo[:k]
                whatToDoMethod = getattr(rel,whatToDo)
                whatToDoMethod(arg)                
            return
        else:
            usageHint = {'pprint':'print the data structure associated with the VIEW',
                         'tasks':'print the abstract map-reduce tasks needed to materialize the VIEW',
                         'plan':'print the commands that invoke each abstract map-reduce task',
                         'store':'materialize the named VIEW and store it in the view directory',
                         'cat': 'store the VIEW and then print each line to stdout'}
            print 'Guinea Pig',GPig.VERSION,GPig.COPYRIGHT
            print 'usage: python %s --(store|pprint|tasks|plan|cat) VIEW [OPTIONS] [PARAMS] --reuse VIEW1 VIEW2 ...' % sys.argv[0]
            print '       python %s --list' % sys.argv[0]
            print ''
            print 'Subcommands that take a VIEW as argument:'
            for a in usageHint:
                print ' --%s VIEW: %s'% (a,usageHint[a])
            print 'The --list subcommand lists possible VIEWs defined by this program.'
            print ''
            print 'OPTIONS are specified as "--opts key:value,...", where legal keys for "opts", with default values, are:'
            for (key,val) in GPig.DEFAULT_OPTS.items():
                print '  %s:\t%s' % (key,str(val))
            print 'The environment variables GP_STREAMJAR and GP_VIEWDIR, if defined, set two of these default values.'
            print 'Options affect Guinea Pig\'s default behavior.' 
            print ''
            print 'PARAMS are specified as "--params key:value,..." and the associated dictionary is accessible to' 
            print 'user programs via the function GPig.getArgvParams(). Params are used as program-specific inputs.'
            print ''
            print 'Values in the "opts" and "params" key/value pairs are assumed to be URL-escaped.  (Note: %3A escapes a colon.)'
            print ''
            print 'There\'s more help at http://curtis.ml.cmu.edu/w/courses/index.php/Guinea_Pig'

if __name__ == "__main__":
    print 'Guinea Pig',GPig.VERSION,GPig.COPYRIGHT
    print 'There\'s help at http://curtis.ml.cmu.edu/w/courses/index.php/Guinea_Pig'    

