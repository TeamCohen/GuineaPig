# earlier version of guineapig.py

##############################################################################
# (C) Copyright 2014 William W. Cohen.  All rights reserved.
##############################################################################

import sys
import logging
import copy
import subprocess
import collections
import os
import os.path
import urlparse
import getopt
import csv

###############################################################################
# helpers functions and data structures
###############################################################################

class GPig(object):
    """Collection of utilities for Guinea Pig."""

    HADOOP_LOC = 'hadoop'  #assume hadoop is on the path at planning time
    MY_LOC = 'guineapig1_2.py'

    #global options for Guinea Pig can be passed in with the --opts
    #command-line option, and these are the default values
    defaultJar = '/usr/lib/hadoop/contrib/streaming/hadoop-streaming-1.2.0.1.3.0.0-107.jar'
    envjar = os.environ.get('GP_STREAMJAR', defaultJar)
    DEFAULT_OPTS = {'streamJar': envjar,
                    'parallel':5,
                    'target':'shell',
                    'echo':0,
                    'viewdir':'gpig_views',
                    }
    #there are the types of each option that has a non-string value
    DEFAULT_OPT_TYPES = {'parallel':int,'echo':int}
    #we need to pass non-default options in to mappers and reducers,
    #but since the remote worker's environment can be different, we
    #also need to pass in options computed from the environment
    COMPUTED_OPTION_DEFAULTS = {'streamJar':defaultJar}

    @staticmethod
    def getArgvParams(): 
        """Return a dictionary holding the argument of the --params option in
        sys.argv."""
        return GPig.getArgvDict('--params')

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
                return dict(pair.split(":") for pair in paramString.split(","))
        return {}

    @staticmethod
    def rowsOf(view):
        """Iterate over the rows in a view."""
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

class Jin(object):
    """"Object to hold description of a single join input."""

    def __init__(self,view,by=(lambda x:x),outer=False):
        self.view = view
        self.joinBy = by
        self.outer = outer
        self._padWithNulls = False

    def __str__(self):
        viewStr = View.asTag(self.view) if self.view else '_'
        outerStr = ',outer=True' if self.outer else ''
        padStr = ',_padWithNulls=True' if self._padWithNulls else ''
        return "Jin(%s,by=%s%s%s)" % (viewStr,self.joinBy,outerStr,padStr)

class ReduceTo(object):
    """An object x that can be the argument of a reducingTo=x
    parameter in a Group view."""
    def __init__(self,baseType,by=lambda accum,val:accum+val):
        self.baseType = baseType
        self.reduceBy = by

class ReduceToCount(ReduceTo):
    """Produce the count of the number of objects that would be placed in a group."""
    def __init__(self):
        ReduceTo.__init__(self,int,by=lambda accum,val:accum+1)

class ReduceToSum(ReduceTo):
    """Produce the sum of the objects - which must be numbers - that would
    be placed in a group."""
    def __init__(self):
        ReduceTo.__init__(self,int,by=lambda accum,val:accum+val)

class ReduceToList(ReduceTo):
    """Produce a list of the objects that would be placed in a group."""
    def __init__(self):
        ReduceTo.__init__(self,list,by=lambda accum,val:accum+[val])

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

    def opts(self,stored=None):
        """Return the same view with options set appropriately.  Possible
        options include:

          - stored=True - Explicitly store this view on disk whenever
            it is used in another view's definition.  This might be set
            by the user for debugging purposes, or by the planner,
            to prevent incorrect optimizations.  Generally "inner"
            views are not explicitly stored.
            
          - stored='distributedCache' - Store this view in the working
            directory and/or the Hadoop distributed cache.
            """

        self.storeMe = stored
        return self

    def showExtras(self):
        """Printable representation of the options for a view."""
        result = ''
        flagPairs = []
        if self.storeMe: flagPairs += ['stored=%s' % repr(self.storeMe)]
        if flagPairs: result += '.opts(' + ",".join(flagPairs) + ')'
        return result

    #
    # how the view is saved on disk
    #

    def storedFile(self):
        """The file that will hold the materialized relation."""
        return self.planner.opts['viewdir'] + '/' + self.tag + '.gp'

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
        return self.planner.buildRecursiveStoragePlan(self)

    def nonrecursiveStoragePlan(self):
        """Materialize the relation, assuming that there are no descendent
        inner views that need to be materialized first."""
        plan = self.checkpointPlan()
        result = plan.extend(Step(self,'doStoreRows',self.checkpoint(),self.storedFile(),why=self.explanation()))
        return result
            
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

    def checkpoint(self):
        ## the checkpoint is the reducer input file
        return self.planner.opts['viewdir'] + '/'  + self.tag + '.gpri'

    def checkpointPlan(self):
        plan = Plan()
        for inner in self.inners:
            plan = plan.append(inner.checkpointPlan())
        return plan.append(self.mapPlan())

    def enforceStorageConstraints(self):
        for inner in self.inners:
            innerChkpt = inner.checkpoint()
            #optimizations break if you chain two map-reduces together
            if innerChkpt and innerChkpt.endswith(".gpri"):
                if not inner.storeMe:
                    logging.info('making %s stored, to make possible a downstream map-reduce view' % inner.tag)
                    inner.storeMe = True

    def mapPlan(self):
        log.error("abstract method not implemented")
        
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

class ReadCSV(Reader):
    """ Returns the lines in a CSV file, converted to Python tuples."""

    def __init__(self,src,**kw):
        Reader.__init__(self,src)
        self.kw = kw

    def rowGenerator(self):
        for tup in csv.reader(sys.stdin,**self.kw):
            yield tup

    def __str__(self):
        return 'ReadCVS("%s",%s)' % (self.src,str(self.kw)) + self.showExtras()


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
        return self.inner.explanation() + [ 'replaced to %s' % self.tag ]

    def __str__(self):
        return 'ReplaceEach(%s, by=%s)' % (View.asTag(self.inner),str(self.replaceBy)) + self.showExtras()

class Augment(Transformation):

    def __init__(self,inner=None,sideviews=None,sideview=None,loadedBy=lambda v:list(GPig.rowsOf(v))):
        Transformation.__init__(self,inner)
        assert not (sideviews and sideview), 'cannot specify both "sideview" and "sideviews"'
        self.sideviews = list(sideviews) if sideviews else [sideview]
        self.loader = loadedBy
        assert self.loader,'must specify a "loadedBy" function for Augment'

    def enforceStorageConstraints(self):
        for sv in self.sideviews:
            sv.storeMe = 'distributedCache'

    def rowGenerator(self):
        augend = self.loader(*self.sideviews)
        for row in self.inner.rowGenerator():
            yield (row,augend)

    def checkpointPlan(self):
        plan = Plan()
        plan.append(self.inner.checkpointPlan())
        #the sideviews should have been stored by the top-level
        #planner already, but they will need to be moved to a
        #distributable location
        for sv in self.sideviews:
            plan.extend(Step(sv, 'DISTRIBUTE'))
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
        step = Step(self, 'doDistinctMap', self.inner.checkpoint(), self.checkpoint(), prereduce=True, why=self.explanation())
        return Plan().extend(step)

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

    def __init__(self,inner=None,by=lambda x:x,reducingTo=ReduceToList(),retaining=None):
        MapReduce.__init__(self,[inner],retaining)
        self.groupBy = by
        self.reducingTo = reducingTo
    
    def mapPlan(self):
        step = Step(self, 'doGroupMap',self.inner.checkpoint(),self.checkpoint(),prereduce=True,why=self.explanation())
        return Plan().extend(step)

    def rowGenerator(self):
        """Group objects from stdin by key, yielding tuples (key,[g1,..,gn])."""
        lastkey = key = None
        accum = self.reducingTo.baseType()
        for line in sys.stdin:
            keyStr,valStr = line.strip().split("\t")
            key = self.planner._serializer.fromString(keyStr)
            val = self.planner._serializer.fromString(valStr)
            if key != lastkey and lastkey!=None: 
                yield (lastkey,accum)
                accum = self.reducingTo.baseType()
            accum = self.reducingTo.reduceBy(accum, val)
            lastkey = key
        if key: 
            yield (key,accum)

    def explanation(self):
        return self.inner.explanation() + ['group to %s' % self.tag]

    def __str__(self):
        return 'Group(%s,by=%s,reducingTo=%s)' % (View.asTag(self.inner),str(self.groupBy),str(self.reducingTo)) + self.showExtras()

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
        innerCheckpoints = map(lambda v:v.checkpoint(), self.inners)
        midfile = self.planner.opts['viewdir'] + '/'  + self.tag+'.gpmo'
        step = Step(self, 'doJoinMap', src=innerCheckpoints, dst=self.checkpoint(), prereduce=True, hasIndex=True, mid=midfile, why=self.explanation())
        return Plan().extend(step)

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

    def __init__(self,joinInput,by=None):
        Join.__init__(self,Jin(None,by),joinInput)
        
    def acceptInnerView(self,otherView):
        self.joinInputs[0].view = otherView
        self.inners[0] = otherView

##############################################################################
#
# the top-level planner, and its supporting classes
#
##############################################################################

class Plan(object):
    """A plan constructed by a GuineaPig."""

    def __init__(self): self.steps = []

    def extend(self,step): 
        self.steps += [step]
        return self

    def append(self,subPlan): 
        self.steps += subPlan.steps
        return self

    def execute(self,gp,echo=False):
        script = self.compile(gp)
        for shellcom in script:
            if echo: print 'calling:',shellcom
            subprocess.check_call(shellcom,shell=True)

    def compile(self,gp):
        """Return a list of strings that can be run as shell commands."""
        script = []
        i = 0
        while (i<len(self.steps)):
            s = self.steps[i]
            i += 1
            if s.whatToDo=='DISTRIBUTE':
                script += s.distributeCommands(gp)
            elif not s.prereduce: 
                script += s.mapOnlyCommands(gp)
            else:
                #look ahead to find the next reduce command
                while self.steps[i].whatToDo=='DISTRIBUTE':
                    script += self.steps[i].distributeCommands(gp)
                    i += 1
                reduceStep = self.steps[i]
                assert not reduceStep.prereduce, 'chained mapreduce steps:' + str(s) + str(reduceStep)
                i += 1
                if not isinstance(s.src,list):
                    script += s.mapReduceCommands(reduceStep,gp)
                else:
                    script += s.multiMapReduceCommands(reduceStep,gp)
        return script

class Step(object):
    """A single step of the plans produced by the planner, along with the
    methods to convert the plans into executable shell commands."""

    # Arguments: 
    #    - view is view that created this step
    #     - whatToDo is the operator, eg doStoreKeyedRows, or else DISTRIBUTE
    #     - src is the file used as input, or possibly a list of files if there are multiple inputs
    #     - dst is the file used as output
    #     - mid is a file used as tmp storage for an unsorted version of output, if needed
    #     - prereduce is True iff this is a checkpoint in a map-reduce step
    #     - hasIndex is True iff the data is of the form <key,index,value> such that
    #     items should be partitioned by key and sorted by index
    #     - why is documentation/explanation.  
    #     - reused is list of tags of views that should be reused.

    def __init__(self,view,whatToDo,src=None,dst=None,prereduce=False,hasIndex=False,mid=None,why=[],reused=[]):
        self.view = view
        self.whatToDo = whatToDo
        self.src = src
        self.dst = dst
        self.prereduce = prereduce
        self.hasIndex = hasIndex
        self.mid = mid
        self.why = why
        self.reused = reused

    def setReusedViews(self,views):
        self.reused = list(views)

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return "Step(%s,%s,src=%s,dst=%s,prereduce=%s,mid=%s,why=%s,reused=%s)" \
            % (repr(self.view.tag),repr(self.whatToDo),repr(self.src),
               repr(self.dst),repr(self.prereduce),repr(self.mid),repr(self.explain()),repr(self.reused))

    def explain(self):
        """Convert an explanation - which is a list of strings - into a string"""
        return "...".join(self.why)

    #
    # subroutines of the general case for code generation
    #

    class HadoopCommand(object):
        def __init__(self,gp,*views):
            logging.info('building hadoop command for '+str(map(lambda v:v.tag, views)))
            self.invocation = [GPig.HADOOP_LOC,'jar',gp.opts['streamJar']]
            self.defs = []
            self.args = []
            self.files = []
            for f in gp._shippedFiles:
                self.files += ['-file',f]
            for view in views:
                viewsToShip = view.sideviewsNeeded()
                if viewsToShip:
                    logging.info('shipping for '+view.tag+': '+str(map(lambda sv:sv.tag, viewsToShip)))
                    for sv in viewsToShip:
                        self.files += ['-file',sv.distributableFile()]
            logging.info('files: '+str(self.files))
        def append(self,*toks):
            self.args += list(toks)
        def appendDef(self,*toks):
            self.defs += list(toks)
        def asEcho(self):
            return " ".join(['echo','hadoop'] + self.args + ['...'])
        def asString(self):
            return " ".join(self.invocation+self.defs+self.files+self.args)

    def subplanHeader(self,reduceStep=None):
        """Generate an explanatory header for a step."""
        if not reduceStep: return ['#', 'echo create '+self.view.tag + ' via map: '+self.explain()]        
        else: return ['#', 'echo create '+reduceStep.view.tag+' via map/reduce: '+reduceStep.explain()]                    

    def coreCommand(self,gp):
        """Python command to call an individual plan step."""
        return 'python %s --view=%s --do=%s' % (gp._gpigSourceFile,self.view.tag,self.whatToDo) + self.coreCommandOptions(gp)

    def ithCoreCommand(self,gp,i):
        """Like coreCommand but allows index parameter to 'do' option"""
        return 'python %s --view=%s --do=%s.%d' % (gp._gpigSourceFile,self.view.tag,self.whatToDo,i) + self.coreCommandOptions(gp)

    def coreCommandOptions(self,gp):
        paramOpts = '' if not gp.param else " --params " + ",".join(map(lambda(k,v):k+':'+v, gp.param.items()))
        nonDefaults = []
        for (k,v) in gp.opts.items():
            #pass in non-default options, or options computed from the environment
            if (gp.opts[k] != GPig.DEFAULT_OPTS[k]) or ((k in GPig.COMPUTED_OPTION_DEFAULTS) and (gp.opts[k] != GPig.COMPUTED_OPTION_DEFAULTS[k])):
                nonDefaults += ["%s:%s" % (k,str(v))]
        optsOpts = '' if not nonDefaults else " --opts " + ",".join(nonDefaults)
        reuseOpts = '' if not self.reused else " --reuse "+ " ".join(self.reused)
        return paramOpts  + optsOpts + reuseOpts

    def hadoopClean(self,gp,fileName):
        """A command to remove a hdfs directory if it exists."""
        #return '(%s fs -test -e %s && %s fs -rmr %s) || echo no need to remove %s' % (GPig.HADOOP_LOC,fileName, GPig.HADOOP_LOC,fileName, fileName)
        return '(%s fs -test -e %s && %s fs -rmr %s) || echo no need to remove %s' % (GPig.HADOOP_LOC,fileName, GPig.HADOOP_LOC,fileName, fileName)

    #
    # actual code generation for the steps
    #
    
    # one special case - 'distribute' a computed view, ie move to distributed cache

    def distributeCommands(self,gp):
        """Special-purpose step: Make a view available for use as a side view."""
        localCopy = self.view.distributableFile()                
        maybeRemoteCopy = self.view.storedFile()
        echoCom = 'echo DISTRIBUTE %s: making a local copy of %s in %s' % (self.view.tag,maybeRemoteCopy,localCopy)
        if gp.opts['target']=='hadoop':
            return [echoCom, 'rm -f %s' % localCopy, '%s fs -getmerge %s %s' % (GPig.HADOOP_LOC,maybeRemoteCopy, localCopy)]
        else:
            return [echoCom, 'cp -f %s %s || echo warning: the copy failed!' % (maybeRemoteCopy,localCopy)]


    # one general case - a map-only step with only one input

    def mapOnlyCommands(self,gp):
        """A subplan for a mapper-only step."""
        if gp.opts['target']=='shell':
            command = None
            if self.src: command = self.coreCommand(gp) + ' < %s > %s' % (self.src,self.dst)
            else: command = self.coreCommand(gp) + (' > %s' % (self.dst))
            return self.subplanHeader() + [command]
        elif gp.opts['target']=='hadoop':
            assert self.src,'Wrap not supported for hadoop'
            hcom = self.HadoopCommand(gp,self.view)
            hcom.appendDef('-D','mapred.reduce.tasks=0')
            hcom.append('-input',self.src,'-output',self.dst)
            hcom.append("-mapper '%s'" % self.coreCommand(gp))
            return self.subplanHeader() + [ hcom.asEcho(), self.hadoopClean(gp,self.dst), hcom.asString() ]
        else:
            assert False

    # another general case - a map-reduce step

    def mapReduceCommands(self,reduceStep,gp):
        """A subplan for a map-reduce step followed by a reduce, where the map has one input."""
        if gp.opts['target']=='shell':
            command = self.coreCommand(gp) + (' < %s' % self.src) + ' | sort -k1 | '+reduceStep.coreCommand(gp) + (' > %s' % reduceStep.dst)
            return self.subplanHeader(reduceStep) + [command]
        elif gp.opts['target']=='hadoop':
            hcom = self.HadoopCommand(gp,self.view,reduceStep.view)
            hcom.appendDef('-D','mapred.reduce.tasks=%d' % gp.opts['parallel'])
            hcom.append('-input',self.src,'-output',reduceStep.dst)
            hcom.append("-mapper '%s'" % self.coreCommand(gp))
            hcom.append("-reducer '%s'" % reduceStep.coreCommand(gp))
            return self.subplanHeader(reduceStep) + [ hcom.asEcho(), self.hadoopClean(gp,reduceStep.dst), hcom.asString() ]
        else:
            assert False

    # another general case - a map-reduce step with multiple map inputs

    def multiMapReduceCommands(self,reduceStep,gp):
        """A subplan for a map-reduce step followed by a reduce, where the map has many inputs."""
        if gp.opts['target']=='shell':
            subplan = ['rm -f %s' % self.mid]
            for i in range(len(self.src)):
                subplan += [ self.ithCoreCommand(gp,i) + ' < %s >> %s' % (self.src[i],self.mid) ]
            sortOpts = '-k1,2' if self.hasIndex else '-k1'
            subplan += [ 'sort ' + sortOpts + ' < ' + self.mid + ' | ' + reduceStep.coreCommand(gp) + (' > %s' % reduceStep.dst)]
            return self.subplanHeader(reduceStep) + subplan
        elif gp.opts['target']=='hadoop':
            def midi(i): return self.mid + '-' + str(i)
            subplan = []
            for i in range(len(self.src)):
                hcom = self.HadoopCommand(gp,self.view)
                hcom.appendDef('-D','mapred.reduce.tasks=%d' % gp.opts['parallel'])
                hcom.append('-input',self.src[i], '-output',midi(i))
                hcom.append("-mapper","'%s'" % self.ithCoreCommand(gp,i))
                subplan += [ self.hadoopClean(gp,midi(i)), hcom.asEcho(), hcom.asString() ]
            hcombineCom = self.HadoopCommand(gp,reduceStep.view)
            hcombineCom.appendDef('-D','mapred.reduce.tasks=%d' % gp.opts['parallel'])
            if (self.hasIndex): 
                hcombineCom.appendDef('-jobconf','stream.num.map.output.key.fields=3')
                hcombineCom.appendDef('-jobconf','num.key.fields.for.partition=1')
            for i in range(len(self.src)):
                hcombineCom.append('-input',midi(i))
            hcombineCom.append('-output',reduceStep.dst)
            hcombineCom.append('-mapper','cat')
            hcombineCom.append('-reducer',"'%s'" % reduceStep.coreCommand(gp))
            if (self.hasIndex): 
                hcombineCom.append('-partitioner','org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner')
            subplan += [ self.hadoopClean(gp,reduceStep.dst),  hcombineCom.asEcho(), hcombineCom.asString() ]
            return self.subplanHeader(reduceStep) + subplan
        else:
            assert False

class RowSerializer(object):
    """Saves row objects to disk and retrieves them."""
    def __init__(self,target):
        self._target = target
        self._reprInverse = None
    def toString(self,x): 
        return repr(x)
    def fromString(self,s): 
        if self._reprInverse: return self._reprInverse(s)
        else: return eval(s)

#
# the planner
#

class Planner(object):
    """Can create storage plans for views that are defined as parts of it."""

    def __init__(self,**kw):

        #parameters are used for programmatically give user-defined
        #config information to a planner, or they can be specified in
        #the command-line
        self.param = kw
        for (key,val) in GPig.getArgvParams().items():
                # don't override non-null values specified in the constructor
                if self.param.get(key)==None:
                    self.param[key] = val

        #opts are used for giving options to the planner from the shell
        self.opts = GPig.getArgvOpts()
        for (key,val) in GPig.DEFAULT_OPTS.items():
            if (not key in self.opts): self.opts[key] = val
        for (key,type) in GPig.DEFAULT_OPT_TYPES.items():
            self.opts[key] = type(self.opts[key])

        #use serializer appropriate for the target
        self._serializer = RowSerializer(self.opts['target'])

        #views that aren't associated with class variable, but are
        #instead named automatically - ie, inner views with no
        #user-provided names.
        self._autoNamedViews = {}

        #by default, use info-level logging at planning time
        if not Planner.partOfPlan(sys.argv): 
            logging.basicConfig(level=logging.INFO)

        #hadoop needs to know where to give the main script file,
        #as well as the guineapig.py file it uses
        self._gpigSourceFile = sys.argv[0]
        self._shippedFiles = [GPig.MY_LOC,self._gpigSourceFile]

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
                logging.warn('hadoop viewdir should be absolite path: will try prefixing /user/$LOGNAME')
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
    # utils
    # 
            

    def getView(self,str,mustExist=False):
        """Find the defined relation named str, and if necessary bind its
        planner and tag appropriately."""
        v = self.__class__.__dict__.get(str) or self.__dict__.get(str) or self._autoNamedViews.get(str)
        if mustExist: assert v,'cannot find a view named '+str
        return v

    def _setView(self,str,view):
        """Internal use only: allow the view to be retreived by name later."""
        view.tag = str
        self._autoNamedViews[str] = view

    def listViewNames(self):
        def namedViews(d): return [vname for vname in d.keys() if isinstance(self.getView(vname),View)]
        userNamedViews =  namedViews(self.__class__.__dict__) + namedViews(self.__dict__)
        return userNamedViews + self._autoNamedViews.keys()

    #
    # planning
    # 

    def buildRecursiveStoragePlan(self,view):
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
            plan.append(subplan)
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

    #
    # dealing with the file storage system and related stuff
    #

    def ship(self,*fileNames):
        """Declare a set of inputs to be 'shipped' to the hadoop cluster."""
        self._shippedFiles += fileNames

    def setSerializer(self,serializer):
        """Replace the default serializer another RowSerializer object."""
        self._serializer = serializer
        return self

    def setReprInverseFun(self,reprInverse):
        """Specify a function which will deserialize a string that was produced
        by Python's 'repr' function."""
        self._serializer._reprInverse = reprInverse
        return self

    #
    # rest of the API for the planner
    # 

    @staticmethod
    def partOfPlan(argv):
        """True if the command line was generated as part of a storage plan."""
        return any(s.startswith("--do") for s in argv)

    def main(self,argv):
        """Run a main that lets you --store a view, as well as doing a few other things."""
        self.setup()
        self.runMain(argv)

    def runMain(self,argv):

        # parse the options and dispatch appropriately
        argspec = ["store=", "cat=", "reuse", 
                   "list", "pprint=", "steps=", "plan=", 
                   "params=", "opts=", "do=", "view="]
        optlist,args = getopt.getopt(argv[1:], 'x', argspec)
        optdict = dict(optlist)
        
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
        elif '--steps' in optdict: #print a view
            rel = self.getView(optdict['--steps'],mustExist=True)
            plan = rel.storagePlan()
            for s in plan.steps:
                print ' -',s
            return
        elif '--plan' in optdict:    #print a storage plan
            rel = self.getView(optdict['--plan'],mustExist=True)
            plan = rel.storagePlan()
            print "\n".join(plan.compile(self))
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
            print 'usage: --[store|pprint|steps|plan|cat] view [--opts key:val,...] [--params key:val,...] --reuse view1 view2 ...]'
            print '       --[list]'
            print 'current legal keys for "opts", with default values:'
            for (key,val) in GPig.DEFAULT_OPTS.items():
                print '  %s:%s' % (key,str(val))
            print 'There\'s more help at http://curtis.ml.cmu.edu/w/courses/index.php/Guinea_Pig'

if __name__ == "__main__":
    print 'There\'s help at http://curtis.ml.cmu.edu/w/courses/index.php/Guinea_Pig'    
