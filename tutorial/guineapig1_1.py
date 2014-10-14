##############################################################################
# (C) Copyright 2014 William W. Cohen.  All rights reserved.
##############################################################################

import sys
import logging
import getopt
import os
import os.path
import subprocess
import collections
import urlparse
import csv

###############################################################################
# helpers and data structures
###############################################################################

class GPig(object):
    """Collection of utilities for Guinea Pig."""

    HADOOP_LOC = 'hadoop'  #assume hadoop is on the path at planning time

    #global options for Guinea Pig can be passed in with the --opts
    #command-line option, and these are the default values
    envjar = os.environ.get('GP_STREAMJAR')
    defaultJar = '/usr/lib/hadoop/contrib/streaming/hadoop-streaming-1.2.0.1.3.0.0-107.jar'
    DEFAULT_OPTS = {'streamJar': envjar or defaultJar,
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
    def getArgvParams(): return GPig.getArgvDict('--params')

    @staticmethod
    def getArgvOpts(): return GPig.getArgvDict('--opts')
    
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
    """"Object to hold descripton of a single join input."""

    def __init__(self,view,by=(lambda x:x),outer=False):
        self.view = view
        self.joinBy = by
        self.outer = outer
        self._padWithNulls = False

    def __str__(self):
        if self.view: viewStr = View.asTag(self.view)
        else: viewStr = '_'
        if self.outer: outerStr = ',outer=True'
        else: outerStr = ''
        if self._padWithNulls: padStr = ',_padWithNulls=True'
        else: padStr = ''
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
    """Produce the count of the number of objects that would be placed in a group."""
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
    """A relation object for guineaPig.  A view - usually abbreviated rel,
    r, s, t, - can be "materialized" to produce and unordered bag of
    rows - usually abbreviated ro.  A row is just an arbitrary python
    data structure, which must be something that can be stored and
    retrieved by the RowSerializer.  A row can be anything, but often
    the top-level structure is a python tuple (a,b,c,...) or a dict
    mapping small integers 0,1,... to different parts of the row.
    
    A guineapig "planner" knows how to construct "plans" that store
    materialized relations on disk. These plans sometimes include
    creating 'checkpoints', which are things places on disk, often
    stored materialized relations, or sometimes intermediate outputs
    or these."""

    def __init__(self):
        """The planner and tag must be set before this is used."""
        self.planner = None       #pointer to planner object
        self.tag = None           #for naming storedFile and checkpoints
        self.storeMe = None       #try and store this view if true
        self.retainedPart = None  #used in map-reduce views only

    #
    # ways to modify a view
    # 

    def opts(self,stored=None):
        """Return the same view with options set appropriately.e"""
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
        elif vname.endswith(".done"): vname = vname[0:-len(".done")]            
        return vname

    #
    # semantics of the view
    #

    def checkpoint(self):
        """A checkpoint is a file that is created in the course of
        materializing a view.  This function is the latest checkpoint
        from which the the relation can be materialized."""
        if self.storeMe: return self.storedFile()
        else: return self.unstoredCheckpoint()

    def unstoredCheckpoint(self):
        """Checkpoint for this view, if the storeMe flag is not set."""
        assert False, 'abstract method called'

    def checkpointPlan(self):
        """A plan to produce checkpoint().  Plans are constructed with the
        help of the planner, and steps in the plan are executed by
        delegation, thru the planner, to methods of this class named
        doFoo."""
        if self.storeMe: return self.storagePlan()
        else: return self.unstoredCheckpointPlan()

    def unstoredCheckpointPlan(self):
        """Plan to produce the checkpoint for this view, if the
        storeMe flag is not set."""
        assert False, 'abstract method called'

    def rowGenerator(self):
        """A generator for the rows in this relation."""
        if self.storeMe and (self.tag in self.planner.alreadyStored):
            for line in sys.stdin:
                yield self.planner._serializer.fromString(line.strip())
        else:
            for row in self.unstoredRowGenerator():
                yield row

    def explanation(self):
        """Return an explanation of how rows are generated."""
        if self.storeMe and (self.tag in self.planner.viewsPlannedToExist):
            return ['read %s with %s' % (self.storedFile(),self.tag)]
        else:
            return self.unstoredExplanation()

    def unstoredExplanation(self):
        """Return an explanation of how rows were generated, ignoring caching issues."""
        assert False,'abstract method called'

    def storagePlan(self):
        """A plan to materialize the relation. """ 
        if self.storeMe and self.tag in self.planner.viewsPlannedToExist:
            return Plan()
        else:
            #WARNING: these computations have to be done in the right order, since planning has the side effect of updating
            #the filePlannedToExist predicate.  
            # 1) build the pre-plan, to set up the view's checkpoints
            plan = self.unstoredCheckpointPlan()
            # 2a) compute the next step of the plan, along with the explanation
            step = Step(self,'doStoreRows',self.unstoredCheckpoint(),self.storedFile(),
                        why=self.explanation(),
                        existingViews=set(self.planner.viewsPlannedToExist)) #shallow copy of current state
            result = plan.extend( step )
            # 2b) if necessary, add a step to upload the 

            # 3) Record that this file has been stored for lated calls to explanation() and storagePlan()
            logging.debug('marking %s as planned-to-exist' % self.tag)
            self.planner.viewsPlannedToExist.add(self.tag)
            # 4) return the result
            return result
            
    def doStoreRows(self):
        """Called by planner at execution time to store the rows of the view."""
        for row in self.rowGenerator():
            print self.planner._serializer.toString(row)

    #
    # traversing and defining views
    #

    def innerViews(self):
        """List of all views that are used as direct inputs."""
        return []

    def nonInnerPrereqViews(self):
        """List any non-inner views that need to be created before the view is executed."""
        return []

    def __or__(self,otherView):
        """Overload the pipe operator x | y to return with y, with x as its inner view."""
        otherView.acceptInnerView(self)
        return otherView

    def acceptInnerView(self,otherView):
        """Replace an appropriate input view with otherView. To be 
        used with the pipe operator."""
        assert False,'abstract method called'

    #
    # meta plans - sequence of store commands
    #
    def metaplan(self,previouslyExistingViews):
        plannedViews = set(previouslyExistingViews) #copy
        return self._metaplanTraverse(plannedViews) + [self.tag]

    def _metaplanTraverse(self,plannedViews):
        mplan = []
        try:
            sideInnerviews = self.sideviews
        except AttributeError:
            sideInnerviews = []            
        for inner in self.innerViews() + sideInnerviews:
            if not inner.tag in plannedViews:
                mplan += inner._metaplanTraverse(plannedViews)
                if inner.storeMe:
                    mplan += [inner.tag]
                    plannedViews.add(inner.tag)
        return mplan

    #
    # printing views
    #

    def pprint(self,depth=0,alreadyPrinted=None,sideview=False):
        """Print a readable representation of the view."""
        if alreadyPrinted==None: alreadyPrinted = set()
        tabStr = '| ' * depth
        tagStr = str(self.tag)
        sideviewIndicator = '*' if sideview else ''
        if self in alreadyPrinted:
            print tabStr + sideviewIndicator + tagStr + ' = ' + '...'
        else:
            sideViewInfo = "  sideviews: {"+",".join(map(lambda x:x.tag, self.nonInnerPrereqViews()))+"}" if self.nonInnerPrereqViews() else ""
            print tabStr + sideviewIndicator + tagStr + ' = ' + str(self) + sideViewInfo
            alreadyPrinted.add(self)
            for inner in self.innerViews():
                inner.pprint(depth+1,alreadyPrinted)
            try:
                for inner in self.sideviews:
                    inner.pprint(depth+1,alreadyPrinted,sideview=True)
            except AttributeError:
                pass

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
    """Wrapper around a stored file relation."""

    def __init__(self,src):
        View.__init__(self)
        self.src = src

    def unstoredCheckpoint(self): 
        return self.src

    def unstoredCheckpointPlan(self):
        return Plan()

    def unstoredExplanation(self):
        return [ 'read %s with %s' % (str(self.src),self.tag) ]

class Transformation(View):
    """A relation that takes an inner relation and processes in a
    stream-like way, including operators like project, flatten,
    select."""

    def __init__(self,inner=None):
        View.__init__(self)
        self.inner = inner
    
    def innerViews(self):
        return [self.inner]

    def nonInnerPrereqViews(self):
        assert self.inner, 'no inner view defined for ' + str(self)
        return self.inner.nonInnerPrereqViews()

    def acceptInnerView(self,otherView):
        assert not self.inner,'An inner view is defined for '+self.tag+' so you cannot use it as RHS of a pipe'
        self.inner = otherView

    # The transformation will stream through the inner relation,
    # and produce a new version, so the latest checkpoint and
    # plan to produce it are delegated to the inner View.

    def unstoredCheckpoint(self):
        return self.inner.checkpoint()

    def unstoredCheckpointPlan(self):
        plan = Plan()
        plan.append(self.inner.checkpointPlan())
        return plan

    def unstoredExplanation(self):
        return self.inner.explanation() + [ 'transform to %s' % self.tag ]

class MapReduce(View):
    """A view that takes an inner relation and processes in a
    map-reduce-like way."""

    def __init__(self,inners,retaining):
        View.__init__(self)
        self.inners = inners
        self.retainedPart = retaining
    
    def innerViews(self):
        return self.inners

    def nonInnerPrereqViews(self):
        result = []
        for inner in self.inners:
            result += inner.nonInnerPrereqViews()
        return result

    def reduceInputFile(self): 
        ## the checkpoint is the reducer input file
        return self.planner.opts['viewdir'] + '/'  + self.tag + '.gpri'

    @staticmethod
    def isReduceInputFile(fileName):
        return fileName.endswith('.gpri')

    def unstoredCheckpoint(self):
        return self.reduceInputFile()

    def unstoredCheckpointPlan(self):
        plan = Plan()
        for inner in self.inners:
            plan = plan.append(inner.checkpointPlan())
        return plan.append(self.mapPlan())

    def innerCheckpoints(self):
        result = []
        for inner in self.inners:
            result += [inner.checkpoint()]
        return result

    def mapPlan(self):
        log.error("abstract method not implemented")
        
    def doStoreKeyedRows(self,subview,key,index):
        """Utility method to compute keys and store key-value pairs.  Usually
        used as the main step in a mapPlan. """
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

class ReadLines(Reader):
    """ Returns the lines in a file, as python strings."""

    def __init__(self,src):
        Reader.__init__(self,src)

    def unstoredRowGenerator(self):
        for line in sys.stdin:
            yield line

    def __str__(self):
        return 'ReadLines("%s")' % self.src + self.showExtras()

class ReadCSV(Reader):
    """ Returns the lines in a CSV file, converted to Python tuples."""

    def __init__(self,src,**kw):
        Reader.__init__(self,src)
        self.kw = kw

    def unstoredRowGenerator(self):
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

    def unstoredRowGenerator(self):
        for row in self.inner.rowGenerator():
            yield self.replaceBy(row)

    def unstoredExplanation(self):
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

    def nonInnerPrereqViews(self):
        return self.inner.nonInnerPrereqViews() + self.sideviews

    def unstoredRowGenerator(self):
        augend = self.loader(*self.sideviews)
        for row in self.inner.rowGenerator():
            yield (row,augend)

    def unstoredCheckpointPlan(self):
        plan = Plan()
        for sv in self.sideviews:
            plan = plan.append(sv.storagePlan())
            plan = plan.extend(Step(sv, 'DISTRIBUTE'))
        plan.append(self.inner.checkpointPlan())
        return plan

    def unstoredExplanation(self):
        return self.inner.explanation() + [ 'augmented to %s' % self.tag ]

    def __str__(self):
        sideviewTags = loaderTag = '*UNSPECIFIED*'
        if self.sideviews!=None:
            sideviewTags = ",".join(map(View.asTag,self.sideviews))
        if self.loader!=None:
            loaderTag = str(self.loader)
        return 'Augment(%s,sideviews=%s,loadedBy=s%s)' % (View.asTag(self.inner),sideviewTags,loaderTag) + self.showExtras()


class Format(ReplaceEach):
    """ Like ReplaceEach, but output should be a string, and it will be be
    stored as strings, ie without using the serializer."""

    def __init__(self,inner=None,by=lambda x:str(x)):
        ReplaceEach.__init__(self,inner,by)

    def doStoreRows(self):
        for row in self.rowGenerator():
            print row

    def __str__(self):
        return 'Format(%s, by=%s)' % (View.asTag(self.inner),str(self.replaceBy)) + self.showExtras()

class Flatten(Transformation):
    """ Example: 
        def idwordGen(row): 
           for w in row['words']: yield (row['id'],w)
        x = gp.Flatten(y, by=idwordGen(row))

    In 'by=f', f is a python function that takes a row and yields
    multiple new rows. """

    def __init__(self,inner=None,by=None):
        Transformation.__init__(self,inner)
        self.flattenBy = by

    def unstoredRowGenerator(self):
        for row in self.inner.rowGenerator():
            for flatrow in self.flattenBy(row):
                yield flatrow

    def unstoredExplanation(self):
        return self.inner.explanation() + [ 'flatten to %s' % self.tag ]

    def __str__(self):
        return 'Flatten(%s, by=%s)' % (View.asTag(self.inner),str(self.flattenBy)) + self.showExtras()

class Filter(Transformation):
    """Filter out a subset of rows that match some predicate."""
    
    def __init__(self,inner=None,by=lambda x:x):
        Transformation.__init__(self,inner)
        self.filterBy = by

    def unstoredRowGenerator(self):
        for row in self.inner.rowGenerator():
            if self.filterBy(row):
                yield row

    def unstoredExplanation(self):
        return self.inner.explanation() + [ 'filtered to %s' % self.tag ]

    def __str__(self):
        return 'Filter(%s, by=%s)' % (View.asTag(self.inner),str(self.filterBy)) + self.showExtras()

class Distinct(MapReduce):
    """Remove duplicate rows."""

    def __init__(self,inner=None,retaining=None):
        MapReduce.__init__(self,[inner],retaining)
        self.inner = inner

    def acceptInnerView(self,otherView):
        assert not self.inner,'An inner view is defined for '+self.tag+' so you cannot use it as RHS of a pipe'
        self.inner = otherView
        self.inners = [self.inner]

    def mapPlan(self):
        step = Step(self, 'doDistinctMap', self.inner.checkpoint(), self.checkpoint(), prereduce=True, 
                    why=self.explanation(),
                    existingViews=set(self.planner.viewsPlannedToExist)) #shallow of current state
        return Plan().extend(step)

    def doDistinctMap(self):
        # called by groupMapAndSortStep
        self.inner.doStoreRows()

    def unstoredRowGenerator(self):
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

    def unstoredExplanation(self):
        return self.inner.explanation() + [ 'make distinct to %s' % self.tag]

    def __str__(self):
        return 'Distinct(%s)' % (View.asTag(self.inner)) + self.showExtras()

class Group(MapReduce):
    """Group by some property of a row, defined with the 'by' option.
    Default outputs are tuples (x,[r1,...,rk]) where the ri's are rows
    that have property values of x."""

    def __init__(self,inner=None,by=lambda x:x,reducingTo=ReduceToList(),retaining=None):
        MapReduce.__init__(self,[inner],retaining)
        self.inner = inner
        self.groupBy = by
        self.reducingTo = reducingTo
    
    def acceptInnerView(self,otherView):
        assert not self.inner,'An inner view is defined for '+self.tag+' so you cannot use it as RHS of a pipe'
        self.inner = otherView
        self.inners = [self.inner]

    def mapPlan(self):
        step = Step(self, 'doGroupMap',self.inner.checkpoint(),self.checkpoint(),prereduce=True,
                    why=self.explanation(),
                    existingViews=set(self.planner.viewsPlannedToExist)) #shallow copy of current state
        return Plan().extend(step)

    def doGroupMap(self):
        # called by groupMapAndSortStep
        self.doStoreKeyedRows(self.inner,self.groupBy,-1)

    def unstoredRowGenerator(self):
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

    def unstoredExplanation(self):
        return self.inner.explanation() + ['group to %s' % self.tag]

    def __str__(self):
        return 'Group(%s,by=%s,reducingTo=%s)' % (View.asTag(self.inner),str(self.groupBy),str(self.reducingTo)) + self.showExtras()

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
        assert self.unpairedJoinBy, 'join cannot be RHS of a pipe'
        #assert self.unpairedJoinBy, 'join can only be RHS of a pipe if it contains a "by" argument not inside a "Jin" join-input'
        #self.joinInputs = joinInputs + [Jin(otherView,by=unpairedJoinBy)]
        #self.inners = map(lambda x:x.view, self.joinInputs)

    def mapPlan(self):
        previousCheckpoints = self.innerCheckpoints()
        midfile = self.planner.opts['viewdir'] + '/'  + self.tag+'.gpmo'
        step = Step(self, 'doJoinMap', src=previousCheckpoints, dst=self.checkpoint(), prereduce=True, hasIndex=True, mid=midfile, 
                    existingViews=set(self.planner.viewsPlannedToExist), #shallow copy of current state
                    why=self.explanation())
        return Plan().extend(step)

    def doJoinMap(self,i):
        # called by joinMapPlan with argument index, and stdin pointing to previousCheckpoints[index]
        self.doStoreKeyedRows(self.joinInputs[i].view,self.joinInputs[i].joinBy,i)

    def unstoredRowGenerator(self):
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

    def unstoredExplanation(self):
        innerEx = []
        for inner in self.inners:
            if innerEx: innerEx += ['THEN']
            innerEx += inner.explanation()
        return innerEx + [ 'FINALLY join to %s' % self.tag ]

    def __str__(self):
        return "Join(%s)" % ",".join(map(str,self.joinInputs)) + self.showExtras()

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
                script += s.distribute(gp)
            elif not s.prereduce: 
                script += s.mapOnlySubscript(gp)
            else:
                reduceStep = self.steps[i]
                assert not reduceStep.prereduce, 'chained mapreduce steps:' + str(s) + str(reduceStep)
                i += 1
                if s.src.__class__ != [].__class__:
                    script += s.mapReduceSubscript(reduceStep,gp)
                else:
                    script += s.multiMapReduceSubscript(reduceStep,gp)
        return script

class Step(object):
    """Steps for the planner."""

    def __init__(self,view,whatToDo,src=None,dst=None,prereduce=False,hasIndex=False,mid=None,why=[],existingViews=set()):
        """Arguments: 
        - view is view that created this step
        - whatToDo is the operator, eg doStoreKeyedRows, or else DISTRIBUTE
        - existingViews is a set of views that are expected to be already stored
        - src is the file used as input, or possibly a LIST of files if there are multiple inputs
        - dst is the file used as output
        - mid is a file used as tmp storage for an unsorted version of output, if needed
        - prereduce is True iff this is a checkpoint in a map-reduce step
        - hasIndex is True iff the data is of the form <key,index,value> such that
        items should be partitioned by key and sorted by index
        - why is documentation/explanation.  """

        self.view = view
        self.whatToDo = whatToDo
        self.existingViews = existingViews
        self.src = src
        self.dst = dst
        self.prereduce = prereduce
        self.hasIndex = hasIndex
        self.mid = mid
        self.why = why

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return "Step('%s','%s',src=%s,dst='%s',prereduce=%s,mid=%s,why=%s,existingViews=%s)" \
            % (self.view.tag,self.whatToDo,repr(self.src),
               self.dst,repr(self.prereduce),repr(self.mid),repr(self.explain()),repr(self.existingViews))

    def explain(self):
        """Convert an explanation - which is a list of strings - into a string"""
        return "...".join(self.why)

    # actual code generation for the steps

    class HadoopCommand(object):
        def __init__(self,gp,view):
            self.invocation = [GPig.HADOOP_LOC,'jar',gp.opts['streamJar']]
            self.defs = []
            self.args = []
            self.files = []
            for f in gp._shippedFiles:
                self.files += ['-file',f]
            for v in view.nonInnerPrereqViews():
                self.files += ['-file',v.distributableFile()]
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
        if not reduceStep: return ['#', 'echo map '+self.view.tag + ': '+self.explain()]        
        else: return ['#', 'echo map/reduce '+self.view.tag+ '/'+ reduceStep.view.tag + ': '+reduceStep.explain()]                    


    def coreCommand(self,gp):
        """Python command to call an individual plan step."""
        return 'python %s --view=%s --do=%s' % (gp._gpigSourceFile,self.view.tag,self.whatToDo) + self.coreCommandOptions(gp)

    def ithCoreCommand(self,gp,i):
        """Like coreCommand but allows index parameter to 'do' option"""
        return 'python %s --view=%s --do=%s.%d' % (gp._gpigSourceFile,self.view.tag,self.whatToDo,i) + self.coreCommandOptions(gp)

    def coreCommandOptions(self,gp):
        paramOpts = '' if not gp.param else " --params " + ",".join(map(lambda(k,v):k+':'+v, gp.param.items()))
        alreadyStoredOpts = '' if not self.existingViews else " --alreadyStored "+",".join(self.existingViews)
        nonDefaults = []
        for (k,v) in gp.opts.items():
            #pass in non-default options, or options computed from the environment
            if (gp.opts[k] != GPig.DEFAULT_OPTS[k]) or ((k in GPig.COMPUTED_OPTION_DEFAULTS) and (gp.opts[k] != GPig.COMPUTED_OPTION_DEFAULTS[k])):
                nonDefaults += ["%s:%s" % (k,str(v))]
        optsOpts = '' if not nonDefaults else " --opts " + ",".join(nonDefaults)
        return paramOpts  + optsOpts + alreadyStoredOpts

    def hadoopClean(self,gp,fileName):
        """A command to remove a hdfs directory if it exists."""
        return '(%s fs -test -e %s && %s fs -rmr %s) || echo no need to remove %s' % (GPig.HADOOP_LOC,fileName, GPig.HADOOP_LOC,fileName, fileName)

    def distribute(self,gp):
        """Make a view availablefor use as a side view."""
        localCopy = self.view.distributableFile()                
        maybeRemoteCopy = self.view.storedFile()
        echo = 'echo making a local copy of %s in %s' % (maybeRemoteCopy,localCopy)
        if gp.opts['target']=='hadoop':
            return [echo, 'rm -f %s' % localCopy, '%s fs -getmerge %s %s' % (GPig.HADOOP_LOC,maybeRemoteCopy, localCopy)]
        else:
            return [echo, 'cp -f %s %s || echo warning: the copy failed!' % (maybeRemoteCopy,localCopy)]

    def mapOnlySubscript(self,gp):
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

    def mapReduceSubscript(self,reduceStep,gp):
        """A subplan for a map-reduce step followed by a reduce, where the map has one input."""
        if gp.opts['target']=='shell':
            command = self.coreCommand(gp) + (' < %s' % self.src) + ' | sort -k1 | '+reduceStep.coreCommand(gp) + (' > %s' % reduceStep.dst)
            return self.subplanHeader(reduceStep) + [command]
        elif gp.opts['target']=='hadoop':
            hcom = self.HadoopCommand(gp,self.view)
            hcom.appendDef('-D','mapred.reduce.tasks=%d' % gp.opts['parallel'])
            hcom.append('-input',self.src, '-output',reduceStep.dst)
            hcom.append("-mapper '%s'" % self.coreCommand(gp))
            hcom.append("-reducer '%s'" % reduceStep.coreCommand(gp))
            return self.subplanHeader(reduceStep) + [ hcom.asEcho(), self.hadoopClean(gp,reduceStep.dst), hcom.asString() ]
        else:
            assert False

    def multiMapReduceSubscript(self,reduceStep,gp):
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
            hcombineCom = self.HadoopCommand(gp,self.view)
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
        #the viewsPlannedToExist is set using the "--reuse" option at
        #planning time, and incrementally added to as the plan is with
        #commands that actually store a view.
        self.viewsPlannedToExist = set()
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
        self._shippedFiles = ['guineapig.py',self._gpigSourceFile]

    def setup(self):
        """Initialize planner, and views used by the planner.  This has to be
        done after the planner is fully configured by adding views."""

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
                for i,inner in enumerate(v.innerViews() + v.nonInnerPrereqViews()):
                    tagUnnamedViews(inner,vname,i,depth+1)
        for vname in self.listViewNames():
            v = self.getView(vname)
            for i,inner in enumerate(v.innerViews() + v.nonInnerPrereqViews()):
                tagUnnamedViews(inner,vname,i,1)

        # Add caching options as needed

        # a mapreduce step can't use a reduce-input as a checkpoint
        # so introduce caching as needed
        for vname in self.listViewNames():
            v = self.getView(vname)
            if isinstance(v,MapReduce):
                for inner in v.inners:
                    innerCheckpoint = inner.checkpoint()
                    if innerCheckpoint and MapReduce.isReduceInputFile(innerCheckpoint):
                        if not inner.storeMe:
                            logging.info('making %s stored, to make possible a downstream map-reduce view' % inner.tag)
                            inner.storeMe = True

        # you can't combine computation of an Augment with its inner
        # view, because then the inner view would also need access to
        # the Augment's side views, which isn't guaranteed
        for vname in self.listViewNames():
            v = self.getView(vname)
            if isinstance(v,Augment):
                if not v.inner.storeMe:
                    logging.info('making %s stored, to make possible a downstream augment view' % v.inner.tag)
                    v.inner.storeMe = True

        #cache anything used more than twice
        numberOfTimesUsed = collections.defaultdict(int)
        for vname in self.listViewNames():
            v = self.getView(vname)
            for inner in v.innerViews() + v.nonInnerPrereqViews():
                numberOfTimesUsed[inner] += 1
        for (v,n) in numberOfTimesUsed.items():
            if n>1 and v.storeMe==None:
                logging.info('making %s stored because it might be used %d times' % (v.tag,n))
                v.storeMe = True

        #mark non-inner prereq views  as storeMe = 'distributedCache'
        for vname in self.listViewNames():
            v = self.getView(vname)
            for inner in v.nonInnerPrereqViews():
                inner.storeMe = 'distributedCache'

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
        argspec = ["list", "pprint=", "plan=", "metaplan=",
                   "store=", "cat=", 
                   "reuse", "alreadyStored=", 
                   "params=", "opts=", "do=", "view="]
        optlist,args = getopt.getopt(argv[1:], 'x', argspec)
        optdict = dict(optlist)
        
        # decide what views can be re-used, vs which need fresh plans
        if '--reuse' in optdict:  #reuse the views listed in the arguments
            for a in args:
                vname = View.viewNameFor(a)
                v = self.getView(vname)
                if v:
                    self.viewsPlannedToExist.add(v.tag)
                    logging.info("re-using data stored for view "+vname+": "+str(v))
                else:
                    logging.warn("cannot re-use view "+vname+" since it's not used in this script")

        #choose the main action to take
        if '--plan' in optdict:    #print a storage plan
            rel = self.getView(optdict['--plan'],mustExist=True)
            plan = rel.storagePlan()
            print "\n".join(plan.compile(self))
            return
        elif '--pprint' in optdict: #print a view
            rel = self.getView(optdict['--pprint'],mustExist=True)
            rel.pprint()
            return
        elif '--metaplan' in optdict:
            rel = self.getView(optdict['--metaplan'],mustExist=True)
            print rel.metaplan(self.viewsPlannedToExist)
            return
        elif '--store' in optdict:  #store a view
            rel = self.getView(optdict['--store'],mustExist=True)            
            plan = rel.storagePlan()
            plan.execute(self, echo=self.opts['echo'])
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
            self.alreadyStored = set(optdict.get('--alreadyStored','').split(','))
            #work out what view to use and what routine to call
            rel = self.getView(optdict['--view'],mustExist=True)
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
            print 'usage: --[store|pprint|plan|cat] view [--opts key:val,...] [--params key:val,...] --reuse view1 view2 ...]'
            print '       --[list]'
            print 'current legal keys for "opts", with default values:'
            for (key,val) in GPig.DEFAULT_OPTS.items():
                print '  %s:%s' % (key,str(val))
            print 'There\'s more help at http://curtis.ml.cmu.edu/w/courses/index.php/Guinea_Pig'

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

if __name__ == "__main__":
    print 'There\'s help at http://curtis.ml.cmu.edu/w/courses/index.php/Guinea_Pig'    
