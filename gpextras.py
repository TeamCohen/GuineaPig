# extensions to guineapig

##############################################################################
# (C) Copyright 2014, 2015 William W. Cohen.  All rights reserved.
##############################################################################

import sys
import os

from guineapig import *

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

class ReadBlocks(Reader):
    """ Returns blocks of non-empty lines, separated by empty lines"""

    def __init__(self,src,isEndBlock=lambda line:line=="\n"):
        Reader.__init__(self,src)
        self.isEndBlock = isEndBlock

    def rowGenerator(self):
        buf = []
        for line in sys.stdin:
            if self.isEndBlock(line):
                yield buf
                buf = []
            else:
                buf.append(line)
        if buf:
            yield buf

    def __str__(self):
        return 'ReadBlocks("%s")' % self.src + self.showExtras()

class Log(ReplaceEach):
    """Print logging messages to stderr as data is processed. 
    For every row, the logfun will be called with arguments
    logfun(rowValue,rowIndex).
    """

    def __init__(self, inner=None, logfun=lambda rowV,rowI:None):
        self.rowNum = 0
        def logfunCaller(rowValue):
            self.rowValue += 1
            self.logfun(rowValue,self.rowNum)
            return rowValue
        ReplaceEach.__init__(self,inner,by=logfunCaller)
        
    def __str__(self):
        return 'Log("%s")' % self.src + self.showExtras()

class LogEchoFirst(Log):

    """Echo the first N things."""
    
    def __init__(self, inner=None, first=10):
        def logfirst(rowValue,rowIndex):
            if rowIndex<=first:
                print >> sys.stderr, 'row %d: "%s"' % (rowIndex,rowValue)
        Log.__init__(self, inner=inner, logfun=logfirst)

class LogProgress(Log):

    """Echo a status message every 'interval' rows."""
    
    def __init__(self, inner=None, msg="Logging progress", interval=1000):
        def logprogress(rowValue,rowIndex):
            if (rowIndex % interval)==0:
                print >> sys.stderr, "%s: %d rows done" % (msg,rowIndex)
        Log.__init__(self, inner=inner, logfun=logprogress)


def MapsideJoin(jinForLargeView, jinForSmallView):
    """ A map-side join implementation using Augment
    """
    def smallViewLoader(view):
        smallViewDict = {}
        for line in open(view.distributableFile()):
            row = view.planner._serializer.fromString(line.strip())            
            key = (jinForSmallView.joinBy)(row)
            smallViewDict[key] = row
        return smallViewDict
    def joiner((rowFromLargeView,smallViewDict)):
        key = (jinForLargeView.joinBy)(rowFromLargeView)
        if key in smallViewDict:
            return [(rowFromLargeView,smallViewDict[key])]
        else:
            return []
      
    return Augment( jinForLargeView.view, sideview=jinForSmallView.view, loadedBy=smallViewLoader) \
        | Flatten( by=joiner )


##############################################################################
# extension to use mrs_gp, a local map-reduce for streaming intended
# mainly for use on ramdisks
##############################################################################

class MRSCompiler(MRCompiler):
    """Compile tasks to commands that are executable on most Unix shells,
    with the mrs_gp.py program.  To use this compiler you need to
    call planner.registerCompiler('mrs_go',gpextras.MRSCompiler)
    """

    def __init__(self):
        self.mrsCommand = os.environ.get('GP_MRS_COMMAND','python -m mrs_gp')

    def distributeCommands(self,task,gp,maybeRemoteCopy,localCopy):
        """Distribute the remote copy to the local directory."""
        return ['%s --fs getmerge %s > %s|| echo warning: the copy failed!' % (self.mrsCommand,maybeRemoteCopy,localCopy)]

    def simpleMapCommands(self,task,gp,mapCom,src,dst):
        """A map-only job with zero or one inputs."""
        assert src,'undefined src for this view? you may be using Wrap with target:mrs'
        # parallelism is ignored for map-only tasks
        return [ "%s --input %s --output %s --mapper '%s'" % (self.mrsCommand,src,dst,mapCom) ]
        
    def simpleMapReduceCommands(self,task,gp,mapCom,reduceCom,src,dst):
        """A map-reduce job with one input."""
        p = task.reduceParallel(gp)
        return [ "%s --input %s --output %s --numReduceTasks %d --mapper '%s'  --reducer '%s'" \
                 % (self.mrsCommand,src,dst,p,mapCom,reduceCom) ]

    def joinCommands(self,task,gp,mapComs,reduceCom,srcs,midpoint,dst):
        """A map-reduce job with several inputs."""
        p = task.reduceParallel(gp)
        def mid(i): return midpoint + '-' + str(i)
        subplan = []
        for i in range(len(srcs)):
            subplan.append("%s --input %s --output %s --mapper '%s'" \
                           % (self.mrsCommand,srcs[i],mid(i),mapComs[i]))
        allMidpoints = ",".join([mid(i) for i in range(len(srcs))])
        subplan.append("%s --inputs %s --output %s --numReduceTasks %d --mapper cat --reducer '%s'" \
                       % (self.mrsCommand,allMidpoints,dst,p,reduceCom))
        return subplan
