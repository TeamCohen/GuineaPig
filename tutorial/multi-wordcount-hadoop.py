from guineapig import *
import sys
import os
import subprocess

#example of programmatic use of Guinea Pig

def wc(corpus=None,prefix=None):
    """Return a planner instance that can word-count a corpus."""
    p = Planner(corpus=corpus,prefix=prefix)
    #self.param is automatically constructed
    p.r = ReadLines(p.param['corpus'])  
    def tokens(line): 
        for tok in line.split(): 
            yield tok.lower()
    p.s = Flatten(p.r, by=tokens) 
    if p.param['prefix']: 
        p.f = Filter(p.s, by=lambda w:w.startswith(p.param['prefix']))
    else: 
        p.f = p.s
    p.t = Group(p.f, reducingTo=ReduceToCount())
    p.setup()  #must call AFTER you finish adding views
    return p

if __name__ == "__main__":
    #run subplan step, if necessary
    if Planner.partOfPlan(sys.argv):
        wc().main(sys.argv)  
    else:
        #run any main you want - this one's usage is python mult-wordcount.py prefix corpus1 corpus2 ....
        prefix = sys.argv[1]
        for a in sys.argv[2:]:
            #create the planner instance
            planner = wc(corpus=a,prefix=prefix)
            planner.config['target'] = 'hadoop'
            #decide where to store the wordcount data
            v = planner.getView('t')
            targetFile = 'wc-for-'+os.path.basename(a)
            print 'storing this view in',targetFile
            v.pprint()                         #print the view
            v.storagePlan().execute(planner)   #store it
            #move the stored file to its final location
            subprocess.call(['hadoop','fs','-getmerge',v.storedFile(),targetFile])

