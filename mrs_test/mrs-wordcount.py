from guineapig import *
import sys
import gpextras

# supporting routines can go here
def tokens(line): 
    for tok in line.split(): 
        yield tok.lower()

#always subclass Planner
class WordCount(Planner):

    wc = ReadLines('corpus.txt') | Flatten(by=tokens) | Group(by=lambda x:x, reducingTo=ReduceToCount())

# always end like this
if __name__ == "__main__":
    p = WordCount()
    p.registerCompiler('mrs',gpextras.MRSCompiler)
    p.main(sys.argv)

