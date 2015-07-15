from guineapig import *
import sys
import math
import logging

def tokens(line): 
    for tok in line.split(): yield tok.lower()

class WordProb(Planner):

    wc = ReadLines('corpus.txt') | Flatten(by=tokens) | Group(by=lambda x:x, reducingTo=ReduceToCount())
    total = Group(wc, by=lambda x:'ANY', retaining=lambda (word,count):count, reducingTo=ReduceToSum()) | ReplaceEach(by=lambda (word,count):count)
    wcWithTotal = Augment(wc, sideview=total,loadedBy=lambda v:GPig.onlyRowOf(v))
    prob = ReplaceEach(wcWithTotal, by=lambda ((word,count),n): (word,count,n,float(count)/n))

# always end like this
if __name__ == "__main__":
    WordProb().main(sys.argv)
