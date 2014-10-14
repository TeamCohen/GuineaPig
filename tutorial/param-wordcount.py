from guineapig1_3 import *
import sys
import os

# supporting routines can go here
def tokens(line): 
    for tok in line.split(): 
        yield tok.lower()

#always subclass Planner
class WordCount(Planner):

    D = GPig.getArgvParams()
    wc = ReadLines(D['corpus']) | Flatten(by=tokens) | Group(by=lambda x:x, reducingTo=ReduceToCount())

# always end like this
if __name__ == "__main__":
    WordCount().main(sys.argv)
