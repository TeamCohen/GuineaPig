from guineapig1_3 import *
import sys
import logging

# supporting routines can go here
def tokens(line): 
    for tok in line.split(): 
        yield tok.lower()

k = int(GPig.getArgvParams().get('prefixLen','1'))

#always subclass Planner
class PrefCount(Planner):

    wc = ReadLines('corpus.txt') | Flatten(by=tokens) | Group(by=lambda x:x, reducingTo=ReduceToCount())
    pc1 = Group(wc, by=lambda (word,count):word[:k], reducingTo=ReduceTo(int, lambda accum,(word,count): accum+count))
    pc2 = Group(wc, by=lambda (word,count):word[:k], retaining=lambda (word,count):count, reducingTo=ReduceTo(int, lambda accum,val:accum+val))
    pc3 = Group(wc, by=lambda (word,count):word[:k], retaining=lambda (word,count):count, reducingTo=ReduceToSum())

# always end like this
if __name__ == "__main__":
    PrefCount().main(sys.argv)
