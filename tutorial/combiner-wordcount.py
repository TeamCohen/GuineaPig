from guineapig import *
import sys

# supporting routines can go here
def tokens(line): 
    for tok in line.split(): 
        yield tok.lower()

#always subclass Planner
class WordCount(Planner):

    wc = ReadLines('corpus.txt') | Flatten(by=tokens) \
        | Group(retaining=lambda x:1, reducingTo=ReduceToSum(), combiningTo=ReduceToSum())

# always end like this
if __name__ == "__main__":
    WordCount().main(sys.argv)
