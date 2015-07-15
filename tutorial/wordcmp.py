from guineapig import *
import sys
import math

def tokens(line): 
    for tok in line.split(): yield tok.lower()

def score(n1,n2): 
    return math.log((n1 + 1.0)/(n1 + n2 + 2.0))

#always subclass Planner
class WordCmp(Planner):

    def wcPipe(fileName): 
        return ReadLines(fileName) | Flatten(by=tokens) | Group(by=lambda x:x, reducingTo=ReduceToCount())

    wc1 = wcPipe('bluecorpus.txt')
    wc2 = wcPipe('redcorpus.txt')

    cmp = Join( Jin(wc1, by=lambda(word,n):word), Jin(wc2, by=lambda(word,n):word) ) \
          | ReplaceEach(by=lambda((word1,n1),(word2,n2)):(word1, score(n1,n2)))

    result = Format(cmp, by=lambda(word,blueScore):'%6.4f %s' % (blueScore,word))

# always end like this
if __name__ == "__main__":
    WordCmp().main(sys.argv)
