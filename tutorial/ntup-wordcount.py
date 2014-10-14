from guineapig1_3 import *
import sys
import math
import collections

def tokens(line): 
    for tok in line.split(): yield tok.lower()

WordStat = collections.namedtuple('WordStat','word count')

class WordCount(Planner):

    wcTuple = ReadLines('corpus.txt') | Flatten(by=tokens) | Group(by=lambda x:x, reducingTo=ReduceToCount())
    wcNamedTuple = ReplaceEach(wcTuple, by=lambda (w,n):WordStat(word=w,count=n)).opts(stored=True)
    commonWords = Filter(wcNamedTuple, by=lambda ws:ws.count>=200) | Group(by=WordStat.count.fget)

# always end like this
if __name__ == "__main__":
    wc = WordCount().setEvaluator(GPig.SafeEvaluator({'WordStat':WordStat}))
    wc.main(sys.argv)
