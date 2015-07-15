from guineapig import *
import sys

def wordCountScript():
    def tokens(line): 
        for tok in line.split(): 
            yield tok.lower()
    p = Planner()
    p.lines = ReadLines('corpus.txt')
    p.words = Flatten(p.lines,by=tokens)
    p.wordCount = Group(p.words, by=lambda x:x, reducingTo=ReduceToCount())
    p.setup() #always end with this
    return p

# always end like this
if __name__ == "__main__":
    wordCountScript().main(sys.argv)
