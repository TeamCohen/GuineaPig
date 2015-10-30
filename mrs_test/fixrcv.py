import sys
import time

# fix the rcv training files in /afs/.../bigML to be format expected by streamNaiveBayesLearner.py

if __name__ == "__main__":
    k = 1
    for line in sys.stdin:
        classes,words = line.strip().split("\t")
        docid = 'rcv%06d.txt' % k
        print "\t".join([docid,classes] + words.lower().split())
        k += 1
