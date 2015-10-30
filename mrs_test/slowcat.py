import sys
import time
import logging


if __name__ == "__main__":
    k = 0
    for line in sys.stdin:
        k += 1
        print line,
        if k % 1000 == 0: 
            logging.warn('sleep after ' + str(k) + ' lines...')
            time.sleep(1)

