# cat command that also produced stderr output, for testing

import sys
import time
import logging
if __name__ == "__main__":
    for line in sys.stdin:
        print line,
        logging.warn(line.strip())
