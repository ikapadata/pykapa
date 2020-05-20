import logging
import sys
# create logger
logger = logging.getLogger('Pykapa')
logging.getLogger("BILLING").propagate = False
logger.setLevel(logging.DEBUG)

ch = logging.StreamHandler(sys.stdout)
# create console handler and set level to debug
# ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

# create formatter
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# add formatter to ch
ch.setFormatter(formatter)

# add ch to logger
logger.addHandler(ch)
