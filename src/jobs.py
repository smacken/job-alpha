import logging

from job_alpha.scheduler import ScrapeScheduler
from job_alpha import language

logger = logging.getLogger('jobs')
fh = logging.FileHandler('job-alpha.log')
fh.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
formatter = logging.Formatter(
    '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
fh.setFormatter(formatter)
logger.addHandler(handler)
logger.addHandler(fh)
logger.setLevel(logging.DEBUG)

def main():
    try:
        while True:
            scheduler = ScrapeScheduler()
    except KeyboardInterrupt:
        logging.config.stopListening()

    del scheduler

if __name__ == "__main__":
    main()
