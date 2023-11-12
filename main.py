from twitter_crawler.crawler import *
from utils.set_logger import setup_logger
from config import *

logger = setup_logger(__name__)

#Number of scroll down refreshing
numIter = NUM_ITER
logger.info(f"Finish loading number of iteration from .env file")
#Interval between 2 Iteration
iterInterval = ITER_INTERVAL
logger.info(f"Finish loading iteration interval from .env file")

data_dir = DATA_DIR
logger.info(f"Finish loading data directory from .env file")

topic = TOPIC
logger.info(f"Finish loading topics from .env file")


if __name__ == "__main__":
    
    crawling_twitter_account(
        data_dir = data_dir,
        topic = topic,
        numIter = numIter,
        iterInterval = iterInterval
    )

