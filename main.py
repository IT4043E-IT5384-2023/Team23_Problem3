from twitter_crawler.crawler import *
from utils.set_logger import setup_logger
from config import *
from time import time
from threading import Lock
from concurrent.futures import ThreadPoolExecutor
from typing import Tuple
import asyncio

logger = setup_logger(__name__)

numIter = NUM_ITER
logger.info(f"Finish loading number of iteration from config file")

iterInterval = ITER_INTERVAL
logger.info(f"Finish loading iteration interval from config file")

data_dir = DATA_DIR
logger.info(f"Finish loading data directory from config file")

topic = TOPIC
logger.info(f"Finish loading topics from config file")

num_threads = THREADS
logger.info(f"Finish loading number of threads from config file")

URLS = []
for t in topic:
    url = f"https://twitter.com/search?q=%23{t}&src=typed_query"
    URLS.append((t, url))
#-------------------------------------------------------------------------------------

async def twitter_crawler(url: Tuple[str]):
    """
    Crawl twitter account
    -------------
    Args:
        url: tuple
            Tuple of topic and url
            to crawl
    Return:
        None
    """
    loop = asyncio.get_event_loop()
    lock = Lock()
    await loop.run_in_executor(
        None, crawling_twitter_account, 
        data_dir, url, numIter, 
        iterInterval, lock
    )

async def task(url: Tuple[str]):
    """
    Function to create asyncio task
    -------------
    Args:
        url: tuple
            Tuple of topic and url
            to crawl
    Return:
        None
    """
    try:
        logger.info("task is created")
        await asyncio.sleep(1) # sleeps for 1s
        title = await twitter_crawler(url)
        logger.info(title)
    except Exception as e:
        logger.info(e)


def main():
    """
    Main function to run the crawler
    -------------
    """
    executor = ThreadPoolExecutor(num_threads)
    loop = asyncio.get_event_loop()
    loop.set_default_executor(executor)
    pool_tasks = []
    for url in URLS:
        t = loop.create_task(task(url))
        pool_tasks.append(t)
    loop.run_forever()


if __name__ == "__main__":
    main()


