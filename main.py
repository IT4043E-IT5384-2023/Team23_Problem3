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
iterInterval = ITER_INTERVAL
data_dir = DATA_DIR
topic = TOPIC
num_threads = THREADS
logger.info(f"Finish loading params from config file")

URLS = []
for t in topic:
    url = f"https://twitter.com/search?q=%23{t}&src=typed_query"
    URLS.append((t, url))
#-------------------------------------------------------------------------------------

async def twitter_crawler(url: Tuple[str]):
    """
    Crawl twitter post
    -------------
    Args:
        url: tuple
            Tuple of topic and url
            to crawl
    """
    loop = asyncio.get_event_loop()
    lock = Lock()
    await loop.run_in_executor(
        None, crawling_twitter_post, 
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


