import asyncio
from config import *
from typing import Tuple, List
from threading import Lock
from twitter_crawler.crawler import *
from utils.set_logger import setup_logger
from concurrent.futures import ThreadPoolExecutor
import multiprocessing as mp


logger = setup_logger(__name__)

numIter = NUM_ITER
iterInterval = ITER_INTERVAL
data_dir = DATA_DIR
topic = TOPIC
num_threads = THREADS
logger.info(f"Finish loading params from config file")

URLS = []
for t in topic:
    if t + '.json' not in os.listdir(data_dir):
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

# def twitter_crawler(url: Tuple[str]):
#     """
#     Crawl twitter post
#     -------------
#     Args:
#         url: tuple
#             Tuple of topic and url
#             to crawl
#     """
#     crawling_twitter_post(
#         data_dir, 
#         url, numIter, 
#         iterInterval
#     )

# def crawl_parallel(func: callable, url: List[Tuple]):
#     p = mp.Pool(len(url))
#     p.map(func, url)
    
    
# def main():
#     """
#     Main function to run the crawler
#     """
#     crawl_parallel(twitter_crawler, URLS)



if __name__ == "__main__":
    main()


