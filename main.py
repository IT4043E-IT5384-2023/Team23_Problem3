import asyncio
from config import *
from typing import Tuple, List
from twitter_crawler.crawler import *
import multiprocessing as mp
import dotenv
import fire
from config import *
from utils.set_logger import setup_logger

logger = setup_logger(__name__)
dotenv.load_dotenv()

username = os.getenv("USER_NAME").split(",")
password = os.getenv("PASSWORD").split(',')
email = os.getenv("EMAIL").split(',')
email_pass = os.getenv("EMAIL_PASSWORD").split(',')
logger.info(f"Number of accounts: {len(username)}")

account = []
for i in range(len(username)):
    account.append({
        "username": username[i].strip(),
        "password": password[i].strip(),
        "email": email[i].strip(),
        "email_password": email_pass[i].strip()
    })

topic_chunk = []
topic_chunk_size = len(TOPIC)//len(account) + 1
for i in range(0, len(TOPIC), topic_chunk_size):
    topic_chunk.append(
        TOPIC[i:i + topic_chunk_size]
    )

# -------------------------------------------------------------

async def run_async_crawler(
        limit=50000, 
        topic_arr: List[List] = topic_chunk,
        account_arr: List[Dict] = account, 
        save_dir=DATA_DIR
    ):
    task_arr = []
    for i, acc in enumerate(account_arr):
        task = asyncio.create_task(
            keyword_tweets_crawler(limit, topic_arr[i], acc, save_dir)
        )
        task_arr.append(task)

    await asyncio.gather(*task_arr)


def main():
    fire.Fire(run_async_crawler)


if __name__ == "__main__":
    main()


