import asyncio

from config import *
from typing import Tuple, List
from crawler import *

import dotenv
import fire

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

# -------------------------------------------------------------

async def run_async_tweets_crawler(
    limit=50000, 
    account_arr: List[Dict] = account, 
    save_dir=TWEETS_DIR
):
    TOPIC = get_keywords_from_json(
        r'E:\Code\BigData_prj\data\crypto_keywords.json'
    )
    topic_arr = []
    topic_chunk_size = len(TOPIC)//len(account) + 1

    for i in range(0, len(TOPIC), topic_chunk_size):
        topic_arr.append(
            TOPIC[i:i + topic_chunk_size]
        )

    task_arr = []
    for i, acc in enumerate(account_arr):
        task = asyncio.create_task(
            twitter_keyword_tweets_crawler(
                limit, topic_arr[i], acc, save_dir
            )
        )
        task_arr.append(task)

    await asyncio.gather(*task_arr)

# -------------------------------------------------------------


async def run_async_users_crawler(
    limit=50000, 
    account_arr: List[Dict] = account, 
    save_dir=USERS_DIR
):
    for tweet_file in os.listdir(TWEETS_DIR):
        tweet_folder = os.path.join(
            USERS_DIR, tweet_file.replace(".json", "")
        )
        if not os.path.exists(tweet_folder):
            os.makedirs(tweet_folder)

        user_id = get_user_id_from_json(
            os.path.join(TWEETS_DIR, tweet_file)
        )
        user_id_arr = []
        user_chunk_size = len(user_id)//len(account) + 1
        for i in range(0, len(user_id), user_chunk_size):
            user_id_arr.append(
                user_id[i:i + user_chunk_size]
            )
        task_arr = []
        for i, acc in enumerate(account_arr):
            task = asyncio.create_task(
                twitter_user_info_crawler(
                    limit, user_id_arr[i], acc, tweet_folder
                )
            )
            task_arr.append(task)

        await asyncio.gather(*task_arr)


def main():
    # fire.Fire(run_async_tweets_crawler)
    fire.Fire(run_async_users_crawler)


if __name__ == "__main__":
    main()


