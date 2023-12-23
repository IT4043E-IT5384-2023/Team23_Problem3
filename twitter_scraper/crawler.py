import os
import sys 
sys.path.insert(
    0, os.path.join(
        os.path.dirname(
            os.path.abspath(__file__)
        ), 
    "..")
)

import json
import asyncio
import dotenv
import fire
from tqdm import tqdm

from config import *
from utils.file_manipulator import *
from utils.set_logger import setup_logger

from typing import Tuple, Dict, List

from twscrape import API, gather
from twscrape.logger import set_log_level

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

async def twitter_keyword_tweets_crawler(
    limit: int = None,
    topic: List[str] = None,
    account: Dict = None,
    save_dir: str = None,
) -> None:
    """
    Async function to crawl tweets for a given keyword
    :param limit: Number of tweets to crawl
    :param topic: Keyword to search
        Example: [
            'bitcoin', 'ethereum',
            'dogecoin', 'bnb',
            'binance', 'shiba',
        ]

    :param account: Account to use for crawling
        Example: {
            "username": "<username>",
            "password": "<password>",
            "email": "<email>",
            "email_password": "<email_password>"
        }

    :param save_dir: Directory to save the crawled tweets

    """
    # Check if save_dir exists
    if not os.path.exists(save_dir):
        logger.info(f"Creating directory {save_dir}")
        os.makedirs(save_dir)

    api = API()
    set_log_level("WARNING")
    await api.pool.add_account(**account)
    await api.pool.login_all()

    for keyword in tqdm(topic, total=len(topic), desc="Fetching tweets"):
        logger.info(f"Fetching tweets for {keyword}")
        filter = "since:2023-01-01 lang:en min_replies:5 min_faves:5 min_retweets:5"
        query = f"(#{keyword} OR ${keyword}) {filter}"
        tweets = []
        tweet_count = 0

        async for tweet in api.search(query, limit=limit):
            tweets.append(tweet.dict())
            tweet_count += 1

            if tweet_count % 50 == 0 and len(tweets) > 0:
                logger.info(f"Collected {tweet_count} tweets for {keyword}")
                save_to_file(tweets, f"{save_dir}/{keyword}.json")
                tweets = []

            # asyncio.sleep(30)

        if len(tweets) > 0:
            save_to_file(tweets, f"{save_dir}/{keyword}_tweets.json")
        logger.info(f"Collected {tweet_count} tweets for {keyword}")


async def twitter_user_info_crawler(
    limit: int = None,
    user_id: List[str] = None,
    account: Dict = None,
    save_dir: str = None,
) -> None:
    """
    Async function to crawl user info for a given user id.
    :param limit: Number of tweets to crawl
    :param user_id: User id to search
    :param account: Account to use for crawling
        Example: {
            "username": "<username>",
            "password": "<password>",
            "email": "<email>",
            "email_password": "<email_password>"
        }
    :param save_dir: Directory to save the crawled tweets
    """
    # Check if save_dir exists
    if not os.path.exists(save_dir):
        logger.info(f"Creating directory {save_dir}")
        os.makedirs(save_dir)

    api = API()
    # set_log_level("WARNING")
    await api.pool.add_account(**account)
    await api.pool.login_all()

    for uid in tqdm(user_id, total=len(user_id), desc="Fetching user info"):
        user_dir = os.path.join(save_dir, str(uid))
        if not os.path.exists(user_dir):
            os.makedirs(user_dir)

        logger.info(f"Fetching user info for {uid}")
        # user_info = await api.user_by_id(uid)
        # save_to_file(user_info.json(), f"{user_dir}/'user.json")
        # logger.info(f"Collected user info for {uid}")

        list_follower = await gather(api.followers(user_id, limit=limit))  # list[User]
        save_to_file(list_follower.json(), f"{user_dir}/followers.json")
        logger.info(f"Collected followers for {uid}")

        list_following = await gather(api.following(user_id, limit=limit))  # list[User]
        save_to_file(list_following.json(), f"{user_dir}/following.json")
        logger.info(f"Collected following for {uid}")

        user_tweets = await gather(api.user_tweets(user_id, limit=limit))  # list[Tweet]
        save_to_file(user_tweets.json(), f"{user_dir}/user_tweets.json")
        logger.info(f"Collected user tweets for {uid}")

        # user_tweets_and_replies = await gather(
        #     api.user_tweets_and_replies(user_id, limit=limit)
        # )  # list[Tweet]
        # save_to_file(
        #     user_tweets_and_replies.json(), f"{user_dir}/user_tweets_and_replies.json"
        # )
# -------------------------------------------------------------

async def run_async_tweets_crawler(
    limit=50000, 
    account_arr: List[Dict] = account, 
    save_dir=OUR_RAW_TWEETS_DIR
):
    """
    Function to run async tweets crawler function.
    Each task will be crawled by a different Twitter account.
    ---------------
    Args:
        :param limit: number of tweets to crawl
        :param account_arr: a ductionary contains .env variable 
                            of each account username and password
        :param save_dir: Directory to save raw data
    Returns:
        None
    """
    TOPIC = get_keywords_from_json(CRYPTO_KEYWORDS_JSON)
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
    tweet_dir=OUR_RAW_TWEETS_DIR,
    save_dir=OUR_RAW_USERS_DIR
):
    """
    Function to run async tweets crawler function.
    Each task will be crawled by a different Twitter account.
    ---------------
    Args:
        :param limit: number of tweets to crawl
        :param account_arr: a ductionary contains .env variable 
                            of each account username and password
        :param tweet_dir: Directory of raw tweets data
        :param save_dir: Directory to save raw user data
    Returns:
        None
    """
    for tweet_file in os.listdir(tweet_dir):
        tweet_folder = os.path.join(
            save_dir, tweet_file.replace(".json", "")
        )
        if not os.path.exists(tweet_folder):
            os.makedirs(tweet_folder)

        user_id = get_user_id_from_json(
            os.path.join(tweet_dir, tweet_file)
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
    fire.Fire(run_async_tweets_crawler)
    fire.Fire(run_async_users_crawler)


if __name__ == "__main__":
    main()



