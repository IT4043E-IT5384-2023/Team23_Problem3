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
from tqdm import tqdm

from config import *
from data_crawling.utils.file_manipulator import *
from data_crawling.utils.set_logger import setup_logger

from typing import Dict, List

from twscrape import API, gather
from twscrape.logger import set_log_level

logger = setup_logger(__name__)


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


if __name__ == "__main__":
    # asyncio.run(twitter_keyword_tweets_crawler(1000, ['bitcoin'], account[0], DATA_DIR))
    pass
