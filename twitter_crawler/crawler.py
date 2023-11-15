import os
from config import *
from utils.utils import *
from typing import Dict, List
from utils.set_logger import setup_logger
import asyncio
from tqdm import tqdm
import json
import os
from twscrape import API
from twscrape.logger import set_log_level
from config import *

logger = setup_logger(__name__)

async def keyword_tweets_crawler(
        limit: int, 
        topic: List,  
        account: Dict,  
        save_dir: str
    ):
    # Check if save_dir exists
    if not os.path.exists(save_dir):
        logger.info(f"Creating directory {save_dir}")
        os.makedirs(save_dir)

    api = API()
    set_log_level("DEBUG")

    await api.pool.add_account(**account)
    await api.pool.login_all()

    for keyword in tqdm(topic, total=len(topic), desc="Fetching tweets"):
        logger.info(f"Fetching tweets for {keyword}")
        filter = "since:2023-01-01 lang:en min_replies:5 min_faves:5 min_retweets:5"
        q = f"(#{keyword} OR ${keyword}) {filter}"
        tweets = []
        tweet_count = 0

        async for tweet in api.search(q, limit=limit):
            tweets.append(tweet.dict())
            tweet_count += 1

            if tweet_count % 1000 == 0:
                logger.info(f"Collected {tweet_count} tweets for {keyword}")
                save_to_file(tweets, f"{save_dir}/twitter/{keyword}_tweets.json")
                tweets = []

        save_to_file(tweets, f"{save_dir}/twitter/{keyword}_tweets.json")
        logger.info(f"Collected {tweet_count} tweets for {keyword}")



if __name__ == "__main__":
    asyncio.run(keyword_tweets_crawler(1000, ['bitcoin'], account[0], DATA_DIR))
