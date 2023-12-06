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

async def twitter_keyword_tweets_crawler(
        limit: int = None, 
        topic: List = None,  
        account: Dict = None,  
        save_dir: str = None
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



if __name__ == "__main__":
    asyncio.run(twitter_keyword_tweets_crawler(1000, ['bitcoin'], account[0], DATA_DIR))