from twitter_crawler.crawler import *
from utils.set_logger import setup_logger
from config import *

logger = setup_logger(__name__)

if __name__ == "__main__":

    df = crawling_twitter_account(
        numIter = numIter,
        iterInterval = iterInterval
    )
    df = pd.DataFrame(df)
    df.to_csv("./data/twitter_account.csv", index=False)
    logger.info("Finish crawling twitter account")
