import time
import os
import pandas as pd
import dotenv
from typing import List, Union
from utils.set_logger import setup_logger
from utils.utils import *
from config import *

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.service import Service as ChromeService # Similar thing for firefox also!
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException

logger = setup_logger(__name__)

dotenv.load_dotenv()
Username = os.getenv("TWITTER_USER_NAME")
logger.info(f"Finish loading user name from .env file")

Password = os.getenv("TWITTER_USER_PASSWORD")
logger.info(f"Finish loading user password from .env file")

#-------------------------------------------------------------------------------------

chrome_options = Options()
chrome_options.add_argument('--ignore-certificate-errors')
chrome_options.add_argument('--disable-gpu')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument("--headless=new")
chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])

# -------------------------------------------------------------------------------------

def crawling_twitter_account(
    data_dir: str = None,
    topic: Union[str, List[str]] = None,
    numIter: int = None, 
    iterInterval: int = None 
) -> pd.DataFrame:
    """
    Crawl twitter account
    -------------
    Args:
        data_dir: str
            Data directory to save crawled data
        topic: str or List[str]
            Topic to crawl
        numIter: int
            Number of scroll down refreshing
        iterInterval: int
            Interval between 2 Iteration
    Return:
        df: pd.DataFrame
            Dataframe of crawled data

    """
    driver = webdriver.Chrome(options=chrome_options)
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

    # -----------------------------------------------------------------
    # Login
    driver.get("https://twitter.com/login")
    time.sleep(5)
    username = driver.find_element(By.XPATH,"//input[@name='text']")
    username.send_keys(Username)
    next_button = driver.find_element(By.XPATH,"//span[contains(text(),'Next')]")
    next_button.click()

    time.sleep(5)
    password = driver.find_element(By.XPATH,"//input[@name='password']")
    password.send_keys(Password)
    log_in = driver.find_element(By.XPATH,"//span[contains(text(),'Log in')]")
    log_in.click()
    time.sleep(5)

    # -----------------------------------------------------------------
    # Crawl
    if isinstance(topic, str):
        topic = [topic]
    
    for t in topic:
        try:
            logger.info(f"Start crawling twitter account for topic {t}")

            URL = f"https://twitter.com/search?q=%23{t}&src=typed_query"
            driver.get(URL)

            df = {
                'UserTag': [], 'Time': [], 
                'Tweet': [], 'Reply': [], 
                'reTweet': [], 'Like': [], 
                'View': []
            }

            time.sleep(5)
            for i in range(numIter):
                try:    
                    articles = driver.find_elements(
                        By.XPATH,"//article[@data-testid='tweet']"
                    )
                    
                except:
                    logger.info(
                        "Iter {}/{}: Lagging catched, waiting.".format(i + 1, numIter)
                    )
                    time.sleep(20)
                    break

                for article in articles:
                    wait = WebDriverWait(article, 5)
                    try:
                        UserTag = wait.until(EC.presence_of_element_located
                            (
                                (By.XPATH, ".//div[@data-testid='User-Name']")
                            )
                        ).text
                        UserTag = format_string(UserTag)
                        df['UserTag'].append(UserTag)

                    except Exception as e:
                        logger.error(e)
                        pass    
                
                    try:
                        Time = wait.until(EC.presence_of_element_located
                            (
                                (By.XPATH, ".//time")
                            )
                        ).get_attribute('datetime')
                        df['Time'].append(Time)

                    except Exception as e:
                        logger.error(e)
                        pass
                
                    try:
                        Tweet = wait.until(EC.presence_of_element_located
                            (
                                (By.XPATH,".//div[@data-testid='tweetText']")
                            )
                        ).text
                        Tweet = format_string(Tweet)
                        df['Tweet'].append(Tweet)   

                    except Exception as e:
                        logger.error(e)
                        pass

                    try:
                        Reply = wait.until(EC.presence_of_element_located
                            (
                                (By.XPATH,".//div[@data-testid='reply']")
                            )
                        ).text
                        df['Reply'].append(Reply)

                    except Exception as e:
                        logger.error(e)
                        pass
                
                    try:
                        reTweet = wait.until(EC.presence_of_element_located
                            (
                                (By.XPATH,".//div[@data-testid='retweet']")
                            )
                        ).text
                        df['reTweet'].append(reTweet)

                    except Exception as e:
                        logger.error(e)
                        pass
                
                    try:
                        Like = wait.until(EC.presence_of_element_located
                            (
                                (By.XPATH,".//div[@data-testid='like']")
                            )
                        ).text
                        df['Like'].append(Like)

                    except Exception as e:
                        logger.error(e)
                        pass

                    try:
                        View = wait.until(EC.presence_of_element_located
                            (
                                (By.XPATH, ".//a[@role='link']/div/div[2]/span/span/span")
                            )
                        ).text
                        df['View'].append(View)
                    except Exception as e:
                        logger.error(e)
                        pass

                logger.info("Iter {}/{}: Retrieving succeed.".format(i + 1, numIter))
                driver.execute_script('window.scrollBy(0,3200);')
                time.sleep(iterInterval)

            output = pd.DataFrame.from_dict(df, orient='index')
            output_ = output.T
            # df = pd.DataFrame(df)
            output_.to_csv(f"{data_dir}/{t}.csv", index=False)
            logger.info(f"Finish crawling twitter account for topic {t}")

        except Exception as e:
            logger.error(e)
            continue

    driver.quit()


if __name__ == "__main__":

    df = crawling_twitter_account(
        numIter = numIter,
        iterInterval = iterInterval
    )
    df = pd.DataFrame(df)
    df.to_csv("./data/twitter_account.csv", index=False)
    logger.info("Finish crawling twitter account")