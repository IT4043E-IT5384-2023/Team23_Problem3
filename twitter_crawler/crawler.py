import time
import os
import pandas as pd
import dotenv
from typing import Dict, Tuple
from utils.set_logger import setup_logger
from utils.utils import *
from config import *
from threading import Lock

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

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

def crawling_twitter_post(
    data_dir: str = None,
    topic_url: Tuple[str] = None,
    numIter: int = None, 
    iterInterval: int = None,
    lock: Lock = None
) -> pd.DataFrame:
    """
    This function is used to crawl twitter post
    The functionaliy is to scroll down the page and get all the posts information
    -------------
    Args:
        data_dir: str
            Data directory to save crawled data
        topic: Tuple[str]
            Tuple of topic (or person) and topic_url to crawl
        numIter: int
            Number of scroll down refreshing
        iterInterval: int
            Interval between 2 Iteration
    Return:
        df: pd.DataFrame
            Dataframe of crawled data

    """
    with lock:
        driver = webdriver.Chrome(options=chrome_options)
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)

        # ------------------------------ Login -----------------------------------
        while True:
            try:
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
                logger.info("Login successfully.")
                break
            
            except Exception as e:
                logger.error(e)
                driver.quit()
                time.sleep(5)

        # ------------------------------- Crawl ----------------------------------
        topic, URL = topic_url
        logger.info(f"Start crawling twitter account for {topic}")
        driver.get(URL)
        df = {
            'UserTag': [], 'Time': [], 
            'Tweet': [], 'Reply': [], 
            'reTweet': [], 'Like': [], 
            'View': []
        }

        time.sleep(5)
        for i in range(numIter):
            initial_scroll_position = driver.execute_script(
                "return window.scrollY;"
            )
            try:    
                articles = driver.find_elements(
                    By.XPATH,"//article[@data-testid='tweet']"
                )
                
            except:
                time.sleep(20)
                break

            xpath_dict = {
                'UserTag': ".//div[@data-testid='User-Name']",
                'Time': ".//time",
                'Tweet': ".//div[@data-testid='tweetText']",
                'Reply': ".//div[@data-testid='reply']",
                'reTweet': ".//div[@data-testid='retweet']",
                'Like': ".//div[@data-testid='like']",
                'View': ".//a[@role='link']/div/div[2]/span/span/span",
            }
            check_duplicate = []
            for article in articles:
                wait = WebDriverWait(article, 5)
                article_data = {}
                while True:
                    try:
                        for key, xpath in xpath_dict.items():
                            information = wait.until(
                                EC.presence_of_element_located(
                                    (By.XPATH, xpath)
                                )
                            )
                            if key == 'Time':
                                article_data[key] = information.get_attribute('datetime')

                            if key == 'UserTag' or key == 'Tweet':
                                text = information.text
                                article_data[key] = format_string(text)

                            article_data[key] = information.text

                        if article_data not in check_duplicate:
                            check_duplicate.append(article_data)

                            for key, value in article_data.items():
                                df[key].append(value)

                        break

                    except Exception as e:
                        logger.error(e)
                        time.sleep(5)
                
                time.sleep(2)

            driver.execute_script('window.scrollBy(0,3200);')
            time.sleep(iterInterval)
            current_scroll_position = driver.execute_script(
                "return window.scrollY;"
            )

            if current_scroll_position == initial_scroll_position:
                logger.info(
                    f"Finish crawling twitter account for topic {topic}"
                )
                break

        output = pd.DataFrame.from_dict(df, orient='index')
        output_ = output.T
        output_.to_csv(f"{data_dir}/{topic}.csv", index=False)

        driver.quit()



if __name__ == "__main__":

    df = crawling_twitter_account(
        numIter = numIter,
        iterInterval = iterInterval
    )
    df = pd.DataFrame(df)
    df.to_csv("./data/twitter_account.csv", index=False)
    logger.info("Finish crawling twitter account")