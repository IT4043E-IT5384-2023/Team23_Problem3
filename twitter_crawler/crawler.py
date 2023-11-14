import time
import os
import dotenv
import random
import pandas as pd
from config import *
from utils.utils import *
from threading import Lock
from typing import Dict, Tuple
from utils.set_logger import setup_logger

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

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
        username_list = Username.split(',')
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)

        # ------------------------------ Login -----------------------------------
        while True:
            try:
                driver.get("https://twitter.com/login")
                time.sleep(5)
                username = driver.find_element(By.XPATH,"//input[@name='text']")
                user_name = random.choice(username_list).strip()
                username.send_keys(user_name)
                next_button = driver.find_element(By.XPATH,"//span[contains(text(),'Next')]")
                next_button.click()

                time.sleep(5)
                password = driver.find_element(By.XPATH,"//input[@name='password']")
                password.send_keys(Password)
                log_in = driver.find_element(By.XPATH,"//span[contains(text(),'Log in')]")
                log_in.click()
                time.sleep(5)
                logger.info(f"Login successfully into account {user_name}.")
                break
            
            except Exception as e:
                logger.info(f"Try to login again into account {user_name}.")
                driver.quit()
                time.sleep(20)
                continue

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

        keep_scrolling = True
        count = 0
        while keep_scrolling:
            initial_scroll_position = driver.execute_script(
                "return window.scrollY;"
            )
            while True:
                try:    
                    articles = WebDriverWait(driver, 20).until(
                        EC.presence_of_all_elements_located(
                            (By.XPATH,"//article[@data-testid='tweet']")
                        )
                    )
                    break
                except TimeoutError:
                    logger.info(f"Iter {count} TimeoutError for topic {topic}")
                    time.sleep(20)
                    continue

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
                run_count = 0
                while run_count < 10:
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
                        logger.info(f"Try to get infomation again for topic {topic}")
                        run_count += 1
                        time.sleep(20)
                        continue

            driver.execute_script('window.scrollBy(0,3200);')
            time.sleep(iterInterval)
            count += 1
            logger.info(f"Finish crawling {count} iteration for topic {topic}")
            current_scroll_position = driver.execute_script(
                "return window.scrollY;"
            )

            if current_scroll_position == initial_scroll_position:
                logger.info(
                    f"Finish crawling twitter account for topic {topic}"
                )
                keep_scrolling = False
        
        write_to_file(df, f"{data_dir}/{topic}.json")
        driver.quit()



if __name__ == "__main__":

    # df = crawling_twitter_account(
    #     numIter = numIter,
    #     iterInterval = iterInterval
    # )
    # df = pd.DataFrame(df)
    # df.to_csv("./data/twitter_account.csv", index=False)
    logger.info("Finish crawling twitter account")