import time
import os
import pandas as pd
import dotenv
from utils.set_logger import setup_logger

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.service import Service as ChromeService # Similar thing for firefox also!
from subprocess import CREATE_NO_WINDOW # This flag will only be available in windows
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException


logger = setup_logger(__name__)

dotenv.load_dotenv()
Username = os.getenv("TWITTER_USER_NAME")
logger.info(f"Finish loading user name from .env file")

Password = os.getenv("TWITTER_USER_PASSWORD")
logger.info(f"Finish loading user password from .env file")

URL = "https://twitter.com/search?q=%23crypto&src=typed_query"

#Number of scroll down refreshing
numIter = 10
#Interval between 2 Iteration
iterInterval = 5
#-------------------------------------------------------------------------------------

chrome_options = Options()
chrome_options.add_argument('--ignore-certificate-errors')
chrome_options.add_argument('--disable-gpu')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument("--headless=new")
chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])

# -------------------------------------------------------------------------------------

def crawling_twitter_account(
    # data_dir: str, 
    numIter: int, 
    iterInterval: int
) -> pd.DataFrame:
    """
    Crawl twitter account
    -------------
    Args:
        data_dir: str
            Directory to save data
        numIter: int
            Number of scroll down refreshing
        iterInterval: int
            Interval between 2 Iteration
    Return:
        df: pd.DataFrame
            Dataframe of crawled data

    """
    driver = webdriver.Chrome(options=chrome_options)

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

    driver.get(URL)

    df = {
        'UserTag': [],
        'Time': [],
        'Tweet': [],
        'Reply': [],
        'reTweet': [],
        'Like': [],
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
            continue

        for article in articles:
            try:
                driver.implicitly_wait(5)
                UserTag = article.find_element(
                    By.XPATH,".//div[@data-testid='User-Name']"
                ).text
                df['UserTag'].append(UserTag)

            except Exception as e:
                logger.error(e)
                pass
        
            try:
                driver.implicitly_wait(5)
                Time = article.find_element(
                    By.XPATH,".//time"
                ).get_attribute('datetime')

                df['Time'].append(Time)
            except Exception as e:
                logger.error(e)
                pass
        
            try:
                driver.implicitly_wait(5)
                Tweet = article.find_element(
                    By.XPATH,".//div[@data-testid='tweetText']"
                ).text
                df['Tweet'].append(Tweet)   
            except Exception as e:
                logger.error(e)
                pass

            try:
                driver.implicitly_wait(5)
                Reply = article.find_element(
                    By.XPATH,".//div[@data-testid='reply']"
                ).text
                df['Reply'].append(Reply)
            except Exception as e:
                logger.error(e)
                pass
        
            try:
                driver.implicitly_wait(5)
                reTweet = article.find_element(
                    By.XPATH,".//div[@data-testid='retweet']"
                ).text
                df['reTweet'].append(reTweet)
            except Exception as e:
                logger.error(e)
                pass
        
            try:
                driver.implicitly_wait(5)
                Like = article.find_element(
                    By.XPATH,".//div[@data-testid='like']"
                ).text
                df['Like'].append(Like)
            except Exception as e:
                logger.error(e)
                pass

            try:
                driver.implicitly_wait(5)
                View = article.find_element(
                    By.XPATH,".//a[@role='link']/div/div[2]/span/span/span"
                ).text
                df['View'].append(View)
            except Exception as e:
                logger.error(e)
                pass

        logger.info("Iter {}/{}: Retrieving succeed.".format(i + 1, numIter))
        driver.execute_script('window.scrollBy(0,3200);')
        time.sleep(iterInterval)

    driver.quit()
    return df


if __name__ == "__main__":

    df = crawling_twitter_account(
        numIter = numIter,
        iterInterval = iterInterval
    )
    df = pd.DataFrame(df)
    df.to_csv("./data/twitter_account.csv", index=False)
    logger.info("Finish crawling twitter account")