import os
import sys 
sys.path.insert(
    0, os.path.join(
        os.path.dirname(
            os.path.abspath(__file__)
        ), 
    "..")
)

# LOGGER
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - line %(lineno)d - %(message)s"
LOG_DIR = './logs'
LOG_FILE = './logs/crawler.log'

# DATA
TWEETS_DIR = '/home/quangbinh/big_data_storage/Team23_Problem3/data/raw/tweets'
USERS_DIR = '/home/quangbinh/big_data_storage/Team23_Problem3/data/raw/users'


# LISTENER
DIR_LISTENER = '/home/quangbinh/big_data_storage/Team23_Problem3/data/new_files.txt'
