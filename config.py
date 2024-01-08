# logger
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - line %(lineno)d - %(message)s"
LOG_DIR = 'logs'
LOG_FILE = 'logs/crawler.log'

# our crawled data (unlabeled)
CRYPTO_KEYWORDS_JSON = 'data/ours/crypto_keywords.json'
OUR_RAW_TWEETS_DIR = 'data/ours/raw/tweets'
OUR_RAW_USERS_DIR = 'data/ours/raw/users'
OUR_PREPROCESSED_DIR = 'data/ours/preprocessed'

# labeled data for training and testing bot detection model
BOT_REPOSITORY_RAW_DIR = 'data/bot_repository/raw'
BOT_REPOSITORY_PREPROCESSED_DIR = 'data/bot_repository/preprocessed'

# listener
DIR_LISTENER = 'data/new_files.txt'

# bot detection output
BOT_DETECTION_OUTPUT_DIR = 'data/ours/predictions'