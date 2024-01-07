# Evaluating Twitter accounts discussing web3 projects


## Environment setup

Create and activate a conda environment named, for example, `group23` with the dependencies specified in the file `environment.yml`:

```sh
conda env create -n group23 --file environment.yml
conda activate group23
```


## Data crawling

1. Create a file named `.env` and set the following environment variables:

   * `USER_NAME`: your Twitter usernames (Ex. `'username1, username2, username3'`)

   * `PASSWORD`:  your Twitter account passwords (Ex. `'pass1, pass2, pass3'`)

   * `EMAIL`: the email addresses associated with the Twitter accounts (Ex. `'email1@gmail.com, email2@gmail.com, email3@gmail.com'`)
   
   * `EMAIL_PASWORD`: the passwords of your email accounts

2. Configure where to save the data and log in the file `config.py`.

3. Run the script `twitter_scraper/crawler.py`


## Data preprocessing

There are two datasets to preprocess:

   * the data we crawl using the process above, which has no bot/human label

   * data from [BotRepository](https://botometer.osome.iu.edu/bot-repository/datasets.html) with a human/bot label for each Twitter account, which we're going to use for training and testing our bot detection model

To run a data preprocessing job:

   1. Configure input and output locations in the file `config.py`

   2. Run the script `data_preprocessing/preprocess_our_data.py` or `data_preprocessing/preprocess_bot_repository_data.py`.


## Data analysis

Following the paper "[Scalable and Generalizable Social Bot Detection through Data Selection (Yang et al., 2020)](https://ojs.aaai.org/index.php/AAAI/article/view/5460)", we implement a random forest using 19 account metadata features to predict whether the account is a human or bot account. A trained model is available at `bot_detection_model/model_storage`.

To use the trained bot detection model on crawled tweets:

1. Configure the preprocessed data location and model output ((id, prediction) rows in parquet format) location in the file `config.py`.

2. Run the script `bot_detection_model/detect_bot.py`