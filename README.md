# Evaluating Twitter accounts discussing web3 projects


## Environment setup

Create and activate a conda environment named, for example, `group23` with the dependencies specified in the file `environment.yml`:

```sh
conda create -n group23 --file environment.yml
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

   1. Configure input and output locations in the file `config.py`. The current code reads and writes data locally.

   2. Run the file `data_preprocessing/preprocess_our_data.py` or `data_preprocessing/preprocess_bot_repository_data.py`.