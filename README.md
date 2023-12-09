# Evaluating Twitter accounts discussing web3 projects


## Environment setup

Create and activate a conda environment named, for example, `group23` with the dependencies specified in the file `environment.yml`:

```sh
conda create -n group23 --file environment.yml
conda activate group23
```


## Data crawling

1. Go to the directory `data_crawling`.

2. Create a file named `.env` and set the following environment variables:

   * `USER_NAME`: your Twitter usernames (Ex. `'username1, username2, username3'`)
   * `PASSWORD`:  your Twitter account passwords (Ex. `'pass1, pass2, pass3'`)
   * `EMAIL`: the email addresses associated with the Twitter accounts (Ex. `'email1@gmail.com, email2@gmail.com, email3@gmail.com'`)
   * `EMAIL_PASWORD`: the passwords of your email accounts

3. Configure where to save the data and log in the file `config.py`.

4. Run the script `main.py`


## Data preprocessing

Run the notebook `data_preprocessing/main.ipynb`. The current code reads and writes data locally.