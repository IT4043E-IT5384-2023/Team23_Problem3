# Evaluating Twitter accounts discussing web3 projects


## Environment setup

Create and activate a conda environment named `group23`:

```sh
conda create -n group23 --file environment.yml
conda activate group23
```

## Data crawling

1. Go to the directory `data_crawling`.

2. Create a file named `.env` and set the following environment variables:

   * `USER_NAME`: Username for your accounts in twitter (Ex. `'username1, username2, username3'`)
   * `PASSWORD`:  Your twitter accounts password (Ex. `'pass1, pass2, pass3'`)
   * `EMAIL`: Your email account associate with twiter accounts above (Ex. `'email1@gmail.com, email2@gmail.com, email3@gmail.com'`)
   * `EMAIL_PASWORD`: Password of your email accounts

3. Configure where to save the data and log in `config.py`:

4. Run the following command:
   ```sh
   python main.py
   ```
