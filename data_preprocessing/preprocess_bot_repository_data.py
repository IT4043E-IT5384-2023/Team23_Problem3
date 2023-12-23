from functools import reduce
import os
import sys 
sys.path.insert(
    0, os.path.join(
        os.path.dirname(
            os.path.abspath(__file__)
        ), 
    "..")
)

from pyspark.sql import SparkSession

from config import BOT_REPOSITORY_RAW_DIR, BOT_REPOSITORY_PREPROCESSED_DIR
from create_spark import create_spark


def preprocess_bot_repository_data( 
    spark: SparkSession,
    bot_repository_raw_path: str, 
    output_path: str
) -> None:
    """
    Extract features and labels from BotRepository datasets and save in parquet format.
    """

    cresci2017_fake_followers_datasets = ('fake_followers.csv',) #train
    cresci2017_other_bot_datasets = (
        'social_spambots_1.csv', 'social_spambots_2.csv', 'social_spambots_3.csv',
        'traditional_spambots_1.csv', 'traditional_spambots_2.csv', 'traditional_spambots_3.csv', 'traditional_spambots_4.csv'
    ) #train
    cresci2017_human_datasets = ('genuine_accounts.csv',) #train
    midterm2018_datasets = ('midterm-2018',) #test
    other_datasets_train = ('botometer-feedback-2019', 'celebrity-2019', 'political-bots-2019')
    other_datasets_test = ('botwiki-2019', 'verified-2019', 'gilani-2017', 'cresci-rtbust-2019')

    df_train_list, df_test_list = [], []

    for dataset in cresci2017_fake_followers_datasets:
        print(f'Preproccesing {dataset}...')
        df_tweets = spark.read.csv(
            os.path.join(bot_repository_raw_path, 'cresci-2017.csv', dataset, 'tweets.csv'),
            header=True,
            inferSchema=True
        )
        df_users = spark.read.option("header", True).csv(
            os.path.join(bot_repository_raw_path, 'cresci-2017.csv', dataset, 'users.csv'),
            header=True,
            inferSchema=True
        )
        df_tweets.createOrReplaceTempView("tweet1")
        df_users.createOrReplaceTempView("user1")
        df_train_list.append(spark.sql("""
        SELECT
            user1.id,
            name,
            screen_name,
            statuses_count,
            followers_count,
            friends_count,
            favourites_count,
            listed_count,
            default_profile IS NOT NULL,
            profile_use_background_image IS NOT NULL,
            verified IS NOT NULL,
            cast((unix_timestamp(to_timestamp(timestamp)) - unix_timestamp(to_timestamp(user1.created_at))) / 3600 AS float) AS user_age,
            LEN(description) AS description_length,
            True as bot
        FROM
            tweet1
            INNER JOIN user1 ON (tweet1.user_id = user1.id)
        """))

    for dataset in cresci2017_other_bot_datasets:
        print(f'Preproccesing {dataset}...')
        df_users = spark.read.csv(
            os.path.join(bot_repository_raw_path, 'cresci-2017.csv', dataset, 'users.csv'),
            header=True,
            inferSchema=True
        )
        df_users.createOrReplaceTempView("user2")
        df_train_list.append(spark.sql("""
        SELECT
            id,
            name,
            screen_name,
            statuses_count,
            followers_count,
            friends_count,
            favourites_count,
            listed_count,
            default_profile IS NOT NULL,
            profile_use_background_image IS NOT NULL,
            verified IS NOT NULL,
            cast((unix_timestamp(to_timestamp(crawled_at)) - unix_timestamp(to_timestamp(created_at))) / 3600 AS float) AS user_age,
            LEN(description) AS description_length,
            True as bot
        FROM user2
        """))

    for dataset in cresci2017_human_datasets:
        print(f'Preproccesing {dataset}...')
        df_users = spark.read.csv(
            os.path.join(bot_repository_raw_path, 'cresci-2017.csv', dataset, 'users.csv'),
            header=True,
            inferSchema=True
        )
        df_users.createOrReplaceTempView("user3")
        df_train_list.append(spark.sql("""
        SELECT
            id,
            name,
            screen_name,
            statuses_count,
            followers_count,
            friends_count,
            favourites_count,
            listed_count,
            default_profile IS NOT NULL,
            profile_use_background_image IS NOT NULL,
            verified IS NOT NULL,
            cast((unix_timestamp(to_timestamp(crawled_at)) - unix_timestamp(to_timestamp(created_at))) / 3600 AS float) AS user_age,
            LEN(description) AS description_length,
            False as bot
        FROM user3
        """))
    
    for dataset in midterm2018_datasets:
        print(f'Preproccesing {dataset}...')
        df_users = spark.read.json(
            os.path.join(bot_repository_raw_path, dataset, f'{dataset}_processed_user_objects.json')
        )
        df_user_labels = spark.read.csv(
            os.path.join(bot_repository_raw_path, dataset, f'{dataset}.tsv'),
            header=False,
            schema='id STRING, label STRING',
            sep='\t'
        )
        df_users.createOrReplaceTempView("user4")
        df_user_labels.createOrReplaceTempView("user_label_4")
        df_test_list.append(spark.sql("""
        SELECT
            user_label_4.id AS id,
            name,
            screen_name,
            statuses_count,
            followers_count,
            friends_count,
            favourites_count,
            listed_count,
            default_profile,
            profile_use_background_image,
            verified,
            cast((unix_timestamp(to_timestamp(probe_timestamp)) - unix_timestamp(to_timestamp(user_created_at))) / 3600 AS float) AS user_age,
            LEN(description) AS description_length,
            label = 'bot' AS bot
        FROM
            user4
            INNER JOIN user_label_4 ON (CAST(user4.user_id AS STRING) = user_label_4.id)
        """))
    
    for dataset in other_datasets_train:
        print(f'Preproccesing {dataset}...')
        df_tweets = spark.read.json(
            os.path.join(bot_repository_raw_path, dataset, f'{dataset}_tweets.json')
        )
        df_user_labels = spark.read.csv(
            os.path.join(bot_repository_raw_path, dataset, f'{dataset}.tsv'),
            header=False,
            schema='id STRING, label STRING',
            sep='\t'
        )
        df_tweets.createOrReplaceTempView("tweet5")
        df_user_labels.createOrReplaceTempView("user_label_5")
        df_train_list.append(spark.sql("""
        SELECT
            tweet5.user.id_str AS id,
            user.name AS name,
            user.screen_name,
            user.statuses_count,
            user.followers_count,
            user.friends_count,
            user.favourites_count,
            user.listed_count,
            user.default_profile,
            user.profile_use_background_image,
            user.verified,
            cast((unix_timestamp(to_timestamp(created_at)) - unix_timestamp(to_timestamp(user.created_at))) / 3600 AS float) AS user_age,
            LEN(user.description) AS description_length,
            user_label_5.label = 'bot' AS bot
        FROM
            tweet5
            INNER JOIN user_label_5 ON (tweet5.user.id = user_label_5.id)
        """))

        for dataset in other_datasets_test:
            print(f'Preproccesing {dataset}...')
            df_tweets = spark.read.json(
                os.path.join(bot_repository_raw_path, dataset, f'{dataset}_tweets.json')
            )
            df_user_labels = spark.read.csv(
                os.path.join(bot_repository_raw_path, dataset, f'{dataset}.tsv'),
                header=False,
                schema='id STRING, label STRING',
                sep='\t'
            )
            df_tweets.createOrReplaceTempView("tweet6")
            df_user_labels.createOrReplaceTempView("user_label_6")
            df_test_list.append(spark.sql("""
            SELECT
                tweet6.user.id_str AS id,
                user.name AS name,
                user.screen_name,
                user.statuses_count,
                user.followers_count,
                user.friends_count,
                user.favourites_count,
                user.listed_count,
                user.default_profile,
                user.profile_use_background_image,
                user.verified,
                cast((unix_timestamp(to_timestamp(created_at)) - unix_timestamp(to_timestamp(user.created_at))) / 3600 AS float) AS user_age,
                LEN(user.description) AS description_length,
                user_label_6.label = 'bot' AS bot
            FROM
                tweet6
                INNER JOIN user_label_6 ON (tweet6.user.id = user_label_6.id)
            """))
    
    df_train = reduce(lambda x, y: x.unionAll(y), df_train_list)
    df_test = reduce(lambda x, y: x.unionAll(y), df_test_list)

    df_train.write.parquet(os.path.join(output_path, 'train'))
    df_test.write.parquet(os.path.join(output_path, 'test'))

    print(f'All preprocessed data saved at {output_path}')


if __name__ == "__main__":

    spark = create_spark(
        app_name="preprocess-our-data",
        master="local",
        executor_memory="1G",
        driver_memory="1G",
        executor_cores="1",
        worker_memory="1G",
        max_result_size="1G",
        kryo_max_buffer="1024M",
        gcs_connector_jar_path="/opt/spark/jars/gcs-connector-latest-hadoop2.jar",
        service_account_keyfile_path="/opt/bucket_connector/lucky-wall-393304-3fbad5f3943c.json"
    )

    preprocess_bot_repository_data(
        spark=spark,
        bot_repository_raw_path=BOT_REPOSITORY_RAW_DIR,
        output_path=BOT_REPOSITORY_PREPROCESSED_DIR
    )

    # check the output
    df_train = spark.read.parquet(os.path.join(BOT_REPOSITORY_PREPROCESSED_DIR, 'train'))
    print('Training data:')
    df_train.show()
    df_test = spark.read.parquet(os.path.join(BOT_REPOSITORY_PREPROCESSED_DIR, 'test'))
    print('Testing data:')
    df_test.show()

    spark.stop()