from functools import reduce
from datetime import datetime
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
from pyspark.sql.types import FloatType

from config import BOT_REPOSITORY_RAW_DIR, BOT_REPOSITORY_PREPROCESSED_DIR
from utils.create_spark import create_spark


def hour_diff(end_time_str: str,
              start_time_str: str,
              time_format: str='%a %b %d %H:%M:%S %z %Y'
) -> float:
    """
    Calculate the difference between two timestamps in hours.
    The default time format is 'Fri Dec 07 13:57:04 +0000 2007'.
    """

    end_time = datetime.strptime(end_time_str, time_format)
    start_time = datetime.strptime(start_time_str, time_format)
    return (end_time - start_time).total_seconds() / 3600


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

    spark.udf.register("hour_diff", hour_diff, FloatType())

    for dataset in cresci2017_fake_followers_datasets:
        print(f'Preproccesing {dataset}...')
        df_tweets_1 = spark.read.csv(
            os.path.join(bot_repository_raw_path, 'cresci-2017.csv', dataset, 'tweets.csv'),
            header=True,
            inferSchema=True
        )
        df_users_1 = spark.read.option("header", True).csv(
            os.path.join(bot_repository_raw_path, 'cresci-2017.csv', dataset, 'users.csv'),
            header=True,
            inferSchema=True
        )
        df_tweets_1.createOrReplaceTempView("tweet1")
        df_users_1.createOrReplaceTempView("user1")
        df_train_list.append(
            spark.sql("""
                SELECT
                    user1.id AS id,
                    screen_name,
                    hour_diff(tweet1.created_at, user1.created_at) AS user_age,
                    statuses_count,
                    followers_count,
                    friends_count,
                    favourites_count,
                    listed_count,
                    default_profile IS NOT NULL AS default_profile,
                    profile_use_background_image IS NOT NULL AS profile_use_background_image,
                    verified IS NOT NULL AS verified,
                    statuses_count / user_age AS tweet_freq,
                    followers_count / user_age AS followers_growth_rate,
                    friends_count / user_age AS friends_growth_rate,
                    favourites_count / user_age AS favourites_growth_rate,
                    listed_count / user_age AS listed_growth_rate,
                    followers_count / GREATEST(friends_count, 1) AS followers_friends_ratio,
                    LEN(screen_name) AS screen_name_length,
                    LEN(REGEXP_REPLACE(screen_name, '[^0-9]', '')) AS num_digits_in_screen_name,
                    IFNULL(LEN(name), 0) AS name_length,
                    IFNULL(LEN(REGEXP_REPLACE(name, '[^0-9]', '')), 0) AS num_digits_in_name,
                    IFNULL(LEN(description), 0) AS description_length,
                    1.0 AS bot
                FROM
                    tweet1
                    INNER JOIN user1 ON (tweet1.user_id = user1.id)
                WHERE hour_diff(tweet1.created_at, user1.created_at) IS NOT NULL
            """)
        )

    for dataset in cresci2017_other_bot_datasets:
        print(f'Preproccesing {dataset}...')
        df_users_2 = spark.read.csv(
            os.path.join(bot_repository_raw_path, 'cresci-2017.csv', dataset, 'users.csv'),
            header=True,
            inferSchema=True
        )
        df_users_2.createOrReplaceTempView("user2")
        df_train_list.append(spark.sql("""
            SELECT
                id,
                screen_name,
                CAST((UNIX_TIMESTAMP(TO_TIMESTAMP(crawled_at)) - UNIX_TIMESTAMP(TO_TIMESTAMP(timestamp))) / 3600 AS FLOAT) AS user_age,
                statuses_count,
                followers_count,
                friends_count,
                favourites_count,
                listed_count,
                default_profile IS NOT NULL AS default_profile,
                profile_use_background_image IS NOT NULL AS profile_use_background_image,
                verified IS NOT NULL AS verified,
                statuses_count / user_age AS tweet_freq,
                followers_count / user_age AS followers_growth_rate,
                friends_count / user_age AS friends_growth_rate,
                favourites_count / user_age AS favourites_growth_rate,
                listed_count / user_age AS listed_growth_rate,
                followers_count / GREATEST(friends_count, 1) AS followers_friends_ratio,
                LEN(screen_name) AS screen_name_length,
                LEN(REGEXP_REPLACE(screen_name, '[^0-9]', '')) AS num_digits_in_screen_name,
                IFNULL(LEN(name), 0) AS name_length,
                IFNULL(LEN(REGEXP_REPLACE(name, '[^0-9]', '')), 0) AS num_digits_in_name,
                IFNULL(LEN(description), 0) AS description_length,
                1.0 AS bot
            FROM user2
            WHERE CAST((UNIX_TIMESTAMP(TO_TIMESTAMP(crawled_at)) - UNIX_TIMESTAMP(TO_TIMESTAMP(timestamp))) / 3600 AS FLOAT) IS NOT NULL
        """))

    for dataset in cresci2017_human_datasets:
        print(f'Preproccesing {dataset}...')
        df_users_3 = spark.read.csv(
            os.path.join(bot_repository_raw_path, 'cresci-2017.csv', dataset, 'users.csv'),
            header=True,
            inferSchema=True
        )
        df_users_3.createOrReplaceTempView("user3")
        df_train_list.append(spark.sql("""
            SELECT
                id,
                screen_name,
                CAST((UNIX_TIMESTAMP(TO_TIMESTAMP(crawled_at)) - UNIX_TIMESTAMP(TO_TIMESTAMP(timestamp))) / 3600 AS FLOAT) AS user_age,
                statuses_count,
                followers_count,
                friends_count,
                favourites_count,
                listed_count,
                default_profile IS NOT NULL AS default_profile,
                profile_use_background_image IS NOT NULL AS profile_use_background_image,
                verified IS NOT NULL AS verified,
                statuses_count / user_age AS tweet_freq,
                followers_count / user_age AS followers_growth_rate,
                friends_count / user_age AS friends_growth_rate,
                favourites_count / user_age AS favourites_growth_rate,
                listed_count / user_age AS listed_growth_rate,
                followers_count / GREATEST(friends_count, 1) AS followers_friends_ratio,
                LEN(screen_name) AS screen_name_length,
                LEN(REGEXP_REPLACE(screen_name, '[^0-9]', '')) AS num_digits_in_screen_name,
                IFNULL(LEN(name), 0) AS name_length,
                IFNULL(LEN(REGEXP_REPLACE(name, '[^0-9]', '')), 0) AS num_digits_in_name,
                IFNULL(LEN(description), 0) AS description_length,
                0.0 AS bot
            FROM user3
            WHERE CAST((UNIX_TIMESTAMP(TO_TIMESTAMP(crawled_at)) - UNIX_TIMESTAMP(TO_TIMESTAMP(timestamp))) / 3600 AS FLOAT) IS NOT NULL
        """))
    
    for dataset in midterm2018_datasets:
        print(f'Preproccesing {dataset}...')
        df_users_4 = spark.read.json(
            os.path.join(bot_repository_raw_path, dataset, f'{dataset}_processed_user_objects.json')
        )
        df_user_labels_4 = spark.read.csv(
            os.path.join(bot_repository_raw_path, dataset, f'{dataset}.tsv'),
            header=False,
            schema='user_id STRING, label STRING',
            sep='\t'
        )
        df_users_4.createOrReplaceTempView("user4")
        df_user_labels_4.createOrReplaceTempView("user_label_4")
        df_test_list.append(spark.sql("""
            SELECT
                user_label_4.user_id AS id,
                screen_name,
                hour_diff(probe_timestamp, user_created_at, '%a %b %d %H:%M:%S %Y') AS user_age,
                statuses_count,
                followers_count,
                friends_count,
                favourites_count,
                listed_count,
                default_profile,
                profile_use_background_image,
                verified,
                statuses_count / user_age AS tweet_freq,
                followers_count / user_age AS followers_growth_rate,
                friends_count / user_age AS friends_growth_rate,
                favourites_count / user_age AS favourites_growth_rate,
                listed_count / user_age AS listed_growth_rate,
                followers_count / GREATEST(friends_count, 1) AS followers_friends_ratio,
                LEN(screen_name) AS screen_name_length,
                LEN(REGEXP_REPLACE(screen_name, '[^0-9]', '')) AS num_digits_in_screen_name,
                IFNULL(LEN(name), 0) AS name_length,
                IFNULL(LEN(REGEXP_REPLACE(name, '[^0-9]', '')), 0) AS num_digits_in_name,
                IFNULL(LEN(description), 0) AS description_length,
                CAST(label = 'bot' AS DOUBLE) AS bot
            FROM
                user4
                INNER JOIN user_label_4 ON (CAST(user4.user_id AS STRING) = user_label_4.user_id)
            WHERE hour_diff(probe_timestamp, user_created_at, '%a %b %d %H:%M:%S %Y') IS NOT NULL
        """))
    
    for dataset in other_datasets_train:
        print(f'Preproccesing {dataset}...')
        df_tweets_5 = spark.read.json(
            os.path.join(bot_repository_raw_path, dataset, f'{dataset}_tweets.json')
        )
        df_user_labels_5 = spark.read.csv(
            os.path.join(bot_repository_raw_path, dataset, f'{dataset}.tsv'),
            header=False,
            schema='id STRING, label STRING',
            sep='\t'
        )
        df_tweets_5.createOrReplaceTempView("tweet5")
        df_user_labels_5.createOrReplaceTempView("user_label_5")
        df_train_list.append(spark.sql("""
            SELECT
                tweet5.user.id_str AS id,
                user.screen_name AS screen_name,
                hour_diff(created_at, user.created_at) AS user_age,
                user.statuses_count AS statuses_count,
                user.followers_count AS followers_count,
                user.friends_count AS friends_count,
                user.favourites_count AS favourites_count,
                user.listed_count AS listed_count,
                user.default_profile AS default_profile,
                user.profile_use_background_image AS profile_use_background_image,
                user.verified AS verified,
                statuses_count / user_age AS tweet_freq,
                followers_count / user_age AS followers_growth_rate,
                friends_count / user_age AS friends_growth_rate,
                favourites_count / user_age AS favourites_growth_rate,
                listed_count / user_age AS listed_growth_rate,
                followers_count / GREATEST(friends_count, 1) AS followers_friends_ratio,
                LEN(screen_name) AS screen_name_length,
                LEN(REGEXP_REPLACE(screen_name, '[^0-9]', '')) AS num_digits_in_screen_name,
                IFNULL(LEN(user.name), 0) AS name_length,
                IFNULL(LEN(REGEXP_REPLACE(user.name, '[^0-9]', '')), 0) AS num_digits_in_name,
                IFNULL(LEN(user.description), 0) AS description_length,
                CAST(user_label_5.label = 'bot' AS DOUBLE) AS bot
            FROM
                tweet5
                INNER JOIN user_label_5 ON (tweet5.user.id_str = user_label_5.id)
            WHERE hour_diff(created_at, user.created_at) IS NOT NULL
        """))

    for dataset in other_datasets_test:
        print(f'Preproccesing {dataset}...')
        df_tweets_6 = spark.read.json(
            os.path.join(bot_repository_raw_path, dataset, f'{dataset}_tweets.json')
        )
        df_user_labels_6 = spark.read.csv(
            os.path.join(bot_repository_raw_path, dataset, f'{dataset}.tsv'),
            header=False,
            schema='id STRING, label STRING',
            sep='\t'
        )
        df_tweets_6.createOrReplaceTempView("tweet6")
        df_user_labels_6.createOrReplaceTempView("user_label_6")
        df_test_list.append(spark.sql("""
            SELECT
                tweet6.user.id_str AS id,
                user.screen_name AS screen_name,
                hour_diff(created_at, user.created_at) AS user_age,
                user.statuses_count AS statuses_count,
                user.followers_count AS followers_count,
                user.friends_count AS friends_count,
                user.favourites_count AS favourites_count,
                user.listed_count AS listed_count,
                user.default_profile AS default_profile,
                user.profile_use_background_image AS profile_use_background_image,
                user.verified AS verified,
                statuses_count / user_age AS tweet_freq,
                followers_count / user_age AS followers_growth_rate,
                friends_count / user_age AS friends_growth_rate,
                favourites_count / user_age AS favourites_growth_rate,
                listed_count / user_age AS listed_growth_rate,
                followers_count / GREATEST(friends_count, 1) AS followers_friends_ratio,
                LEN(screen_name) AS screen_name_length,
                LEN(REGEXP_REPLACE(screen_name, '[^0-9]', '')) AS num_digits_in_screen_name,
                IFNULL(LEN(user.name), 0) AS name_length,
                IFNULL(LEN(REGEXP_REPLACE(user.name, '[^0-9]', '')), 0) AS num_digits_in_name,
                IFNULL(LEN(user.description), 0) AS description_length,
                CAST(user_label_6.label = 'bot' AS DOUBLE) AS bot
            FROM
                tweet6
                INNER JOIN user_label_6 ON (tweet6.user.id_str = user_label_6.id)
            WHERE hour_diff(created_at, user.created_at) IS NOT NULL
        """))
    
    df_train = reduce(lambda x, y: x.unionAll(y), df_train_list)
    df_test = reduce(lambda x, y: x.unionAll(y), df_test_list)

    df_train.write.parquet(os.path.join(output_path, 'train'), mode='overwrite')
    df_test.write.parquet(os.path.join(output_path, 'test'), mode='overwrite')

    print(f'All preprocessed data saved at {output_path}')


if __name__ == "__main__":

    spark = create_spark(
        app_name="preprocess-bot-repository-data",
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
    print('\nTraining data:')
    df_train.show()
    print(df_train.count(), 'rows')
    df_test = spark.read.parquet(os.path.join(BOT_REPOSITORY_PREPROCESSED_DIR, 'test'))
    print('\nTesting data:')
    df_test.show()
    print(df_test.count(), 'rows')

    spark.stop()