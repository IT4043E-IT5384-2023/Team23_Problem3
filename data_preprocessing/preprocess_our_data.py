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

from config import OUR_RAW_TWEETS_DIR, OUR_PREPROCESSED_DIR
from utils.create_spark import create_spark


def preprocess_our_data( 
    spark: SparkSession,
    input_data_path: str, 
    output_data_path: str
) -> None:
    """
    Extract features from our raw crawled tweet data and save in parquet format.
    """

    print(f'Preproccesing crawled data at {input_data_path}...')
    df_tweets = spark.read.option("multiline", True).json(input_data_path)
    df_tweets.createTempView("tweet")

    df_user_features = spark.sql("""
        SELECT DISTINCT
            user.id_str AS id,
            user.username AS screen_name,
            CAST((UNIX_TIMESTAMP(TO_TIMESTAMP(date)) - UNIX_TIMESTAMP(TO_TIMESTAMP(user.created))) / 3600 AS FLOAT) AS user_age,
            user.statusesCount AS statuses_count,
            user.followersCount AS followers_count,
            user.friendsCount AS friends_count,
            user.favouritesCount AS favourites_count,
            user.listedCount AS listed_count,
            user.profileImageUrl IS NULL AS default_profile,
            user.profileBannerUrl IS NOT NULL AS profile_use_background_image,
            user.verified AS verified,
            statuses_count / user_age AS tweet_freq,
            followers_count / user_age AS followers_growth_rate,
            friends_count / user_age AS friends_growth_rate,
            favourites_count / user_age AS favourites_growth_rate,
            listed_count / user_age AS listed_growth_rate,
            followers_count / GREATEST(friends_count, 1) AS followers_friends_ratio,
            LEN(screen_name) AS screen_name_length,
            LEN(REGEXP_REPLACE(screen_name, '[^0-9]', '')) AS num_digits_in_screen_name,
            IFNULL(LEN(user.displayname), 0) AS name_length,
            IFNULL(LEN(REGEXP_REPLACE(user.displayname, '[^0-9]', '')), 0) AS num_digits_in_name,
            IFNULL(LEN(user.rawDescription), 0) AS description_length
        FROM tweet
        WHERE CAST((UNIX_TIMESTAMP(TO_TIMESTAMP(date)) - UNIX_TIMESTAMP(TO_TIMESTAMP(user.created))) / 3600 AS FLOAT) IS NOT NULL
    """)

    df_user_features.write.parquet(output_data_path, mode='overwrite')

    print(f'Preprocessed data saved at {output_data_path}')  


if __name__ == "__main__":

    spark = create_spark(
        app_name="data-preprocessing",
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

    preprocess_our_data(
        spark=spark,
        input_data_path=OUR_RAW_TWEETS_DIR,
        output_data_path=OUR_PREPROCESSED_DIR
    )

    # check the output
    df_user_features = spark.read.parquet(OUR_PREPROCESSED_DIR)
    df_user_features.printSchema()
    df_user_features.show()

    spark.stop()