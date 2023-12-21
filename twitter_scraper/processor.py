from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, 
    StringType, IntegerType
)
from google.cloud import storage


def process_data(
    app_name: str = None, 
    master: str = None, 
    executor_memory: str = None, 
    driver_memory: str = None, 
    executor_cores: str = None, 
    worker_memory: str = None, 
    max_result_size: str = None, 
    kryo_max_buffer: str = None,
    gcs_connector_jar_path: str = None, 
    service_account_keyfile_path: str = None, 
    input_data_path: str = None, 
    output_data_path: str = None
) -> None:
    # Initialize Spark Session
    spark = (
        SparkSession.builder.appName(app_name).master(master)
        .config("spark.jars", gcs_connector_jar_path)
        .config("spark.executor.memory", executor_memory)
        .config("spark.driver.memory", driver_memory)
        .config("spark.executor.cores", executor_cores)
        .config("spark.python.worker.memory", worker_memory)
        .config("spark.driver.maxResultSize", max_result_size)
        .config("spark.kryoserializer.buffer.max", kryo_max_buffer)
        .getOrCreate()
    )

    # Configure credentials for Google Cloud Storage
    spark.conf.set(
        "google.cloud.auth.service.account.json.keyfile",
        service_account_keyfile_path
    )
    spark._jsc.hadoopConfiguration().set(
        'fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem'
    )
    spark._jsc.hadoopConfiguration().set('fs.gs.auth.service.account.enable', 'true')

    # Read data
    df = spark.read.option("multiline", True).json(input_data_path)
    df.createOrReplaceTempView("data")
    df.printSchema()

    # Processing data
    df_features = spark.sql("""
    SELECT DISTINCT
        user.id,
        -- 4 among top 10 boolean user features having highest IG with human/bot label
        size(user.descriptionLinks) > 0 as hasURL,
        user.location <> "" as geoEnabled,
        user.verified,
        user.profileBannerUrl IS NOT NULL as profileBackgroungImageURL,
        -- top 10 numerical user features having highest IG with human/bot label
        user.followersCount / user.friendsCount as followerFriendRatio,
        user.listedCount,
        LENGTH(user.rawDescription) as descriptionLength,
        user.followersCount,
        LENGTH(user.username) as nameLength,
        UNIX_TIMESTAMP(TO_DATE(user.created)) as createdAt,
        user.friendsCount,
        user.statusesCount,
        user.favouritesCount,
        LENGTH(user.displayname) as screenNameLength,
        -- tweet
        rawContent
    FROM data
    """)

    # Write the processed data
    df_features.write.parquet(output_data_path)

    # Stop the Spark session
    spark.stop()


if __name__ == "__main__":

    process_data(
        app_name="SimpleSparkJob",
        master="local",
        executor_memory="2G",
        driver_memory="4G",
        executor_cores="3",
        worker_memory="1G",
        max_result_size="3G",
        kryo_max_buffer="1024M",
        gcs_connector_jar_path="/opt/spark/jars/gcs-connector-latest-hadoop2.jar",
        service_account_keyfile_path="/opt/spark/lucky-wall-393304-2a6a3df38253.json",
        input_data_path="../data",
        output_data_path="../data/preprocessed.parquet"
    )
