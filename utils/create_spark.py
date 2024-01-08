from pyspark.sql import SparkSession

from google.cloud import storage


def create_spark(
    app_name: str, 
    master: str, 
    executor_memory: str, 
    driver_memory: str, 
    executor_cores: str, 
    worker_memory: str, 
    max_result_size: str, 
    kryo_max_buffer: str,
    gcs_connector_jar_path: str, 
    service_account_keyfile_path: str
) -> SparkSession:
    """
    Create SparkSession with Google Cloud Storage connector.
    """
    
    # Configure SparkSession
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
        'fs.gs.impl',
        'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem'
    )
    spark._jsc.hadoopConfiguration().set('fs.gs.auth.service.account.enable', 'true')

    return spark