
import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(
    os.path.abspath(__file__)), "..", ".."))

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task


default_args = {
    'owner': 'binh.truong',
    'retries': 0,
    'retry_delay': timedelta(minutes=10)
}

with DAG(
    dag_id='crawl_raw_data',
    default_args=default_args,
    description=(
        'Components: '
        '- Crawler: Get the raw data from Twitter, partitioned by topics.\n'
        '- Processor: Using pyspark to preprocess the data.\n'
        '- Directory listener: listen to new file from raw data directory to '
    ),
    start_date=datetime.fromtimestamp(1685433464),
    catchup=False,
    schedule_interval='@daily'
) as dag:

    ##############################################################################################################
    ################################################## Get data ##################################################
    ##############################################################################################################

    @task(task_id='get_raw_data')
    def get_tweets_data(**kwargs):
        from twitter_scraper.crawler import main

        t1 = datetime.now()
        print('Start crawling stock data...')
        main()
        print(f'Finished crawling in {datetime.now() - t1}')


    @task(task_id='format_raw_data')
    def preprocess_data():
        from data_preprocessing.preprocess_our_data import preprocess_our_data
        from config import OUR_RAW_TWEETS_DIR, OUR_PREPROCESSED_DIR
        from utils.create_spark import create_spark 

        t2 = datetime.now()
        print('Start formating raw data...')
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
        print(f'Finished formating in {datetime.now() - t2}')


    @task(task_id='bot_detection_model')
    def detect_bot():
        from pyspark.sql import SparkSession, DataFrame
        from pyspark.ml.pipeline import PipelineModel

        from config import OUR_PREPROCESSED_DIR, BOT_DETECTION_OUTPUT_DIR
        from utils.create_spark import create_spark

        from bot_detection_model.detect_bot import detect_bot

        t3 = datetime.now()
        print('Start loading data to database...')
        spark = create_spark(
            app_name="bot-detection",
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

        detect_bot(
            spark=spark,
            preprocessed_input_path=OUR_PREPROCESSED_DIR,
            model_path='bot_detection_model/model_storage',
            output_path=BOT_DETECTION_OUTPUT_DIR
        )
        print(f'Finished loading in {datetime.now() - t3}')

    # get_stock_data_task = get_stock_data('{{ execution_date }}')  # Pass execution_date to the task
    # format_data_task = format_data()
    # load_transformed_data = load_transformed_data()


    get_tweets_data() >> preprocess_data() >> detect_bot()
        