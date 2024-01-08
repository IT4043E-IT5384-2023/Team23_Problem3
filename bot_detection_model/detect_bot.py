import os
import sys 
sys.path.insert(
    0, os.path.join(
        os.path.dirname(
            os.path.abspath(__file__)
        ), 
    "..")
)

from pyspark.sql import SparkSession, DataFrame
from pyspark.ml.pipeline import PipelineModel

from config import OUR_PREPROCESSED_DIR, BOT_DETECTION_OUTPUT_DIR
from utils.create_spark import create_spark


def detect_bot(spark: SparkSession, preprocessed_input_path: str, model_path: str, output_path: str=None) -> DataFrame:
    model = PipelineModel.load(model_path)
    input_df = spark.read.parquet(preprocessed_input_path)
    output_df = model.transform(input_df)
    output_df.show(truncate=False)
    if output_path:
        output_df.select('id', 'prediction').write.parquet(output_path, mode='overwrite')
        print('Saved (id, prediction) rows in parquet format at', os.path.abspath(output_path))
    return output_df


if __name__ == "__main__":

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