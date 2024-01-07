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
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.pipeline import PipelineModel

from config import BOT_REPOSITORY_PREPROCESSED_DIR
from utils.create_spark import create_spark



def train_and_test(
    spark: SparkSession,
    preprocessed_train_data_path: str,
    preprocessed_test_data_path: str,
    model_storage_path: str=None
) -> PipelineModel:
    """
    Train and test random forest bot detection model.
    """

    # Load training and test data
    train_data = spark.read.parquet(preprocessed_train_data_path)
    test_data = spark.read.parquet(preprocessed_test_data_path)
    print("Loaded", train_data.count(), "rows of training data")
    print("Loaded", test_data.count(), "rows of test data")

    # Define pipeline
    assembler = VectorAssembler(
        inputCols=[
            'statuses_count', 'followers_count', 'friends_count', 'favourites_count', 'listed_count', 'default_profile', 'profile_use_background_image', 'verified',
            'tweet_freq', 'followers_growth_rate', 'friends_growth_rate', 'favourites_growth_rate', 'listed_growth_rate', 'followers_friends_ratio',
            'screen_name_length', 'num_digits_in_screen_name', 'name_length', 'num_digits_in_name', 'description_length'
        ],
        outputCol='features',
        handleInvalid='keep'
    )
    random_forest = RandomForestClassifier(labelCol='bot', seed=42)
    pipeline = Pipeline(stages=[assembler, random_forest])

    # Train model
    print("Training model...")
    model = pipeline.fit(train_data)
    print("Training complete")

    # Test model
    print("Testing model...")
    predictions = model.transform(test_data)
    predictions.show(truncate=False)
    binary_classif_metrics = ('areaUnderROC', 'areaUnderPR')
    for metric in binary_classif_metrics:
        evaluator = BinaryClassificationEvaluator(metricName=metric, labelCol='bot')
        print(f'{metric}: {evaluator.evaluate(predictions)}')
    multiclass_classif_metrics = ('accuracy', 'weightedPrecision', 'weightedRecall', 'f1', 'weightedFMeasure')
    for metric in multiclass_classif_metrics:
        evaluator = MulticlassClassificationEvaluator(metricName=metric, labelCol='bot')
        print(f'{metric}: {evaluator.evaluate(predictions)}')
    
    # Save model
    if model_storage_path:
        model.write().overwrite().save(model_storage_path)
        print('Saved model at', os.path.abspath(model_storage_path))
    
    return model



if __name__ == "__main__":

    spark = create_spark(
        app_name="bot-detection-model-training-and-testing",
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

    train_and_test(
        spark=spark,
        preprocessed_train_data_path=os.path.join(BOT_REPOSITORY_PREPROCESSED_DIR, 'train'),
        preprocessed_test_data_path=os.path.join(BOT_REPOSITORY_PREPROCESSED_DIR, 'test'),
        model_storage_path='bot_detection_model/model_storage'
    )