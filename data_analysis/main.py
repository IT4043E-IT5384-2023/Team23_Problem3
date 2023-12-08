from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

##Configuration
spark = SparkSession \
            .builder \
            .appName("SimpleSparkJob") \
            .master("local[*]") \
            .config("spark.jars", "/opt/spark/jars/gcs-connector-latest-hadoop2.jar") \
            .getOrCreate()

#config the credential to identify the google cloud hadoop file
spark.conf.set("google.cloud.auth.service.account.json.keyfile","/opt/spark/lucky-wall-393304-2a6a3df38253.json")
spark._jsc.hadoopConfiguration().set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
spark._jsc.hadoopConfiguration().set('fs.gs.auth.service.account.enable', 'true')

# Connect to the file in Google Bucket with Spark
path=f"gs://it4043e-it5384/addresses.csv"
df = spark.read.csv(path)
df.show()

spark.stop() # Ending spark job