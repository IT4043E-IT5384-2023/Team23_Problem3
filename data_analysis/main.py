from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("SimpleSparkJob").master("local[*]").getOrCreate()
#TODO set master URL to connect to Centic's Spark cluster

#Testing Spark
df = spark.range(0, 10, 1).toDF("id")
df_transformed = df.withColumn("square", df["id"] * df["id"])
df_transformed.show()

spark.stop() # Ending spark job