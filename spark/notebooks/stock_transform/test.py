# # README:
# # SPARK_APPLICATION_ARGS contains stock-market/AAPL/prices.json
# # SPARK_APPLICATION_ARGS will be passed to the Spark application as an argument -e when running the Spark application from Airflow
# # - Sometimes the script can stay stuck after "Passing arguments..."
# # - Sometimes the script can stay stuck after "Successfully stopped SparkContext"
# # - Sometimes the script can show "WARN TaskSchedulerImpl: Initial job has not accepted any resources; check your cluster UI to ensure that workers are registered and have sufficient resources"
# # The easiest way to solve that is to restart your Airflow instance
# # astro dev kill && astro dev start
# # Also, make sure you allocated at least 8gb of RAM to Docker Desktop
# # Go to Docker Desktop -> Preferences -> Resources -> Advanced -> Memory

# # Import the SparkSession module
# from pyspark.sql import SparkSession
# from pyspark import SparkContext
# from pyspark.sql.functions import explode, arrays_zip, from_unixtime, col, to_date
# from pyspark.sql.types import StructType, StructField, ArrayType, StringType, DoubleType, IntegerType

# import os
# import sys

# if __name__ == '__main__':

#     def app():
#         # Create a SparkSession
#         spark = SparkSession.builder.appName("FormatStock") \
#             .config("fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "minio")) \
#             .config("fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "minio123")) \
#             .config("fs.s3a.endpoint", os.getenv("ENDPOINT", "http://host.docker.internal:9000")) \
#             .config("fs.s3a.connection.ssl.enabled", "false") \
#             .config("fs.s3a.path.style.access", "true") \
#             .config("fs.s3a.attempts.maximum", "1") \
#             .config("fs.s3a.connection.establish.timeout", "5000") \
#             .config("fs.s3a.connection.timeout", "10000") \
#             .getOrCreate()

#         # Define the schema for the JSON data
#         schema = StructType([
#             StructField("meta", StructType([
#                 StructField("symbol", StringType(), True)
#             ])),
#             StructField("prices", ArrayType(StructType([
#                 StructField("Date", StringType(), True),
#                 StructField("Open", DoubleType(), True),
#                 StructField("High", DoubleType(), True),
#                 StructField("Low", DoubleType(), True),
#                 StructField("Close", DoubleType(), True),
#                 StructField("Volume", IntegerType(), True),
#                 StructField("Dividends", DoubleType(), True),
#                 StructField("Stock Splits", DoubleType(), True)
#             ])))
            
#         ])
        
#         # Read the JSON file from MimiO
#         input_path = os.getenv('SPARK_APPLICATION_ARGS')
#         df = spark.read.schema(schema).json(f"s3a://{input_path}/*.json")
        
#         df_exploded = df.select(col("meta.symbol").alias("Symbol"), explode("prices").alias("price")) \
#             .select("Symbol",
#                     col("price.Date").alias("Date"),
#                     col("price.Open").alias("Open"),
#                     col("price.High").alias("High"),
#                     col("price.Low").alias("Low"),
#                     col("price.Close").alias("Close"),
#                     col("price.Volume").alias("Volume"))
#         # Convert Date string to Date type
#         df_formatted = df_exploded.withColumn("Date", to_date("Date"))

#         output_path = f"{input_path}/formatted_prices"
#         # Store in MinIO
#         df_formatted.write \
#             .mode("overwrite") \
#             .option("header", "true") \
#             .option("delimiter", ",") \
#             .csv(f"s3a://{output_path}")
            
#         print(f"Formatted data saved to: s3a://{output_path}")

#     app()
#     os.system('kill %d' % os.getpid())