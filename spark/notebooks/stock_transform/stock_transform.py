from pyspark.sql import SparkSession
from pyspark.sql.functions import col

import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == '__main__':
    
    def app():
        try:
            logger.info("Starting Spark session...")
            # Create a SparkSession
            spark = SparkSession.builder.appName("FormatStock") \
                .config("fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "minio")) \
                .config("fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "minio123")) \
                .config("fs.s3a.endpoint", os.getenv("ENDPOINT", "http://host.docker.internal:9000")) \
                .config("fs.s3a.connection.ssl.enabled", "false") \
                .config("fs.s3a.path.style.access", "true") \
                .config("fs.s3a.attempts.maximum", "1") \
                .config("fs.s3a.connection.establish.timeout", "5000") \
                .config("fs.s3a.connection.timeout", "10000") \
                .getOrCreate()

            # JSON 파일 경로 (MinIO에서 읽어오는 경우)
            input_json_path = f's3a://{os.getenv("SPARK_APPLICATION_ARGS")}'
            logger.info(f"Reading JSON file from: {input_json_path}")

            # JSON 파일 읽기
            df = spark.read.json(input_json_path)
            logger.info("JSON file read successfully.")

            # 필요한 열 추출
            formatted_df = df.select(
                col("prices.Date").alias("Date"),
                col("prices.Open").alias("Open"),
                col("prices.High").alias("High"),
                col("prices.Low").alias("Low"),
                col("prices.Close").alias("Close"),
                col("prices.Volume").alias("Volume")
            )

            # CSV 파일로 저장 (MinIO에 저장하는 경우)
            output_csv_path = f's3a://stock-market/{os.getenv("SPARK_APPLICATION_ARGS")}_formatted'
            logger.info(f"Saving formatted data to: {output_csv_path}")

            formatted_df.write \
                .mode("overwrite") \
                .option("header", "true") \
                .csv(output_csv_path)
            logger.info("Formatted data saved successfully.")

        except Exception as e:
            logger.error("An error occurred:", exc_info=True)

        finally:
            logger.info("Stopping Spark session...")
            spark.stop()

    app()
    os.system('kill %d' % os.getpid())