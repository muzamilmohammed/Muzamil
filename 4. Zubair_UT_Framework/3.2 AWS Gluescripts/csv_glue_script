import sys
import boto3
import os
from datetime import datetime
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
# Date for folder structure
today = datetime.today()
year = today.strftime("%Y")
month = today.strftime("%b")
day = today.strftime("%d")
# S3 Setup
input_bucket = "datascience-input-bucket"
output_bucket = "datascience-output-bucket"
input_prefix = f"csv_files/Full_load/{year}/{month}/{day}/"
output_prefix = f"bronze/csv_files/{year}/{month}/{day}/"
temp_prefix = f"temp/"
# S3 client
s3 = boto3.client('s3')
# List CSV files in input path
response = s3.list_objects_v2(Bucket=input_bucket, Prefix=input_prefix)
csv_files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith(".csv")]
for key in csv_files:
    filename = os.path.basename(key)                    # e.g., Orders_20250428.csv
    base_name = filename.replace(".csv", "")            # e.g., Orders_20250428
    input_path = f"s3://{input_bucket}/{key}"
    temp_output_path = f"s3://{output_bucket}/{temp_prefix}{base_name}/"
    final_parquet_key = f"{output_prefix}{base_name}.parquet"
    # Read CSV
    df = spark.read.option("header", True).csv(input_path)
    # Write as single Parquet file to temp location
    df.coalesce(1).write.mode("overwrite").parquet(temp_output_path)
    # Find the part file in temp output
    temp_objects = s3.list_objects_v2(Bucket=output_bucket, Prefix=f"{temp_prefix}{base_name}/")
    parquet_key = None
    for obj in temp_objects.get("Contents", []):
        if obj["Key"].endswith(".parquet"):
            parquet_key = obj["Key"]
            break
    # Copy to final destination with original filename
    if parquet_key:
        s3.copy_object(
            Bucket=output_bucket,
            CopySource={"Bucket": output_bucket, "Key": parquet_key},
            Key=final_parquet_key
        )
    # Clean up temp directory
    for obj in temp_objects.get("Contents", []):
        s3.delete_object(Bucket=output_bucket, Key=obj["Key"])
print(" All CSV files converted to Parquet and saved.")
