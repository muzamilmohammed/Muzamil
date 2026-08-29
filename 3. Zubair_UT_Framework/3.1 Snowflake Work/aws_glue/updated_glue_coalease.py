import sys
import boto3
import os
from datetime import datetime
from awsglue.context import GlueContext
from pyspark.context import SparkContext

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Date for folder structure
today = datetime.today()
year = today.strftime("%Y")     # e.g., 2025
month = today.strftime("%b")    # e.g., Aug
day = today.strftime("%d")      # e.g., 09

# S3 Setup
input_bucket = "datascience-input-bucket"
output_bucket = "datascience-output-bucket"
input_prefix = f"csv_files/Full_load/"
output_prefix = f"bronze/csv_files/Full_load/"
temp_prefix = "temp/"

# S3 client
s3 = boto3.client('s3')

# 1️⃣ List all CSV files from input bucket for today's date
response = s3.list_objects_v2(Bucket=input_bucket, Prefix=input_prefix)

if 'Contents' not in response:
    print("❌ No files found in input bucket.")
    sys.exit(0)

csv_files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith(".csv")]

for key in csv_files:
    # Example key before change:
    # csv_files/Full_load/2025/Aug/09/customers_09082025_172525.csv

    filename = os.path.basename(key)               # customers_09082025_172525.csv
    base_name = filename.replace(".csv", "")       # customers_09082025_172525
    entity_name = base_name.split("_")[0].lower()  # customers

    # ✅ New input and output folder pattern
    # Input:  csv_files/Full_load/customers/YYYY/MMM/DD/<filename>
    # Output: bronze/csv_files/Full_load/customers/YYYY/MMM/DD/<filename>.parquet
    input_path = f"s3://{input_bucket}/{key}"
    temp_output_path = f"s3://{output_bucket}/{temp_prefix}{base_name}/"
    final_parquet_key = f"{output_prefix}{entity_name}/{year}/{month}/{day}/{base_name}.parquet"

    print(f"Processing {entity_name} from {input_path}")

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

    # Copy parquet to final destination with correct folder structure
    if parquet_key:
        s3.copy_object(
            Bucket=output_bucket,
            CopySource={"Bucket": output_bucket, "Key": parquet_key},
            Key=final_parquet_key
        )

    # Clean up temp directory
    for obj in temp_objects.get("Contents", []):
        s3.delete_object(Bucket=output_bucket, Key=obj["Key"])

print("✅ All CSV files converted to Parquet with folder structure.")
