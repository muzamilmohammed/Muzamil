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
day = today.strftime("%d")      # e.g., 11

# S3 Setup
input_bucket = "datascience-input-bucket"
output_bucket = "datascience-output-bucket"
temp_prefix = "temp/"

# Load types to check
load_types = ["Full_load", "Delta_load"]

# S3 client
s3 = boto3.client('s3')

# Exact date folder pattern to search
date_suffix = f"/{year}/{month}/{day}/"

for load_type in load_types:
    input_prefix = f"csv_files/{load_type}/"
    output_prefix = f"bronze/csv_files/{load_type}/"

    # List objects for this load type
    response = s3.list_objects_v2(Bucket=input_bucket, Prefix=input_prefix)

    if 'Contents' not in response:
        print(f"ℹ️ No files found for {load_type}. Skipping.")
        continue

    # Filter only today's files
    csv_files = [
        obj['Key'] for obj in response['Contents']
        if obj['Key'].endswith(".csv") and date_suffix in obj['Key']
    ]

    if not csv_files:
        print(f"ℹ️ No CSV files for today's date in {load_type}. Skipping.")
        continue

    for key in csv_files:
        filename = os.path.basename(key)               # e.g., customers_09082025_172525.csv
        base_name = filename.replace(".csv", "")       # customers_09082025_172525
        entity_name = base_name.split("_")[0].lower()  # customers

        final_parquet_prefix = f"{output_prefix}{entity_name}/{year}/{month}/{day}/{base_name}/"

        # ✅ Check if parquet for this base_name already exists
        existing_output = s3.list_objects_v2(
            Bucket=output_bucket, Prefix=final_parquet_prefix
        )
        if 'Contents' in existing_output and any(obj['Key'].endswith(".parquet") for obj in existing_output['Contents']):
            print(f"⏩ Skipping {entity_name} ({base_name}) — already processed.")
            continue

        input_path = f"s3://{input_bucket}/{key}"
        temp_output_path = f"s3://{output_bucket}/{temp_prefix}{base_name}/"

        print(f"Processing {entity_name} from {load_type} → {input_path}")

        # Read CSV
        df = spark.read.option("header", True).csv(input_path)

        # Write as Parquet (multiple part files, no coalesce)
        df.write.mode("overwrite").parquet(temp_output_path)

        # Move all part files from temp to final parquet path
        temp_objects = s3.list_objects_v2(Bucket=output_bucket, Prefix=f"{temp_prefix}{base_name}/")
        for obj in temp_objects.get("Contents", []):
            file_key = obj["Key"]
            if file_key.endswith(".parquet"):
                file_name = os.path.basename(file_key)
                s3.copy_object(
                    Bucket=output_bucket,
                    CopySource={"Bucket": output_bucket, "Key": file_key},
                    Key=f"{final_parquet_prefix}{file_name}"
                )

        # Clean up temp directory
        for obj in temp_objects.get("Contents", []):
            s3.delete_object(Bucket=output_bucket, Key=obj["Key"])

print("✅ All CSV files for Full_load and Delta_load processed successfully (today only, no duplicates).")
