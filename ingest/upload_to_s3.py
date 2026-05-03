import boto3
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BUCKET = "ec-dwh-main"
RAW_PREFIX = 'raw/olist'
LOCAL_DIR  = Path('./.data/raw')

def upload_to_s3():
    s3 = boto3.client(
        "s3",
        aws_access_key_id = os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name = os.environ["AWS_DEFAULT_REGION"]
    )

    csv_files = list(LOCAL_DIR.glob("*.csv"))
    print(f"{len(csv_files)} files uploads ...")
    for csv in csv_files:
        key = f"{RAW_PREFIX}/{csv.name}"
        s3.upload_file(str(csv), BUCKET, key)
        print(f"[Sucsess] Upload {key}")
    print("Upload done.")

if __name__ == '__main__':
    upload_to_s3()