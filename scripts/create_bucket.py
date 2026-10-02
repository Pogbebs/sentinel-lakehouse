"""Create the lake bucket in the S3-compatible store. Safe to run repeatedly."""

import os
import sys
import time

import boto3
from botocore.exceptions import ClientError, EndpointConnectionError


def main(bucket: str) -> None:
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_SECRET_KEY"],
        region_name="us-east-1",
    )
    for attempt in range(30):  # the S3 gateway can come up a few seconds after the master
        try:
            s3.create_bucket(Bucket=bucket)
            print(f"created bucket {bucket}")
            return
        except ClientError as e:
            if e.response["Error"]["Code"] in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
                print(f"bucket {bucket} already exists")
                return
            raise
        except EndpointConnectionError:
            print(f"waiting for S3 endpoint (attempt {attempt + 1})")
            time.sleep(2)
    sys.exit("S3 endpoint never became reachable")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "lake")
