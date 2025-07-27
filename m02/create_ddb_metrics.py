import boto3
import time
import argparse
import uuid
import os

# Config
TABLE_NAME = os.environ.get("DYNAMODB_TABLE", "surveysMain")
PARTITION_KEY = "pk"
SORT_KEY = "sk"

# Initialize DynamoDB
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(TABLE_NAME)


def create_metrics(duration):
    print("Starting to generate DynamoDB metrics")
    start = time.time()
    item_id = str(uuid.uuid4())
    pk = f"TEMP#{item_id}"
    sk = "REPEATED_METRICS"
    while time.time() - start < duration:
        time.sleep(1)
        table.put_item(Item={PARTITION_KEY: pk, SORT_KEY: sk})
        table.get_item(Key={PARTITION_KEY: pk, SORT_KEY: sk})
        time.sleep(1)
        table.get_item(Key={PARTITION_KEY: pk, SORT_KEY: sk})
        table.delete_item(Key={PARTITION_KEY: pk, SORT_KEY: sk})


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--duration",
        type=int,
        default=60,
        help="Seconds to run metrics generation script."
    )
    args = parser.parse_args()
    create_metrics(args.duration)
