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


def log_rolling_average(logs, read_total, write_total, phase):
    # Keep only the last 10 seconds of logs
    logs = [entry for entry in logs if time.time() - entry[0] <= 10]
    if logs:
        duration = time.time() - logs[0][0]
        delta_reads = read_total - logs[0][1]
        delta_writes = write_total - logs[0][2]
        rps = delta_reads / duration if duration > 0 else 0
        wps = delta_writes / duration if duration > 0 else 0
        print(f"[{phase}] Reads: {read_total} (avg: {rps:.2f}/s) | Writes: {write_total} (avg: {wps:.2f}/s)")
    return logs


def burst_clearance_phase(burst_clear_seconds=200):
    print(f"Starting burst capacity clearance phase ({burst_clear_seconds} seconds)...\n")
    start = time.time()
    read_count = 0
    write_count = 0
    ops_log = []

    while time.time() - start < burst_clear_seconds:
        item_id = str(uuid.uuid4())
        pk = f"BURST#{item_id}"
        sk = "STATIC"

        # Write
        table.put_item(Item={PARTITION_KEY: pk, SORT_KEY: sk, "temp": "x"})
        write_count += 1

        # Read
        table.get_item(Key={PARTITION_KEY: pk, SORT_KEY: sk})
        read_count += 1

        # Second Read for Parity
        table.get_item(Key={PARTITION_KEY: pk, SORT_KEY: sk})
        read_count += 1

        # Delete (A write)
        table.delete_item(Key={PARTITION_KEY: pk, SORT_KEY: sk})
        write_count += 1

        # Log
        ops_log.append((time.time(), read_count, write_count))
        ops_log = log_rolling_average(ops_log, read_count, write_count, "Burst Phase")

    print("\nBurst clearance complete.\n")

def main_test_phase(duration):
    print(f"Starting main performance test for {duration} seconds...\n")
    start = time.time()
    read_count = 0
    write_count = 0
    ops_log = []

    item_id = str(uuid.uuid4())
    pk = f"TEST#{item_id}"
    sk = "REPEATED"
    counter_attr = "#ctr"  # alias to avoid reserved word "counter"

    while time.time() - start < duration:
        # Write
        table.put_item(Item={PARTITION_KEY: pk, SORT_KEY: sk, "counter": 0})
        write_count += 1

        # Update
        table.update_item(
            Key={PARTITION_KEY: pk, SORT_KEY: sk},
            UpdateExpression="SET #ctr = #ctr + :inc",
            ExpressionAttributeNames={counter_attr: "counter"},
            ExpressionAttributeValues={":inc": 1}
        )
        write_count += 1

        # Read
        table.get_item(Key={PARTITION_KEY: pk, SORT_KEY: sk})
        read_count += 1

        # Second read for parity
        table.get_item(Key={PARTITION_KEY: pk, SORT_KEY: sk})
        read_count += 1

        # Delete
        table.delete_item(Key={PARTITION_KEY: pk, SORT_KEY: sk})
        write_count += 1

        # Log
        ops_log.append((time.time(), read_count, write_count))
        if int(time.time()) % 5 == 0:
            ops_log = log_rolling_average(ops_log, read_count, write_count, "Main Test")

    print("\nMain test complete.")
    print(f"Total Reads: {read_count}")
    print(f"Total Writes: {write_count}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=int, default=60, help="Test duration in seconds after burst clear phase")
    parser.add_argument("--burstsecs", type=int, default=200, help="Burst clearing duration in seconds")
    args = parser.parse_args()
    burst_clearance_phase(args.burstsecs)
    main_test_phase(args.duration)
