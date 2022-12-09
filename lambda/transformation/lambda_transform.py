"""Lambda function to transform raw data."""

import json

import boto3
import transform
from aws_lambda_typing import context as context_
from aws_lambda_typing import events

KEY = "transformed_key"
s3 = boto3.client("s3")


def lambda_handler(event: events.SQSEvent, context: context_.Context) -> None:
    """Gets raw data from S3, transforms, and uploads to S3 under different prefix."""
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = event["Records"][0]["s3"]["object"]["key"]

    file_elements = key.split("/")
    timestamp = file_elements[1]
    elements = file_elements[2].split(".")[0].split("_")

    script_name = elements[-1]
    node_name = "_".join(elements[:-1])

    res = {  # JSON template stored for transformed output
        "node": node_name,
        "timestamp": timestamp,
        "script_name": script_name,
        "output_filepath": key,
    }

    obj = s3.get_object(Bucket=bucket, Key=key)
    data = transform.transform_ebpf(script_name, obj, node_name)
    res["data"] = data

    file_name = node_name + "_" + script_name + ".json"
    result_filepath = KEY + timestamp + "/" + file_name

    s3.put_object(  # upload to S3 under transformed prefix
        Bucket=bucket, Key=result_filepath, Body=json.dumps(res).encode("utf-8")
    )
