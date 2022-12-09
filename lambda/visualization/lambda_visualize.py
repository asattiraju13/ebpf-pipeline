"""Lambda function to generate visualization."""

import datetime
import json
from collections import defaultdict

import boto3
import combine
import visualize
from aws_lambda_typing import context as context_
from aws_lambda_typing import events

BUCKET = "my_s3_bucket"
TRANSFORMED_KEY = "my_transformed_key"
VISUALIZATION_KEY = "my_visualization_key"

s3 = boto3.resource("s3")
s3_crud = boto3.client("s3")

# ------ EXAMPLE QUERY ------
# {
#   "date": "12-07-2022",
#   "node_ids": [
#     "diskIONode3",
#     "idleNode2",
#     "idleNode1",
#     "matrixNode"
#   ]
# }


def generate_filepath(query: events.SQSEvent) -> str:
    """Generates filepath with date, epoch naming convention."""
    timestamp = datetime.datetime.now().strftime("%s")
    file_name = f'{query["date"]}_visualization_on_epoch_{timestamp}.html'
    dashboard_filepath = f"/tmp/{file_name}"  # lambda memory
    return dashboard_filepath


def lambda_handler(query: events.SQSEvent, context: context_.Context) -> None:
    """Lambda function to generate visualization in S3."""
    bucket = s3.Bucket(BUCKET)
    prefix_objs = bucket.objects.filter(
        Prefix=f'{TRANSFORMED_KEY}/{query["date"]}/'
    )  # transformed data objects to combine
    query["scripts"] = ["biolatency", "runqlat", "syscount", "execsnoop", "biosnoop"]

    script_transformed_results = defaultdict(
        lambda: []
    )  # stores list of transformed outputs for each script name
    for obj in prefix_objs:
        if obj.key.endswith(".json"):  # extracts elements from file name
            file_name = obj.key[obj.key.rfind("/") + 1 : -5]
            elements = file_name.split("_")
            script_name = elements[-1]
            node_id = "_".join(elements[:-1])

            if node_id in query["node_ids"] and script_name in query["scripts"]:
                file_data = json.loads(obj.get()["Body"].read())
                script_transformed_results[script_name].append(file_data)

    script_combined_results = {}  # stores combined output for each script name
    for script in script_transformed_results:
        script_combined_results[script] = combine.combine_ebpf(
            script_transformed_results[script], script
        )

    dashboard_filepath = generate_filepath(query)
    visualize.visualize_ebpf(script_combined_results, dashboard_filepath)
    dashboard_dest_path = f"{VISUALIZATION_KEY}/{file_name}"  # S3 filepath

    with open(dashboard_filepath, encoding="utf-8") as file_object:
        data_string = file_object.read()
        s3_crud.put_object(
            Bucket=BUCKET,
            Key=dashboard_dest_path,
            Body=data_string,
            ContentType="text/html",
        )
