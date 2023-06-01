import logging
import os

import requests
from requests.auth import HTTPBasicAuth

logging.basicConfig(level=logging.INFO)

API_KEY = os.getenv("FIVETRAN_API_KEY")
API_SECRET = os.getenv("FIVETRAN_API_SECRET")
GROUP_ID = os.getenv("FIVETRAN_GROUP_ID")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_PREFIX = os.getenv("S3_PREFIX")
S3_ARN = os.getenv("S3_BUCKET_ARN")

auth = HTTPBasicAuth(API_KEY, API_SECRET)

groups_url = "https://api.fivetran.com/v1/groups"
params = {"limit": 1000}
response = requests.get(url=groups_url, auth=auth, params=params).json()
group_list = response["data"]["items"]
while "next_cursor" in response["data"]:
    params = {"limit": 1000, "cursor": response["data"]["next_cursor"]}
    response_paged = requests.get(url=groups_url, auth=auth, params=params).json()
    if any(response_paged["data"]["items"]):
        group_list.extend(response_paged["data"]["items"])
group_id = group_list[0]["id"]

connector_url = "https://api.fivetran.com/v1/connectors"

tables = [
    "departments",
    "departures",
    "dept_emp",
    "dept_manager",
    "employees",
    "salaries",
    "titles",
]
for table in tables:
    body = {
        "service": "s3",
        "group_id": group_id,
        "config": {
            "schema": "raw",
            "table": table,
            "is_public": "false",
            "role_arn": S3_ARN,
            "bucket": S3_BUCKET,
            "prefix": S3_PREFIX,
            "pattern": f"{table}.csv",
            "file_type": "csv",
            "on_error": "fail",
            "append_file_option": "upsert_file",
            "empty_header": "false",
            "list_strategy": "complete_listing",
        }
    }
    s3_response = requests.post(url=connector_url, auth=auth, json=body).json()
    logging.info(s3_response)
