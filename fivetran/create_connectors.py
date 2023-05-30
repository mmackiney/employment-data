import logging
import os
import requests
from requests.auth import HTTPBasicAuth

logging.basicConfig(level=logging.INFO)

API_KEY = os.getenv("FIVETRAN_API_KEY")
API_SECRET = os.getenv("FIVETRAN_API_SECRET")
GROUP_ID = os.getenv("FIVETRAN_GROUP_ID")
S3_ARN = os.getenv("S3_BUCKET_ARN")
SNOWFLAKE_HOST = os.getenv("SNOWFLAKE_HOST")
SNOWFLAKE_USERNAME = os.getenv("SNOWFLAKE_USERNAME")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")

auth = HTTPBasicAuth(API_KEY, API_SECRET)

groups_url = "https://api.fivetran.com/v1/groups"
params = {"limit": 1000}
response = requests.get(url=groups_url, auth=auth, params=params).json()
group_list = response["data"]["items"]
while "next_cursor" in response["data"]:
    params = {"limit": 1000, "cursor": response["data"]["next_cursor"]}
    if any(response_paged["data"]["items"]):
        group_list.extend(response_paged["data"]["items"])
group_id = group_list[0]["id"]

connectors_url = "https://api.fivetran.com/v1/connectors"

s3_body = {
    "service": "s3",
    "group_id": group_id,
    "config": {
        "schema": "s3",
        "table": "employee_data",
        "is_public": "false",
        "role_arn": S3_ARN,
        "bucket": "av-takehome-challenge",
        "prefix": "data",
        "file_type": "csv",
        "on_error": "fail",
        "append_file_option": "upsert_file",
        "empty_header": "false",
        "list_strategy": "complete_listing",
    }
}

snowflake_body = {
    "service": "snowflake_db",
    "group_id": group_id,
    "config": {
        "schema_prefix": "snowflake_db",
        "auth": "password",
        "host": SNOWFLAKE_HOST,
        "port": "443",
        "database": "EMPLOYEE_DATA",
        "user": SNOWFLAKE_USERNAME,
        "password": SNOWFLAKE_PASSWORD,
        "connectionType": "Directly",
        "passphrase": "passphrase",
        "private_key": "privateKey",
        "is_private_key_encrypted": "true",
        "role": "role",
        "update_method": "TELEPORT"
    }
}

s3_response = requests.post(url=connectors_url,auth=auth,json=s3_body).json()
snowflake_response = requests.post(url=connectors_url,auth=auth,json=snowflake_body).json()
logging.info(s3_response)
logging.info(snowflake_response)
