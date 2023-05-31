import logging
import os
import requests
from requests.auth import HTTPBasicAuth

logging.basicConfig(level=logging.INFO)

API_KEY = os.getenv("FIVETRAN_API_KEY")
API_SECRET = os.getenv("FIVETRAN_API_SECRET")
GROUP_ID = os.getenv("FIVETRAN_GROUP_ID")
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
    response_paged = requests.get(url=groups_url, auth=auth, params=params).json()
    if any(response_paged["data"]["items"]):
        group_list.extend(response_paged["data"]["items"])
group_id = group_list[0]["id"]

destination_url = "https://api.fivetran.com/v1/destinations"

snowflake_body = {
    "service": "snowflake",
    "group_id": group_id,
    "region": "AWS_US_EAST_1",
    "time_zone_offset":"-5",
    "config": {
        "auth": "PASSWORD",
        "host": SNOWFLAKE_HOST,
        "port": "443",
        "database": "FIVETRAN",
        "user": SNOWFLAKE_USERNAME,
        "password": SNOWFLAKE_PASSWORD,
    }
}

snowflake_response = requests.post(url=destination_url,auth=auth,json=snowflake_body).json()
logging.info(snowflake_response)
