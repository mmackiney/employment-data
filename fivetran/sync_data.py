import logging
import os
import requests
from requests.auth import HTTPBasicAuth

logging.basicConfig(level=logging.INFO)

API_KEY = os.getenv(FIVETRAN_API_KEY)
API_SECRET = os.getenv(FIVETRAN_API_SECRET)
CONNECTOR_ID = os.getenv(FIVETRAN_CONNECTOR_ID)

auth = HTTPBasicAuth(API_KEY, API_SECRET)

url = "https://api.fivetran.com/v1/connectors/{}/force".format(CONNECTOR_ID)

response = requests.post(url=url,auth=auth).json()
logging.info(response)
