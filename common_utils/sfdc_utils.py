import requests 
import pandas as pd 
from pandas import DataFrame
import requests
import json
import time 
import asyncio
import aiohttp
from aiohttp import ClientSession
import io
from common_utils.logger import get_logger



# Get the bearer token AS WELL AS instance url from salesforce
# Returns -> A tuple having structure like (token, url)
def get_bearertoken_and_instanceurl(username, password, security_token, client_id, client_secret) -> tuple:
    auth_url = "https://login.salesforce.com/services/oauth2/token"
    auth_data = {
        "grant_type":"password",
        "client_id": client_id,
        "client_secret": client_secret,
        "username": username,
        "password": password + security_token,
    }
    response = requests.post(auth_url, data=auth_data).json()
    return response['access_token'], response['instance_url']

class SlaesforceAPIHelper:
    def __init__(self, instance_url: str, bearer_token: str, api_version: str = "v62.0"):
        self.instance_url = instance_url
        self.bearer_token = bearer_token
        self.api_version = api_version

    # Extract Data from SFDC with syncronous API
    # Reccommended for Incremental Load and full load with small amouns of the data
    def fetch_data_from_sfdc_syncapi(self, query: str) -> pd.DataFrame:
        """
        Fetches all paginated data from Salesforce using the REST API.
        """
        headers = {
            "Authorization": f"Bearer {self.bearer_token}",
            "Content-Type": "application/json"
        }

        records = []
        url = f"{self.instance_url}/services/data/{self.api_version}/queryAll?q={query}"

        while url:
            response = requests.get(url, headers=headers)
            if response.status_code != 200:
                raise Exception(f"Failed to fetch data from Salesforce: {response.text}")

            response_json = response.json()
            records.extend(response_json.get("records", []))

            # Check if there's a nextRecordsUrl for pagination
            next_records_url = response_json.get("nextRecordsUrl")
            url = f"{self.instance_url}{next_records_url}" if next_records_url else None

        return pd.DataFrame(records)

    # Submit the job with BULK API 2.0
    # Use it for Full load
    def submit_sfdc_bulk_request(self, query: str):
        """
        Submits a bulk request to Salesforce using the REST API.
        """
        url = f"{self.instance_url}/services/data/{self.api_version}/jobs/query"
        body = {
                "operation": "queryAll",
                "query": query
            }
        
        headers = {
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {self.bearer_token}",
                    "Accept": "application/json"
                }

        return requests.post(url, headers=headers, json=body) 

    # Get the status of the bulk job
    def get_sfdc_bulk_job_status(self, queryJobId: str):
        """
        Get the status of the bulk job
        """
        url = f"{self.instance_url}/services/data/{self.api_version}/jobs/query/{queryJobId}"

        headers = {
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {self.bearer_token}",
                }

        return requests.get(url, headers=headers)  
    
    def wait_until_bulk_job_is_completed(self, queryJobId: str, timeout: int = 12000):
        """
        Wait until the bulk job is completed
        timeout at 20mins by default
        """
        logger = get_logger('stdout_logger')
        i = 0
        while True:
            time.sleep(60)
            i += 60
            response = self.get_sfdc_bulk_job_status(queryJobId)
            response_json = response.json()
            
            if response_json["state"] in ["Failed" , "Aborted"]:
                logger.error(f"Job failed with state: {response_json['state']}")
                raise Exception(f"Job failed with state: {response_json['state']}")
            elif response_json["state"] == "JobComplete" or i >= timeout:
                break
            # Check if we need return anything to make this logic work
    
    # Delete the Bulk jon after Initial load is completed
    def delete_sfdc_bulk_job(self, queryJobId: str):
        """
        Delete the bulk job
        """
        url = f"{self.instance_url}/services/data/{self.api_version}/jobs/query/{queryJobId}"

        headers = {
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {self.bearer_token}",
                }

        return requests.delete(url, headers=headers) 
    
    # Get BULK API Request Result Pages
    def fetch_sfdc_bulkapi_resultpages(self, queryJobId: str, parallelism : int) -> list:
        url = f"{self.instance_url}/services/data/{self.api_version}/jobs/query/{queryJobId}/resultPages"

        headers = {
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {self.bearer_token}",
                }
        req = requests.get(url=url, headers=headers).json()
        print(req)
        #requests.get(url=url, headers=headers).content.decode()['resultPages']
        return req
    
    # Make get call with aiohttp
    async def async_api_get_call(self,session: ClientSession, url: str) -> str:
        async with await session.get(url, ssl= False) as response:
            response.raise_for_status()
            return await response.text()

    async def get_chunkd_url_response(self, urls : list) -> pd.DataFrame:
        async with aiohttp.ClientSession as session:
            tasks = []
            for url in urls:
                # Creates asyncio.Task that will return a future 
                task = asyncio.create_task(
                    coro=self.async_api_get_call(
                        session=session,
                        url = url,
                    )
                )
                tasks.append(task)
            # Tasks will run with asyncio.gather 
            # return_exceptions making it True to fail all the tasks if any task fails 
            result = await asyncio.gather(*tasks, return_exceptions=True)

            dfs = []
            for i in result:
                df = pd.read_csv(io.StringIO(i))
                dfs.append(df)

            df_concat = pd.concat(dfs, ignore_index=True)

            return df_concat

        
    # Extract Data from SFDC with BULK API 2.0
    def fetch_sfdc_bulkapi_results(self, result_pages : list) -> pd.DataFrame:

        asyncio.run(self.get_chunkd_url_response(urls = result_pages)) 
