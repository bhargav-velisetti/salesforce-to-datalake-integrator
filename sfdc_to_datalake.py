import datetime
import psycopg2
import requests
import json
import yaml 
import configparser
from common_utils.helper import parse_args, get_config , chunk_list
from common_utils.logger import get_logger
from common_utils.sfdc_utils import get_bearertoken_and_instanceurl, SlaesforceAPIHelper
from common_utils.sink_utils import DBHelper
import  logging
import pandas as pd



def main( config : dict, logger ):
    '''
    Main function to orchestrate data replication from Salesforce to a sink database.
    Handles both full and incremental loads for each table in the config.
    '''

    sfdc_config,bulk_api_flag = get_config( config['sfdc_config'] )
    sink_config = get_config( config['sink_config'] )
    rep_config  = get_config( config['replication_config'] )

    logger.debug(f"SFDC Config: {sfdc_config}")
    logger.debug(f"Sink Config: {sink_config}")

    for table in config['tables']:
        # Get Salesforce credentials and initialize helpers
        bearer_token , instance_url = get_bearertoken_and_instanceurl(**sfdc_config)

        logger.debug(f"Bearer Token: {bearer_token}")
        logger.debug(f"Instance URL: {instance_url}")
        logger.debug(f"Processing Table: {table}")
        # Extract table-specific details
        sfdc_table  = config['tables'][table]['sfdc_table']
        sink_db     = f"{config['tables'][table]['sink_dbname']}"
        sink_schema = f"{config['tables'][table]['sink_schema']}"
        sink_table  = f"{config['tables'][table]['sink_table']}"
        query       = config['tables'][table]['query']
        replication_key  = config['tables'][table]['replication_key']
        replication_type = config['tables'][table]['replication_type']

        logger.debug(f"Replication Type: {replication_type}")

        # Adding Sink Database, Schema, and Table keys to sink_config dictionary
        sink_config["sink_db"]     = sink_db
        sink_config["sink_schema"] = sink_schema
        sink_config["sink_table"]  = sink_table


        # initializing DBHelper object
        dbhelper = DBHelper(sink_config, rep_config, logger)
        salesforceapihelper = SlaesforceAPIHelper(instance_url, bearer_token)

        logger.debug(f"\tChecking if Config DB exists or Not. If not Trying to create it")

        dbhelper.create_checkpoint_table()

        # Updating Query based on load type
        if replication_type in config['metadata']['allowed_full_replication_types']:
            logger.debug("Starting Full Replication")

        elif replication_type in config['metadata']['allowed_incremental_replication_types']:
            logger.debug("Starting Incremental Replication")
            # fetching the last time stamp and converting it into iso format.
            last_ts = dbhelper.last_fetch_ts(table)
            last_ts_iso = last_ts.strftime('%Y-%m-%dT%H:%M:%SZ')  # ISO format with Z for UTC
            logger.debug(f"replication_key : {replication_key}  ,  last_ts_iso  {last_ts_iso}")
            query = query + f" WHERE  {replication_key}> {last_ts_iso}"
        else:
            logger.debug("Please provide Valid ingestion type. Hint: full/incremental")
            exit()

        logger.debug(f"Using Query to fetch from salesforce : {query}")

        # Clear table for full load
        if replication_type in config['metadata']['allowed_full_replication_types']:
            dbhelper.flush_table(sink_table)

        if bulk_api_flag =="True":
            # Submit and process job with bulk api
            bulk_job = salesforceapihelper.submit_sfdc_bulk_request(query).json()
            logger.debug(f"Bulk Job: {bulk_job}")
            queryJobId = bulk_job['id']
            salesforceapihelper.wait_until_bulk_job_is_completed(queryJobId)

            parallelism = 2

            resultpages = salesforceapihelper.fetch_sfdc_bulkapi_resultpages(queryJobId, parallelism)
            logger.debug(f'\t{queryJobId} result pages are {resultpages}')

            if isinstance(resultpages, list):
                for page_url in resultpages:
                    logger.debug(f'Processing page: {page_url}')
                    df = salesforceapihelper.fetch_sfdc_bulkapi_results([page_url])
                    logger.debug(f"Inserting {len(df)} rows from page")
                    dbhelper.pd_insert_into_table(sink_table, df)
                    del df  # Optional: Explicitly free memory
            else:
                df = salesforceapihelper.fetch_sfdc_bulkapi_results(resultpages)
                dbhelper.pd_insert_into_table(sink_table, df)

        else:

            logger.debug('Starting sync api call')
            total_rows = 0
            for df_chunk in salesforceapihelper.fetch_data_from_sfdc_syncapi(query):
                chunk_size = len(df_chunk)
                total_rows += chunk_size
                logger.debug(f"Processing sync API chunk with {chunk_size} rows (total: {total_rows})")
                if not df_chunk.empty:
                    # Convert datetime fields if needed
                    if 'LastModifiedDate' in df_chunk.columns:
                        if sink_config['engine'] == "mysql":
                            df_chunk['LastModifiedDate'] = pd.to_datetime(df_chunk['LastModifiedDate'], utc = True).dt.tz_localize(None)
                        else:
                            df_chunk['LastModifiedDate'] = pd.to_datetime(df_chunk['LastModifiedDate'])
                    dbhelper.pd_insert_into_table(sink_table, df_chunk.to_dict('records'))
                    del df_chunk
            logger.debug(f"Total rows processed via sync API: {total_rows}")


if __name__ == '__main__':

    repl_conf_path = parse_args()

    logger = get_logger('stdout_logger')
    logger.debug(f"Replication Config file path: {repl_conf_path}")

    with open(repl_conf_path, 'r') as f:
        config = yaml.load(f, Loader=yaml.SafeLoader)
        import time
        start_time = time.perf_counter()

        # Start replication
        main(config, logger)

        end_time = time.perf_counter()
        elapsed_time = end_time - start_time
        print(f"Elapsed time: {elapsed_time:.4f} seconds")
