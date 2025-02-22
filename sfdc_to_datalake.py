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



def main( config : dict, logger ):

    sfdc_config = get_config( config['sfdc_config'] )
    sink_config = get_config( config['sink_config'] )

    print('type is => ' , sink_config['dbname'])

    logger.debug(f"SFDC Config: {sfdc_config}")
    logger.debug(f"Sink Config: {sink_config}")


    for table in config['tables']:

        bearer_token , instance_url = get_bearertoken_and_instanceurl(**sfdc_config)

        logger.debug(f"Bearer Token: {bearer_token}")
        logger.debug(f"Instance URL: {instance_url}")
        logger.debug(f"Table: {table}")

        sfdc_table = config['tables'][table]['sfdc_table']
        sink_db=f"{config['tables'][table]['sink_dbname']}"
        sink_schema = f"{config['tables'][table]['sink_schema']}"  # NEW VARIABLE
        sink_table = f"{config['tables'][table]['sink_table']}"     # REMOVED SCHEMA NAME FROM TABLENAME
        query = config['tables'][table]['query']
        replication_type = config['tables'][table]['replication_type']
        logger.debug(f"sink Schema : {sink_schema}")
        logger.debug(f"sink Table : {sink_table}")
        logger.debug(f"Query : {query}")

        sink_config["sink_db"]= sink_db
        sink_config["sink_schema"] =sink_schema
        sink_config["sink_table"] =sink_table
        dbhelper = DBHelper(sink_config,logger)

        salesforceapihelper = SlaesforceAPIHelper(instance_url, bearer_token)

        if replication_type in config['metadata']['allowed_full_replication_types']:
            
            logger.debug("Full Replication")
            logger.debug(f"Truncating the table: {sink_table}")

            dbhelper.flush_table(sink_table)

            bulk_job = salesforceapihelper.submit_sfdc_bulk_request(query).json()
            logger.debug(f"Bulk Job: {bulk_job}")
            queryJobId = bulk_job['id']
            salesforceapihelper.wait_until_bulk_job_is_completed(queryJobId)

            parallelism = 2

            resultpages = salesforceapihelper.fetch_sfdc_bulkapi_resultpages(queryJobId, parallelism)

            logger.debug(f'{queryJobId} result pages are {resultpages}')

            if isinstance(resultpages, list):
                chunkd_result_pages = chunk_list(resultpages, parallelism) # [1,2,3,4,5] -> [ [1,2], [3,4], [5]]
            else:
                # comeup with the single result page code
                chunkd_result_pages = ["single_page_url"]
            
            dbhelper.flush_table(sink_table)

            for chunk in chunkd_result_pages:

                df = salesforceapihelper.fetch_sfdc_bulkapi_results(chunk)
                logger.debug(f'Df records recieved for {chunk}: Len={len(df)}, records={df}')
                dbhelper.pd_insert_into_table(sink_table, df)

        else:
            
            print('Build Inc with regular sync api')


if __name__ == '__main__':

    repl_conf_path = parse_args()

    print(repl_conf_path)
    logger = get_logger('stdout_logger')
    logger.debug(f"Replication Config file path: {repl_conf_path}")

    with open(repl_conf_path, 'r') as f:
        config = yaml.load(f, Loader=yaml.SafeLoader)
        main(config, logger)
