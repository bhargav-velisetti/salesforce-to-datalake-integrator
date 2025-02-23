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

def act_rep_tbl_conf(dbhealper,table_id,repl_schema, input_tbl_id_conf, src_tbl_nm,trg_schema, trg_tbl_nm, isenabled, incr_col ):

        SQL1 = f"DELETE FROM {repl_schema}.ing_tbl_list  WHERE TABLE_ID='{table_id}'"
        ARG1 = {}

        logger.debug(f'Executing the SQL: {SQL1} ARG = {ARG1}')
        dbhealper.db_execute(SQL1, ARG1)

        SQL2 = f"DELETE FROM {repl_schema}.ing_tbl_columns WHERE TABLE_ID=:table_id"
        ARG2 = {'table_id' : table_id}


        logger.debug(f'Executing the SQL: {SQL2} ARG = {ARG2}')
        dbhealper.db_execute(SQL2, ARG2)

        "EXECUTE ABOVE SQL"
        "Make call to sfdc and get Stage table defination"
        logger.debug(f'Executing the SQL: {SQL2}')

        SQL3 = f"""INSERT INTO {repl_schema}.ing_tbl_list (table_id,src_tbl_nm,trg_schema,trg_tbl_nm, isenabled,incr_col)
                VALUES (:table_id,:src_tbl_nm,:trg_schema,:trg_tbl_nm, :isenabled,:incr_col)"""
        ARG3 = {
            'table_id'   : table_id,
            'src_tbl_nm' : src_tbl_nm,
            'trg_schema' : trg_schema,
            'trg_tbl_nm' : trg_tbl_nm,
            'isenabled'  : True,
            'incr_col'   : 'LastModifiedDate'
        }
        
        dbhealper.db_execute(SQL3, ARG3)


def main(config : dict, input_table_id : str,logger):
    sink_config = get_config( config['sink_config'] )
    repl_db = config.get('replication_config').get('config_dbname')
    repl_schema = config.get('replication_config').get('config_schema')

    logger.debug(f"Replication config db = {repl_db} , schema = {repl_schema}")

    dbhealper = DBHelper(sink_config)


    if input_table_id == None:
        raise ValueError
    
    elif input_table_id is not None and input_table_id != 'all':

        table_id = input_table_id
        input_tbl_id_conf =  config['tables'][input_table_id]
        src_tbl_nm = input_tbl_id_conf['sfdc_table']
        trg_schema = input_tbl_id_conf['sink_schema']
        trg_tbl_nm = input_tbl_id_conf['sink_table']
        isenabled = True
        incr_col = input_tbl_id_conf['replication_key']

        act_rep_tbl_conf(dbhealper,table_id=table_id, repl_schema=repl_schema,input_tbl_id_conf=input_tbl_id_conf, src_tbl_nm=src_tbl_nm,
                    trg_schema=trg_schema, trg_tbl_nm=trg_tbl_nm, isenabled=isenabled, incr_col=incr_col)
       
    elif input_table_id.lower() == 'all':
        for table_id in config['tables']:

            table_id = table_id
            input_tbl_id_conf =  config['tables'][table_id]
            src_tbl_nm = input_tbl_id_conf['sfdc_table']
            trg_schema = input_tbl_id_conf['sink_schema']
            trg_tbl_nm = input_tbl_id_conf['sink_table']
            isenabled = True
            incr_col = input_tbl_id_conf['replication_key']

            act_rep_tbl_conf(dbhealper,table_id=table_id, repl_schema=repl_schema,input_tbl_id_conf=input_tbl_id_conf, src_tbl_nm=src_tbl_nm,
                             trg_schema=trg_schema, trg_tbl_nm=trg_tbl_nm, isenabled=isenabled, incr_col=incr_col)

  
if __name__ == '__main__':

    repl_conf_path, input_table_id = parse_args()

    print(input_table_id)
    print(repl_conf_path)
    logger = get_logger('stdout_logger')
    logger.debug(f"Replication Config file path: {repl_conf_path}")

    with open(repl_conf_path, 'r') as f:
        config = yaml.load(f, Loader=yaml.SafeLoader)
        main(config, input_table_id, logger)
