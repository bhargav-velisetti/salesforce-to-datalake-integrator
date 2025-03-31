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
import logging
import sqlalchemy
import re

def act_rep_tbl_conf(dbhelper, repl_config : dict, table_id, repl_schema, input_tbl_id_conf, src_tbl_nm,trg_schema, trg_tbl_nm, isenabled, incr_col, column_list):
    ctrlTable = repl_config['control_table']
    configTable = repl_config['config_table']

    """SQL1 = f"DELETE FROM {repl_schema}.ing_tbl_list  WHERE TABLE_ID='{table_id}'"
    ARG1 = {}

    SQL2 = f"DELETE FROM {repl_schema}.ing_tbl_columns WHERE TABLE_ID=:table_id"
    ARG2 = {'table_id' : table_id}

    logger.debug(f'Executing the SQL: {SQL2} ARG = {ARG2}')
    dbhelper.db_execute(SQL2, ARG2)

    "EXECUTE ABOVE SQL"
    "Make call to sfdc and get Stage table defination"
    logger.debug(f'Executing the SQL: {SQL2}')
    """
    # Delete row from
    SQL1 = f"DELETE FROM {repl_schema}.{ctrlTable}  WHERE TABLE_ID='{table_id}'"
    dbhelper.db_execute(SQL1)

    SQL2 = f"""INSERT INTO {repl_schema}.{ctrlTable} (table_id,src_tbl_nm,trg_schema,trg_tbl_nm,isenabled,incr_col)
            VALUES (:table_id,:src_tbl_nm,:trg_schema,:trg_tbl_nm, :isenabled,:incr_col)"""
    ARG2 = {
        'table_id'   : table_id,
        'src_tbl_nm' : src_tbl_nm,
        'trg_schema' : trg_schema,
        'trg_tbl_nm' : trg_tbl_nm,
        'isenabled'  : True,
        'incr_col'   : incr_col
    }
    dbhelper.db_execute(SQL2, arg=ARG2)

    # Load Columns from the Replication config query
    SQL3 = f"DELETE FROM {repl_schema}.{configTable}  WHERE TABLE_ID='{table_id}'"
    dbhelper.db_execute(SQL3)
    for col in column_list:

        ARG4 = {
            'table_id': table_id,
            'src_tbl_schema': 'null',
            'src_tbl_nm': src_tbl_nm,
            'src_tbl_col': col,
            'src_tbl_col_type': 'TEXT',
            'trg_tbl_schema': trg_schema,
            'trg_tbl_nm': trg_tbl_nm,
            'trg_tbl_col': col,
            'trg_tbl_col_type': 'TEXT'
        }

        # Insert Each column detail one by one
        SQL4 = f"""INSERT INTO {repl_schema}.{configTable} (table_id,src_tbl_schema,src_tbl_nm,src_tbl_col,src_tbl_col_type,trg_tbl_schema,trg_tbl_nm,trg_tbl_col,trg_tbl_col_type)
            VALUES (:table_id,:src_tbl_schema,:src_tbl_nm,:src_tbl_col,:src_tbl_col_type,:trg_tbl_schema,:trg_tbl_nm,:trg_tbl_col,:trg_tbl_col_type)"""
        dbhelper.db_execute(SQL4, arg=ARG4)


def check_table_exist(dbhelper, repl_schema, table_name) -> bool:
    exists = False
    if getattr(dbhelper, 'engine') == 'postgresql':
        sql1 = f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = '{repl_schema}' AND table_name = '{table_name}');"
        logger.debug(f'Executing the SQL: {sql1}')
        engine = dbhelper.create_engine()
        with engine.connect() as conn:
            cursor = conn.execute(sqlalchemy.text(sql1))
            exists = cursor.fetchone()[0]
            logger.debug(f"Query response: {exists}, {type(exists)}")
    return exists

def verify_replication_tables(dbhelper, repl_config : dict):
    replSchema = repl_config['config_schema']
    ctrlTable = repl_config['control_table']
    configTable = repl_config['config_table']

    # Verify Table List Table
    try:
        ctrlExists = check_table_exist(dbhelper, replSchema, ctrlTable)
        if not ctrlExists:
            print(f"Control Table doesn't Exist {replSchema}.{ctrlTable}, Trying Creation")
            sql1 = f"""CREATE TABLE {replSchema}.{ctrlTable} (
                    table_id VARCHAR(255) PRIMARY KEY,
                    src_tbl_nm VARCHAR(255) NOT NULL,
                    trg_schema VARCHAR(255) NOT NULL,
                    trg_tbl_nm VARCHAR(255) NOT NULL,
                    isenabled BOOLEAN DEFAULT TRUE,
                    incr_col VARCHAR(255)
                    );"""
            dbhelper.db_execute(sql1)
        else:
            print(f"Control table {replSchema}.{ctrlTable} verified Successfully!")

        # Verify Columns List table
        configExists = check_table_exist(dbhelper, replSchema, configTable)
        if not configExists:
            print(f"Config Table doesn't exist: {replSchema}.{configTable}, Trying Creation")
            sql2 = f"""CREATE TABLE {replSchema}.{configTable} (
                    table_id VARCHAR(255) NOT NULL, -- cant be PK cuz multiple cols per table will share table_id
                    src_tbl_schema VARCHAR(255), 
                    src_tbl_nm VARCHAR(255),
                    src_tbl_col VARCHAR(255),
                    src_tbl_col_type VARCHAR(255),
                    trg_tbl_schema VARCHAR(255),   -- for now hardcode it to sfdc_edw. but we will come up with the flag in the replication.yaml file later 
                    trg_tbl_nm VARCHAR(255),
                    trg_tbl_col VARCHAR(255),
                    trg_tbl_col_type VARCHAR(50)
                    );"""
            dbhelper.db_execute(sql2)
        else:
            print(f"Config table {replSchema}.{configTable} verified Successfully!")
        return True
    except Exception as e:
        print(f"Error Verifying Replication Tables: {e}")
        return False

def main(config : dict, input_table_id : str,logger):
    sink_config = get_config( config['sink_config'] )
    repl_config = get_config(config['replication_config'])
    repl_db     = repl_config['config_dbname']
    repl_schema = repl_config['config_schema']
    logger.debug("Sink Config: ", sink_config)
    logger.debug("Replication config: ", repl_config)

    # Initializing DBHelper class for sink database
    dbhelper = DBHelper(sink_config, repl_config, logger)
    if verify_replication_tables(dbhelper, repl_config) is False:
        raise ValueError

    if input_table_id is None:
        raise ValueError
    
    elif input_table_id is not None and input_table_id != 'all':

        table_id = input_table_id
        input_tbl_id_conf =  config['tables'][input_table_id]
        src_tbl_nm = input_tbl_id_conf['sfdc_table']
        trg_schema = input_tbl_id_conf['sink_schema']
        trg_tbl_nm = input_tbl_id_conf['sink_table']
        isenabled  = True
        incr_col   = input_tbl_id_conf['replication_key']
        query = config['tables'][table_id]['query']
        columns = re.findall(r"SELECT\s+(.*?)\s+FROM", query, re.S)
        column_list = [col.strip() for col in columns[0].split(",")]

        act_rep_tbl_conf(dbhelper,repl_config=repl_config,table_id=table_id, repl_schema=repl_schema,input_tbl_id_conf=input_tbl_id_conf, src_tbl_nm=src_tbl_nm,
                    trg_schema=trg_schema, trg_tbl_nm=trg_tbl_nm, isenabled=isenabled, incr_col=incr_col,column_list=column_list)
       
    elif input_table_id.lower() == 'all':
        for table_id in config['tables']:
            logger.debug('Processing table: ', table_id)
            table_id = table_id
            input_tbl_id_conf =  config['tables'][table_id]
            src_tbl_nm = input_tbl_id_conf['sfdc_table']
            trg_schema = input_tbl_id_conf['sink_schema']
            trg_tbl_nm = input_tbl_id_conf['sink_table']
            isenabled  = True
            incr_col   = input_tbl_id_conf['replication_key']
            query = config['tables'][table_id]['query']
            columns = re.findall(r"SELECT\s+(.*?)\s+FROM", query, re.S)
            column_list = [col.strip() for col in columns[0].split(",")]
            act_rep_tbl_conf(dbhelper,repl_config=repl_config,table_id=table_id, repl_schema=repl_schema,input_tbl_id_conf=input_tbl_id_conf, src_tbl_nm=src_tbl_nm,
                             trg_schema=trg_schema, trg_tbl_nm=trg_tbl_nm, isenabled=isenabled, incr_col=incr_col, column_list=column_list)

  
if __name__ == '__main__':

    repl_conf_path, input_table_id = parse_args()

    logger = get_logger('stdout_logger')
    logger.debug(f"Replication Config file path: {repl_conf_path}")

    with open(repl_conf_path, 'r') as f:
        config = yaml.load(f, Loader=yaml.SafeLoader)
        main(config, input_table_id, logger)
