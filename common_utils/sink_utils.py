import psycopg2
import sqlalchemy
import pandas as pd
from pandas import DataFrame
from sqlalchemy.exc import ProgrammingError
from datetime import datetime
from google.cloud import bigquery

class DBHelper:
    def __init__(self, sink_config : dict, rep_config : dict, logger):
        '''
        Initilizing DBHelper class with sink and replication configs
        '''
        self.logger = logger
        logger.debug(f"\tInitializing DBHelper with:")

        self.logger.debug(f"\tSink Configs: {sink_config}")
        self.logger.debug(f"\tReplication Configs: {rep_config}")

        for key, value in sink_config.items():
            setattr(self, key, value)

        for key, value in rep_config.items():
            setattr(self, key, value)
        '''
        if self.engine == "mysql":
            self.schema = self.dbname
            self.config_schema = self.config_dbname
        '''    

    
    def create_engine(self, target_db_par: str = None):
        '''
        Creates a database engine based on the sink type (e.g., MySQL, PostgreSQL).
        Uses the default dbname from config unless a specific database is provided.
        '''

        #target_db = self.dbname

        if self.engine == 'mysql':
            self.logger.debug(f"\t\tCreating MySQL Engine for Target Database: {self.dbname}")
            return sqlalchemy.create_engine(f"{self.engine}://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}")
        
        elif self.engine == 'postgresql':
            self.logger.debug(f"\t\tCreating Postgres Engine for Target Database: {self.dbname}")
            return sqlalchemy.create_engine(f"{self.engine}://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}")
        
        elif self.engine == 'oracle+oracledb':
            return sqlalchemy.create_engine(f"{self.engine}://{self.user}:{self.password}@{self.host}:{self.port}/?service_name={self.self.dbname}")
        
        elif self.engine == 'mssql+pymssql':
            self.logger.debug(f"\t\tCreating mssql+pymssql Engine for Target Database: {self.dbname}")
            return sqlalchemy.create_engine(f"{self.engine}://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}")
        
        elif self.engine == 'bigquery':
            self.logger.debug(f"\t\tCreating BigQuery Engine for Target Project : {self.project_id} & Target Dataset : {self.dataset_id}")
            return bigquery.Client(project=self.project_id)

        else:
            print("extend above if condition for other databases")
            self.logger.debug(f"\t\tUnsupported engine: {self.engine}")
            raise ValueError(f"Unsupported engine: {self.engine}")

    def db_execute(self, query, target_db_par: str = None):
        '''
        Runs a SQL query and commits it to the specified database (or default from config if none provided).
        Ensures a commit happens even for schema changes.
        '''

        engine = self.create_engine()

        if self.engine == 'bigquery':

            self.logger.debug(f"\t\tExecuting BigQuery Query: {query}")
            query_job  = engine.query(query)
            query_job.result()  # Wait for the job to complete
            self.logger.debug(f"\t\tQuery Completed Successfully")

        elif self.engine in ['mysql', 'postgresql', 'oracle+oracledb', 'mssql+pymssql']:
            #sqlalchemy based database engines
            #works with relational databases
            with engine.connect() as conn:
            #    conn.execute(sqlalchemy.text("COMMIT;"))
                conn.execute(sqlalchemy.text(query))  # Use sqlalchemy.text() for raw SQL execution
                conn.commit()

    '''
    def Create_DB(self,db_name : str):
       # Creates Database in appropriate sink such as postgresql,mysql,..
        if self.engine == "postgresql" or self.engine == "mysql" :

            # Create the database if it does not exist
            try:
                self.logger.debug(f"\t\tTRYING DATABASE CREATION : {db_name}")
                self.db_execute(f"CREATE DATABASE {db_name}")
                self.logger.debug(f"Created DataBase : {db_name}")
            except ProgrammingError:
                self.logger.debug(f"\t\tDataBase : {db_name} already exists, or some error was encountered while creating it.")
        else:
            print(f"Creating Databases not implemented yet for {self.engine}")
            return False
    '''
    def create_checkpoint_table(self):
        '''
        This function will create db, schema, checkpoint table  based on replication config config_dbname, config_schema, config_table
        '''
        #self.Create_DB(self.config_dbname) # We should not create the DB id it does not exists.

        if self.engine == "postgresql":
            #self.logger.debug(f"\t\tTrying to create config schema : {self.config_schema}")
            #create_schema_query = f"create schema if not exists {self.config_schema};"
            #self.db_execute(create_schema_query, self.config_dbname)
            create_table_query=f"""CREATE TABLE if not exists {self.config_schema}.{self.config_table} (
                            trg_tbl_nm VARCHAR(255) PRIMARY KEY,
                            last_fetch_ts TIMESTAMP WITH TIME ZONE DEFAULT '0001-01-01 00:00:00 UTC' -- Oldest possible timestamp
                            );"""
            self.logger.debug(f"\t\tTrying to create config table :  {self.config_dbname}.{self.config_table}")
            self.db_execute(create_table_query,self.config_dbname)

        elif self.engine == "mysql":
            create_table_query = f"""CREATE TABLE if not exists {self.config_dbname}.{self.config_table} (
                                        trg_tbl_nm VARCHAR(255) PRIMARY KEY,
                                        last_fetch_ts TIMESTAMP DEFAULT '1970-01-01 00:00:01' -- Oldest possible timestamp
                                        );"""
            self.logger.debug(f"\t\tTrying to create config table :  {self.config_dbname}.{self.config_table}")
            self.db_execute(create_table_query, self.config_dbname)
        
        elif self.engine == "bigquery":
            create_table_query = f"""CREATE TABLE if not exists `{self.project_id}.{self.config_dbname}.{self.config_table}` (
                            trg_tbl_nm STRING,
                            last_fetch_ts TIMESTAMP
                            );"""
            self.logger.debug(f"\t\tTrying to create config table :  `{self.project_id}.{self.config_dbname}.{self.config_table}` ")
            self.db_execute(create_table_query)

    def last_fetch_ts(self, table_name : str):
        '''
        Retrieves the last fetch timestamp for a given table from the checkpoint table.
        Returns a default old timestamp if the table or checkpoint doesn’t exist.
        '''
        try:

            if self.engine in ['mysql', 'postgresql', 'oracle+oracledb', 'mssql+pymssql']:

                query = f"select last_fetch_ts from {self.config_dbname}.{self.config_table} where trg_tbl_nm='{table_name}'"
                engine = self.create_engine(self.config_dbname)
                with engine.connect() as conn:
                    last_ts=conn.execute(sqlalchemy.text(query)).fetchone()
                    self.logger.debug(f"last_ts : {last_ts[0]}")
                return last_ts[0]
            
            elif self.engine == 'bigquery':
                query = f"SELECT last_fetch_ts FROM `{self.project_id}.{self.config_dbname}.{self.config_table}` WHERE trg_tbl_nm = '{table_name}'"
                client = self.create_engine()
                rows = client.query(query).result()
                for row in rows:
                    return row['last_fetch_ts']
            
        except ProgrammingError:
            self.logger.debug(f"{self.config_dbname} {self.config_dbname}.{self.config_table} does not exist")
            # Return oldest value for Full Load
            return datetime(1900, 1, 1, tzinfo=datetime.timezone.utc)

    def update_timestamp(self,table : str ,last_fetch_ts : datetime):
        try:
            #self.logger.debug(f"\t\tUpdating Timestamp for Table: {update_chek_query}")
            if self.engine=='postgresql':
                update_chek_query = f"""INSERT INTO {self.config_dbname}.{self.config_table} (trg_tbl_nm, last_fetch_ts) 
                VALUES ('{table}', '{last_fetch_ts}')
                ON CONFLICT (trg_tbl_nm) DO UPDATE 
                SET last_fetch_ts = EXCLUDED.last_fetch_ts;"""
                self.db_execute(update_chek_query)

            elif self.engine=='mysql':
                update_chek_query = f"""
                        INSERT INTO {self.config_dbname}.{self.config_table} (trg_tbl_nm, last_fetch_ts) 
                        VALUES ('{table}', '{last_fetch_ts}')
                        ON DUPLICATE KEY UPDATE 
                        last_fetch_ts = VALUES(last_fetch_ts);
                    """
                self.db_execute(update_chek_query)

            elif self.engine == 'bigquery':
                update_chek_query = f"""WITH new_checkpoint AS ( SELECT '{table}' as trg_tbl_nm, '{last_fetch_ts}' as last_fetch_ts)
                MERGE INTO `{self.project_id}.{self.config_dbname}.{self.config_table}` 
                USING new_checkpoint
                ON trg_tbl_nm = new_checkpoint.trg_tbl_nm
                WHEN MATCHED THEN
                UPDATE SET last_fetch_ts = new_checkpoint.last_fetch_ts
                WHEN NOT MATCHED THEN
                INSERT (trg_tbl_nm, last_fetch_ts) VALUES (new_checkpoint.trg_tbl_nm, new_checkpoint.last_fetch_ts);
                """
                self.db_execute(update_chek_query)


        except Exception as e:
            self.logger.debug(f" Error while updating timestamp : {e}")

    def flush_table(self, sink_table : str):

        '''
        Creates Schema and drop table if exists
        '''
        if self.engine in ['mysql', 'postgresql', 'oracle+oracledb', 'mssql+pymssql']:
            drop_query = f"DROP TABLE IF EXISTS {self.dbname}.{self.sink_table};"

        elif self.engine == 'bigquery':
            drop_query = f"DROP TABLE IF EXISTS {self.project_id}.{self.dataset_id}.{self.sink_table};"


        self.logger.debug(f"\t\tFlushing Table: {sink_table}")
        self.db_execute(drop_query,self.sink_db)


    def pd_insert_into_table(self, table_name : str , records : list ):
        '''
        1. converts records into pandas dataframe and writes to provided table name
        2. update the timestamp of the table to current time
        3. If success returns True else False.
        '''

        self.logger.debug(f"\t\tStarting Insertion of : {len(records)} rows...")

        data = pd.DataFrame(records)

        engine = self.create_engine()

        try:

            if self.engine == 'bigquery':

                client = engine

                table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
                job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", create_disposition = "CREATE_IF_NEEDED") # Table will be created if not exists and data will be appended
                job = client.load_table_from_dataframe(data, table_id, job_config=job_config)
                job.result() # Wait for the job to complete.


            elif self.engine in ['mysql', 'postgresql', 'oracle+oracledb', 'mssql+pymssql']:

                with engine.connect() as conn:
                        data.to_sql(table_name, conn, schema=self.sink_schema, if_exists='append', index=False)
                        if len(data)>0:
                            max_last_modified_date = datetime.strptime(data['LastModifiedDate'].max(), '%Y-%m-%dT%H:%M:%S.%f%z').strftime("%Y-%m-%d %H:%M:%S") 
                            #print(type(max_last_modified_date))
                            self.update_timestamp(table_name,max_last_modified_date)
                        self.logger.debug(f"\t\tSuccesssfully Completed!")
                        return True  # Indicating success
            
        except Exception as e:
                    self.logger.debug(f"Error inserting data: {e}")
                    return False  # Indicating failure