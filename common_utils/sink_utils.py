import psycopg2
import sqlalchemy
import pandas as pd
from pandas import DataFrame
from sqlalchemy.exc import ProgrammingError
import datetime

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


    def create_update_checkpoint(self):
        '''
        This function will create db, schema, checkpoint table  based on replication config config_dbname, config_schema, config_table
        '''
        self.Create_DB(self.config_dbname)
        create_schema_query = f"create schema if not exists {self.config_schema};"
        self.logger.debug(f"\t\tTrying to create config schema : {self.config_schema}")
        self.db_execute(create_schema_query, self.config_dbname)
        create_table_query=f"""CREATE TABLE if not exists {self.config_schema}.{self.config_table} (
                        trg_tbl_nm VARCHAR(255) PRIMARY KEY,
                        last_fetch_ts TIMESTAMP WITH TIME ZONE DEFAULT '0001-01-01 00:00:00 UTC' -- Oldest possible timestamp
                        );"""
        self.logger.debug(f"\t\tTrying to create config table : {self.config_table}")
        self.db_execute(create_table_query,self.config_dbname)



    def last_fetch_ts(self, table_name : str):
        '''
        Retrieves the last fetch timestamp for a given table from the checkpoint table.
        Returns a default old timestamp if the table or checkpoint doesn’t exist.
        '''
        try:
            query = f"select last_fetch_ts from {self.config_schema}.{self.config_table} where trg_tbl_nm='{table_name}_test'"
            engine = self.create_engine(self.config_dbname)
            with engine.connect() as conn:
                last_ts=conn.execute(sqlalchemy.text(query)).fetchone()
                self.logger.debug(f"last_ts : {last_ts[0]}")
            return last_ts[0]
        except ProgrammingError:
            self.logger.debug(f"{self.config_dbname} {self.config_schema}.{self.config_table} does not exist")
            # Return oldest value for Full Load
            return datetime.datetime(1900, 1, 1, tzinfo=datetime.timezone.utc)

    def update_timestamp(self,table : str ,last_fetch_ts : datetime.datetime):
        try:
            update_chek_query = f"""INSERT INTO {self.config_schema}.{self.config_table} (trg_tbl_nm, last_fetch_ts) 
                VALUES ('{table}', '{last_fetch_ts}')
                ON CONFLICT (trg_tbl_nm) DO UPDATE 
                SET last_fetch_ts = EXCLUDED.last_fetch_ts;"""
            self.db_execute(update_chek_query,self.config_dbname)
        except ProgrammingError:
            self.logger.debug(f" {self.config_schema}.{self.config_table} not exist")

    def create_engine(self, target_db_par: str = None) -> sqlalchemy.engine.base.Connection:
        '''
        Creates a database engine based on the sink type (e.g., MySQL, PostgreSQL).
        Uses the default dbname from config unless a specific database is provided.
        '''
        target_db = self.dbname
        if target_db_par is not None:
            target_db = target_db_par
        if self.engine == 'mysql':
            self.logger.debug(f"\t\tCreating MySQL Engine for Target Database: {target_db}")
            return sqlalchemy.create_engine(f"{self.engine}://{self.user}:{self.password}@{self.host}:{self.port}/{target_db}")
        elif self.engine == 'postgresql':
            self.logger.debug(f"\t\tCreating Postgres Engine for Target Database: {target_db}")
            return sqlalchemy.create_engine(f"{self.engine}://{self.user}:{self.password}@{self.host}:{self.port}/{target_db}")
        elif self.engine == 'oracle+oracledb':
            return sqlalchemy.create_engine(f"{self.engine}://{self.user}:{self.password}@{self.host}:{self.port}/?service_name={self.service_name}")
        elif self.engine == 'mssql+pymssql':
            self.logger.debug(f"\t\tCreating mssql+pymssql Engine for Target Database: {target_db}")
            return sqlalchemy.create_engine(f"{self.engine}://{self.user}:{self.password}@{self.host}:{self.port}/{target_db}")
        else:
            print("extend above if condition for other databases")
            self.logger.debug(f"\t\tUnsupported engine: {self.engine}")
            raise ValueError(f"Unsupported engine: {self.engine}")

    def db_execute(self, query, target_db_par: str = None):
        '''
        Runs a SQL query and commits it to the specified database (or default from config if none provided).
        Ensures a commit happens even for schema changes.
        '''
        db_name = self.dbname
        if target_db_par is not None:
            db_name = target_db_par
        engine = self.create_engine(db_name)
        with engine.connect() as conn:
            conn.execute(sqlalchemy.text("COMMIT;"))
            conn.execute(sqlalchemy.text(query))  # Use sqlalchemy.text() for raw SQL execution
            conn.commit()

    def Create_DB(self,db_name : str):
        '''
        Creates Database in appropriate sink such as postgresql,mysql,..
        '''
        if self.engine=="postgresql":
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

    def flush_table(self, table_name):  # WORKING ON RN
        '''
        Creates Schema and drop table if exists
        '''
        self.Create_DB(self.sink_db)
        create_schema_query = f"create schema if not exists {self.sink_schema};"
        drop_query = f"DROP TABLE IF EXISTS {self.sink_schema}.{table_name} CASCADE;"
        self.db_execute(create_schema_query,self.sink_db)
        self.db_execute(drop_query,self.sink_db)


    def pd_insert_into_table(self, table_name : list , records : list ):
        '''
        1. converts records into pandas dataframe and writes to provided table name
        2. update the timestamp of the table to current time
        3. If success returns True else False.
        '''
        self.logger.debug(f"\t\tStarting Insertion of : {len(records)} rows...")
        engine = self.create_engine(self.sink_db)
        with engine.connect() as conn:
            try:
                data = pd.DataFrame(records)
                data.to_sql(table_name, conn, schema=self.sink_schema, if_exists='append', index=False)
                self.update_timestamp(table_name,datetime.datetime.now())
                self.logger.debug(f"\t\tSuccesssfully Completed!")
                return True  # Indicating success
            except Exception as e:
                print(f"Error inserting data: {e}")
                return False  # Indicating failure


