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
        self.logger=logger
        logger.debug(f" sink_configgg : {sink_config}")
        for key, value in sink_config.items():
            setattr(self, key, value)
        logger.debug(f"replication config : {rep_config}")
        for key, value in rep_config.items():
            setattr(self, key, value)
        if self.engine == "mysql":
            self.schema = self.dbname
            self.config_schema = self.config_dbname


    def create_update_checkpoint(self):
        '''
        This function will create db, schema, checkpoint table  based on replication config config_dbname, config_schema, config_table
        '''
        self.Create_DB(self.config_dbname)
        if self.engine == "postgresql":
            create_schema_query = f"create schema if not exists {self.config_schema};"
            self.db_execute(create_schema_query, self.config_dbname)
            create_table_query=f"""CREATE TABLE if not exists {self.config_schema}.{self.config_table} (
                            trg_tbl_nm VARCHAR(255) PRIMARY KEY,
                            last_fetch_ts TIMESTAMP WITH TIME ZONE DEFAULT '0001-01-01 00:00:00 UTC' -- Oldest possible timestamp
                            );"""
            self.db_execute(create_table_query,self.config_dbname)
        elif self.engine == "mysql":
            create_table_query = f"""CREATE TABLE if not exists {self.config_dbname}.{self.config_table} (
                                        trg_tbl_nm VARCHAR(255) PRIMARY KEY,
                                        last_fetch_ts TIMESTAMP DEFAULT '1970-01-01 00:00:01' -- Oldest possible timestamp
                                        );"""
            self.db_execute(create_table_query, self.config_dbname)




    def last_fetch_ts(self,table_name : str):
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
            self.logger.debug(f"{self.config_dbname} {self.config_schema}.{self.config_table} not exist")
            # if try fails then return default time stamp
            return datetime.datetime(1900, 1, 1, tzinfo=datetime.timezone.utc)      #return oldest value or Full Load



    def update_timestamp(self,table : str ,last_fetch_ts : datetime.datetime):
        '''
        Updates the timestamp for a table in the checkpoint table after data loading completes.
        Uses an upsert to handle both new and existing entries.
        '''
        try:
            if self.engine=='postgresql':
                update_chek_query = f"""INSERT INTO {self.config_schema}.{self.config_table} (trg_tbl_nm, last_fetch_ts) 
                VALUES ('{table}', '{last_fetch_ts}')
                ON CONFLICT (trg_tbl_nm) DO UPDATE 
                SET last_fetch_ts = EXCLUDED.last_fetch_ts;"""
            elif self.engine=='mysql':
                update_chek_query = f"""
                        INSERT INTO {self.config_schema}.{self.config_table} (trg_tbl_nm, last_fetch_ts) 
                        VALUES ('{table}', '{last_fetch_ts}')
                        ON DUPLICATE KEY UPDATE 
                        last_fetch_ts = VALUES(last_fetch_ts);
                    """
            self.db_execute(update_chek_query,self.config_dbname)
        except ProgrammingError:
            self.logger.debug(f" {self.config_schema}.{self.config_table} not exist")

    def create_engine(self, db_par: str = None) -> sqlalchemy.engine.base.Connection:
        '''
         Creates a database engine based on the sink type (e.g., MySQL, PostgreSQL).
        Uses the default dbname from config unless a specific database is provided.
        '''

        # if db_par is none then create engine with dbname from config else we will create engine with passed db name(db_par)
        DB_name=self.dbname
        if db_par!=None:
            DB_name=db_par
        print("DB name in create engine ::: ", DB_name,self.sink_db)
        if self.engine == 'mysql':
            return sqlalchemy.create_engine(f"{self.engine}://{self.user}:{self.password}@{self.host}:{self.port}/{DB_name}")
        elif self.engine == 'postgresql':
            self.logger.debug(f"host : {self.host}, User: {self.user}, Password: {self.password}, Port: {self.port},DB_Name: {DB_name}")
            return sqlalchemy.create_engine(f"{self.engine}://{self.user}:{self.password}@{self.host}:{self.port}/{DB_name}")
        elif self.engine == 'oracle+oracledb':
            return sqlalchemy.create_engine(f"{self.engine}://{self.user}:{self.password}@{self.host}:{self.port}/?service_name={self.service_name}")
        elif self.engine == 'mssql+pymssql':
            return sqlalchemy.create_engine(f"{self.engine}://{self.user}:{self.password}@{self.host}:{self.port}/{DB_name}")
        else:
            print("extend above if condition for other databases")
            raise ValueError(f"Unsupported engine: {self.engine}")

    def db_execute(self, query, db_par: str = None):
        '''
        Runs a SQL query and commits it to the specified database (or default from config if none provided).
        Ensures a commit happens even for schema changes.        '''
        db_name = self.dbname
        if db_par != None:
            db_name = db_par
        engine = self.create_engine(db_name)
        with engine.connect() as conn:
            conn.execute(sqlalchemy.text("COMMIT;"))
            conn.execute(sqlalchemy.text(query))  # Use sqlalchemy.text() for raw SQL execution
            conn.commit()

    def Create_DB(self,db_name : str):
        '''
        Creates Database in appropriate sink such as postgresql,mysql,..
        '''

        if self.engine == "postgresql" or self.engine == "mysql" :
            # Create the database if it does not exist
            try:
                self.db_execute(f"CREATE DATABASE {db_name}")
                self.logger.debug(f"Created DataBase : {db_name}")
            except ProgrammingError:
                self.logger.debug(f" DataBase : {db_name} already exists")



    def flush_table(self, table_name):
        '''
        Creates Schema and drop table if exists
        '''

        self.Create_DB(self.sink_db)
        if self.engine== "postgresql":
            create_schema_query = f"create schema if not exists {self.sink_schema};"
            self.db_execute(create_schema_query, self.sink_db)
        drop_query = f"DROP TABLE IF EXISTS {self.sink_schema}.{table_name};"
        self.db_execute(drop_query,self.sink_db)


    def pd_insert_into_table(self, table_name : list , records : list ):
        '''
        1. converts records into pandas dataframe and writes to provided table name
        2. update the timestamp of the table to current time
        3. If success returns True else False.
        '''

        self.logger.debug(f"No of rows: {len(records)}")
        self.logger.debug(f"Records: {records}")
        engine = self.create_engine(self.sink_db)
        with engine.connect() as conn:
            try:
                data = pd.DataFrame(records)
                data.to_sql(table_name, conn, schema=self.sink_schema, if_exists='append', index=False)
                self.update_timestamp(table_name,datetime.datetime.now())
                return True  # Indicating success
            except Exception as e:
                print(f"Error inserting data: {e}")
                return False  # Indicating failure


