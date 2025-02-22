import psycopg2
import sqlalchemy
import pandas as pd 
from pandas import DataFrame
from sqlalchemy.exc import ProgrammingError

class DBHelper:
    def __init__(self, db_config : dict, logger):
        self.logger=logger
        logger.debug(f" sink_configgg : {db_config}")
        for key, value in db_config.items():
            setattr(self, key, value)



    def create_engine(self,db_par: str = None) -> sqlalchemy.engine.base.Connection:
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

    def db_execute(self, query,db_par: str = None):
        db_name = self.dbname
        if db_par != None:
            db_name = db_par
        engine = self.create_engine(db_name)
        with engine.connect() as conn:
            conn.execute(sqlalchemy.text("COMMIT;"))
            conn.execute(sqlalchemy.text(query))  # Use sqlalchemy.text() for raw SQL execution
            conn.commit()

    def Create_DB(self):
        if self.engine=="postgresql":
            # Create the database if it does not exist
            try:
                self.db_execute(f"CREATE DATABASE {self.sink_db}")
                self.logger.debug(f"Created DataBase : {self.sink_db}")
            except ProgrammingError:
                self.logger.debug(f" DataBase : {self.sink_db} already exists")


    def flush_table(self, table_name):  # WORKING ON RN


        # SINK needs to be dropped in case query is changing (col number is different)
        # Check if table and schema exists logic to be implemnted
        self.Create_DB()
        create_schema_query = f"create schema if not exists {self.sink_schema};"
        drop_query = f"DROP TABLE IF EXISTS {self.sink_schema}.{table_name} CASCADE;"  ## CHANGE AND FIGURE OUT TO PASS SCHEMA NAME
        self.db_execute(create_schema_query,self.sink_db)
        self.db_execute(drop_query,self.sink_db)


    def pd_insert_into_table(self, table_name : list , records : list ):
        self.logger.debug(f"No of rows: {len(records)}")
        self.logger.debug(f"Records: {records}")
        engine = self.create_engine(self.sink_db)
        with engine.connect() as conn:
            try:
                data = pd.DataFrame(records)
                data.to_sql(table_name, conn, schema=self.sink_schema, if_exists='append', index=False)
                return True  # Indicating success
            except Exception as e:
                print(f"Error inserting data: {e}")
                return False  # Indicating failure

