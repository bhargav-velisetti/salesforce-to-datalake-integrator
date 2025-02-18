import psycopg2
import sqlalchemy
import pandas as pd 
from pandas import DataFrame

class DBHelper:
    def __init__(self, **db_config):
        self.engine = db_config['engine']
        self.dbname=db_config['dbname'],
        self.user=db_config['user'],
        self.password=db_config['password'],
        self.host=db_config['host'],
        self.port=db_config['port']
        self.service_name=db_config['service_name']

    def create_engine(self) -> sqlalchemy.engine.base.Connection:
        if self.engine == 'mysql':
            return sqlalchemy.create_engine(f"{self.engine}://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}")
        elif self.engine == 'postgresql':
            return sqlalchemy.create_engine(f"{self.engine}://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}")
        elif self.engine == 'oracle+oracledb':
            return sqlalchemy.create_engine(f"{self.engine}://{self.user}:{self.password}@{self.host}:{self.port}/?service_name={self.service_name}")
        elif self.engine == 'mssql+pymssql':
            return sqlalchemy.create_engine(f"{self.engine}://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}")
        else:
            print("extend above if condition for other databases")
            raise ValueError(f"Unsupported engine: {self.engine}")

    def db_execute(self, query):
        with self.create_engine() as conn:
            conn.execute(query)
            conn.commit()

    def flush_table(self, table_name):
        # Check if table exists logic to be impleemnted
        query = f"TRUNCATE TABLE {table_name}"
        self.db_execute(query)

    def pd_insert_into_table(self, table_name : list , records : list ):
        with self.create_engine() as conn:
            try:
                data = pd.DataFrame(records)
                data.to_sql(table_name, conn, if_exists='append', index=False)
                return True  # Indicating success
            except Exception as e:
                print(f"Error inserting data: {e}")
                return False  # Indicating failure

