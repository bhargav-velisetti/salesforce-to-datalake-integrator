import psycopg2
import sqlalchemy
import pandas as pd 
from pandas import DataFrame

class DBHelper:
    def __init__(self, db_config : dict):
        for key, value in db_config.items():
            setattr(self, key, value)

    def create_engine(self) -> sqlalchemy.engine.base.Connection:
        if self.engine == 'mysql':
            return sqlalchemy.create_engine(f"{self.engine}://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}")
        elif self.engine == 'postgresql':
            print(self.host, self.user, self.password, self.port, self.dbname)
            return sqlalchemy.create_engine(f"{self.engine}://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}")
        elif self.engine == 'oracle+oracledb':
            return sqlalchemy.create_engine(f"{self.engine}://{self.user}:{self.password}@{self.host}:{self.port}/?service_name={self.service_name}")
        elif self.engine == 'mssql+pymssql':
            return sqlalchemy.create_engine(f"{self.engine}://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}")
        else:
            print("extend above if condition for other databases")
            raise ValueError(f"Unsupported engine: {self.engine}")

    def db_execute(self, query):
        #print(self.host)
        engine = self.create_engine()
        with engine.connect() as conn:
            conn.execute(sqlalchemy.text(query))  # Use sqlalchemy.text() for raw SQL execution
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

