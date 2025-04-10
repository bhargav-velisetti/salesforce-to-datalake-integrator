import psycopg2
import sqlalchemy
import pandas as pd
import requests
from pathlib import Path
import base64
from sqlalchemy.exc import ProgrammingError
from datetime import datetime
from google.cloud import bigquery
from databricks import sql

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

        target_db = getattr(self, 'dbname', None)
        if target_db_par is not None:
            target_db = target_db_par

        if self.engine == 'mysql':
            self.logger.debug(f"\t\tCreating MySQL Engine for Target Database: {target_db}")
            return sqlalchemy.create_engine(f"{self.engine}://{self.user}:{self.password}@{self.host}:{self.port}/{target_db}")

        elif self.engine == 'postgresql':
            self.logger.debug(f"\t\tCreating Postgres Engine for Target Database: {target_db }")

            return sqlalchemy.create_engine(f"{self.engine}://{self.user}:{self.password}@{self.host}:{self.port}/{target_db}")

        elif self.engine == 'oracle+oracledb':
            return sqlalchemy.create_engine(f"{self.engine}://{self.user}:{self.password}@{self.host}:{self.port}/?service_name={self.dbname}")

        elif self.engine == 'mssql+pymssql':
            self.logger.debug(f"\t\tCreating mssql+pymssql Engine for Target Database: {target_db}")
            return sqlalchemy.create_engine(f"{self.engine}://{self.user}:{self.password}@{self.host}:{self.port}/{target_db}")

        elif self.engine == 'bigquery':
            self.logger.debug(f"\t\tCreating BigQuery Engine for Target Project : {self.project_id} & Target Dataset : {self.dataset_id}")
            return bigquery.Client(project=self.project_id)

        elif self.engine == 'databricks':
            self.logger.debug(f"\t\tCreating Databricks Engine for Server : {self.server_hostname}, Catalog: {self.dbx_catalog}, Schema : {self.dbx_schema}")
            return sql.connect(
                server_hostname=getattr(self, 'server_hostname', None),
                http_path=getattr(self, 'http_path', None),
                access_token=getattr(self, 'access_token', None)
            )

        else:
            print("extend above if condition for other databases")
            self.logger.debug(f"\t\tUnsupported engine: {self.engine}")
            raise ValueError(f"Unsupported engine: {self.engine}")

    def db_execute(self, query, target_db_par: str = None, arg = None):
        '''
        Runs a SQL query and commits it to the specified database (or default from config if none provided).
        Ensures a commit happens even for schema changes.
        '''
        if self.engine == 'databricks':
            db_name = getattr(self, 'dbx_catalog', None)
        else:
            db_name = getattr(self, 'dbname', None)

        if target_db_par is not None:
            db_name = target_db_par

        engine = self.create_engine(db_name)

        if self.engine == 'bigquery':

            self.logger.debug(f"\t\tExecuting BigQuery Query: {query}")
            query_job  = engine.query(query)
            query_job.result()  # Wait for the job to complete
            self.logger.debug(f"\t\tQuery Completed Successfully")

        elif self.engine == 'databricks':
            # Using Databricks-sql connector for text queries
            with engine.cursor() as cursor:
                self.logger.info(f"\t\tExecuting Query on DBX: {query}")
                cursor.execute(query)
                engine.commit()
                cursor.close()
                engine.close()

        elif self.engine in ['mysql', 'postgresql', 'oracle+oracledb', 'mssql+pymssql']:
            # Sqlalchemy based database engines
            # Works with relational databases
            with engine.connect() as conn:
                conn.execute(sqlalchemy.text(query), arg)  # Use sqlalchemy.text() for raw SQL execution
                conn.commit() # causing error "Connection has no attribute commit"

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
        #self.Create_DB(self.config_dbname) # We should not create the DB if it does not exists.
        # MAYBE USE EXCEPTION HANDLING AND RETRY HERE
        if self.engine == "postgresql":
            #self.logger.debug(f"\t\tTrying to create config schema : {self.config_schema}")
            #create_schema_query = f"create schema if not exists {self.config_schema};"
            #self.db_execute(create_schema_query, self.config_dbname)
            create_table_query=f"""CREATE TABLE if not exists {self.config_schema}.{self.chk_table} (
                            trg_tbl_nm VARCHAR(255) PRIMARY KEY,
                            last_fetch_ts TIMESTAMP WITH TIME ZONE DEFAULT '0001-01-01 00:00:00 UTC' -- Oldest possible timestamp
                            );"""
            self.logger.debug(f"\t\tTrying to create config table :  {self.config_dbname}.{self.chk_table}")
            self.db_execute(create_table_query, target_db_par=self.config_dbname)

        elif self.engine == 'databricks':
            #allow_defaults_query = f"ALTER TABLE {self.config_dbname}.{self.config_schema}.{self.chk_table} SET TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')"
            #self.db_execute(allow_defaults_query)
            # Not setting default value as DeltaLake Default column property needs to be enabled on table
            create_table_query = f"""
                CREATE TABLE IF NOT EXISTS {self.config_dbname}.{self.config_schema}.{self.chk_table} (
                    trg_tbl_nm STRING,
                    last_fetch_ts TIMESTAMP
                )
                USING DELTA;
            """
            self.logger.debug(f"\t\tTrying to create config table :  {self.config_schema}.{self.chk_table}")
            self.db_execute(create_table_query)

        elif self.engine == "mysql":
            create_table_query = f"""CREATE TABLE if not exists {self.config_dbname}.{self.chk_table} (
                                        trg_tbl_nm VARCHAR(255) PRIMARY KEY,
                                        last_fetch_ts TIMESTAMP DEFAULT '1970-01-01 00:00:01' -- Oldest possible timestamp
                                        );"""
            self.logger.debug(f"\t\tTrying to create config table :  {self.config_dbname}.{self.chk_table}")
            self.db_execute(create_table_query, target_db_par=self.config_dbname)

        elif self.engine == "bigquery":
            create_table_query = f"""CREATE TABLE if not exists `{self.project_id}.{self.config_dbname}.{self.chk_table}` (
                            trg_tbl_nm STRING,
                            last_fetch_ts TIMESTAMP
                            );"""
            self.logger.debug(f"\t\tTrying to create config table :  `{self.project_id}.{self.config_dbname}.{self.chk_table}` ")
            self.db_execute(create_table_query)

    def last_fetch_ts(self, table_name : str):
        '''
        Retrieves the last fetch timestamp for a given table from the checkpoint table.
        Returns a default old timestamp if the table or checkpoint doesn’t exist.
        '''
        try:

            if self.engine in ['mysql', 'postgresql', 'oracle+oracledb', 'mssql+pymssql']:
                if self.engine == 'postgresql':
                    schema_or_db_name =self.config_schema
                else:
                    schema_or_db_name=self.config_dbname

                query = f"select last_fetch_ts from {schema_or_db_name}.{self.chk_table} where trg_tbl_nm='{table_name}'"

                engine = self.create_engine(self.config_dbname)

                with engine.connect() as conn:
                    last_ts = conn.execute(sqlalchemy.text(query)).fetchone()
                    self.logger.debug(f"last_ts response recieved: {last_ts}")    #If Nonetype returned, causing error with return #
                if last_ts is None:
                    return datetime(1900, 1, 1)
                else:
                    return last_ts[0]

            elif self.engine == 'databricks':
                schema_or_db_name = f"{self.dbx_catalog}.{self.config_schema}"
                engine = self.create_engine()
                query = f"select last_fetch_ts from {schema_or_db_name}.{self.chk_table} where trg_tbl_nm='{table_name}'"
                with engine.cursor() as cursor:
                    last_ts = cursor.execute(query).fetchone()
                    self.logger.debug(f"last_ts response recieved: {last_ts}")
                if last_ts is None:
                    return datetime(1900, 1, 1)
                else:
                    return last_ts[0]

            elif self.engine == 'bigquery':
                query = f"SELECT last_fetch_ts FROM `{self.project_id}.{self.config_dbname}.{self.chk_table}` WHERE trg_tbl_nm = '{table_name}'"
                client = self.create_engine()
                rows = client.query(query).result()
                for row in rows:
                    return row['last_fetch_ts']

        except ProgrammingError:
            self.logger.debug(f"{self.config_dbname} {self.config_dbname}.{self.chk_table} does not exist")
            # Return oldest value for Full Load
            return datetime(1900, 1, 1, tzinfo=datetime.timezone.utc)

    def update_timestamp(self,table : str ,last_fetch_ts : datetime):
        # Updated config_table to chk_table
        try:
            #self.logger.debug(f"\t\tUpdating Timestamp for Table: {update_chek_query}")
            if self.engine=='postgresql':
                update_chek_query = f"""INSERT INTO {self.config_schema}.{self.chk_table} (trg_tbl_nm, last_fetch_ts) 
                VALUES ('{table}', '{last_fetch_ts}')
                ON CONFLICT (trg_tbl_nm) DO UPDATE 
                SET last_fetch_ts = EXCLUDED.last_fetch_ts;"""
                self.db_execute(update_chek_query)

            elif self.engine=='mysql':
                update_chek_query = f"""
                        INSERT INTO {self.config_dbname}.{self.chk_table} (trg_tbl_nm, last_fetch_ts) 
                        VALUES ('{table}', '{last_fetch_ts}')
                        ON DUPLICATE KEY UPDATE 
                        last_fetch_ts = VALUES(last_fetch_ts);
                    """
                self.db_execute(update_chek_query)

            elif self.engine == 'bigquery':
                update_chek_query = f"""WITH new_checkpoint AS ( SELECT '{table}' as trg_tbl_nm, '{last_fetch_ts}' as last_fetch_ts)
                MERGE INTO `{self.project_id}.{self.config_dbname}.{self.chk_table}` 
                USING new_checkpoint
                ON trg_tbl_nm = new_checkpoint.trg_tbl_nm
                WHEN MATCHED THEN
                UPDATE SET last_fetch_ts = new_checkpoint.last_fetch_ts
                WHEN NOT MATCHED THEN
                INSERT (trg_tbl_nm, last_fetch_ts) VALUES (new_checkpoint.trg_tbl_nm, new_checkpoint.last_fetch_ts);
                """
                self.db_execute(update_chek_query)

            elif self.engine == 'databricks':
                update_chek_query = f"""
                MERGE INTO {self.dbx_catalog}.{self.config_schema}.{self.chk_table} AS target
                USING (SELECT '{table}' AS trg_tbl_nm, '{last_fetch_ts}' AS last_fetch_ts) AS source
                ON target.trg_tbl_nm = source.trg_tbl_nm
                WHEN MATCHED THEN 
                    UPDATE SET target.last_fetch_ts = source.last_fetch_ts
                WHEN NOT MATCHED THEN 
                    INSERT (trg_tbl_nm, last_fetch_ts) 
                    VALUES (source.trg_tbl_nm, source.last_fetch_ts);
                """
                self.db_execute(update_chek_query)

        except Exception as e:
            self.logger.debug(f" Error while updating timestamp : {e}")

    def flush_table(self, sink_table : str):
        '''
        Creates Schema and drop table if exists
        '''
        if self.engine in ['mysql','oracle+oracledb', 'mssql+pymssql']:
            drop_query = f"DROP TABLE IF EXISTS {self.dbname}.{self.sink_table};"
        elif self.engine ==  'postgresql':
            drop_query = f"DROP TABLE IF EXISTS {self.sink_schema}.{sink_table};"
        elif self.engine == 'bigquery':
            drop_query = f"DROP TABLE IF EXISTS {self.project_id}.{self.dataset_id}.{self.sink_table};"
        elif self.engine == 'databricks':
            drop_query = f"DROP TABLE IF EXISTS `{self.dbx_catalog}`.{self.sink_schema}.{sink_table};"
        self.logger.debug(f"\t\tFlushing Table: {sink_table}")
        self.db_execute(drop_query, target_db_par=self.sink_db)


    def dbx_pandas_dtype_to_databricks_sql(self, dtype):
        """Returns Databricks equivalent for Pandas dtype"""
        if pd.api.types.is_string_dtype(dtype):
            return "STRING"
        elif pd.api.types.is_integer_dtype(dtype):
            return "BIGINT"
        elif pd.api.types.is_float_dtype(dtype):
            return "DOUBLE"
        elif pd.api.types.is_bool_dtype(dtype):
            return "BOOLEAN"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            return "TIMESTAMP"
        else:
            return "STRING"  # default fallback


    def dbx_create_sink_table(self, cursor, tableName, df, schema = 'default'):
        cols = []
        for colName, dType in df.dtypes.items():
            dbxType = self.dbx_pandas_dtype_to_databricks_sql(dType)
            cols.append(f"`{colName}` {dbxType}")
        colStr = ',\n '.join(cols)
        """ddl = f
        CREATE TABLE IF NOT EXISTS `{getattr(self, 'dbx_catalog')}`.{self.sink_schema}.{tableName} (
            {colStr}
        )
        USING DELTA
        """
        ddl = f"CREATE TABLE IF NOT EXISTS `{getattr(self, 'dbx_catalog')}`.{self.sink_schema}.{tableName};"
        self.db_execute(ddl)

    def dbx_upload_csv(self, df : pd.DataFrame, table_name : str, dbfs_path : str, timestamp):
        script_path = Path(__file__).resolve().parent.parent
        csv_dir = script_path / "dbx_stage_csvs" / f"{table_name}"
        csv_dir.mkdir(parents=True, exist_ok=True)
        LOCAL_PATH = csv_dir / f"{table_name}_{timestamp}.csv"
        API_URL = f"https://{getattr(self, 'server_hostname', None)}/api/2.0/dbfs/"

        self.logger.debug(f"CREATING LOCAL COPY of Df at {LOCAL_PATH}")
        df.to_csv(LOCAL_PATH, index=False)

        create_resp = requests.post(
            API_URL + "create",
            headers={"Authorization": f"Bearer {getattr(self, 'access_token', None)}"},
            json={"path":dbfs_path, "overwrite": True}
        )
        #print(create_resp.json())
        handle = create_resp.json()["handle"]
        self.logger.debug(f"UPLOADING LOCAL FILE TO DATABRICKS at : {dbfs_path}")
        with open(LOCAL_PATH, "rb") as f:
            while True:
                data = f.read(1_000_000)  # Read in 10MB chunks
                if not data:
                    break
                encoded_data = base64.b64encode(data).decode("utf-8")
                try:
                    upload_resp = requests.post(
                        API_URL + "add-block",
                        headers={"Authorization": f"Bearer {getattr(self, 'access_token', None)}"},
                        json={"handle": handle, "data": encoded_data}
                    )
                    upload_resp.raise_for_status()  # Raise exception for HTTP errors
                except requests.exceptions.RequestException as e:
                    self.logger.error(f"Error uploading block. Response: {e}")
                    return

        requests.post(
            API_URL + "close",
            headers={"Authorization": f"Bearer {getattr(self, 'access_token', None)}"},
            json={"handle": handle}
        )
        self.logger.debug("Finished uploading to Databricks, removing local copy")
        LOCAL_PATH.unlink(missing_ok=True)

    def upload_csv_to_dbx_bulkapi(self, df: pd.DataFrame, table_name: str): # Not fully implemented yet
        script_path = Path(__file__).resolve().parent.parent
        csv_dir = script_path / "dbx_stage_csvs" / f"{table_name}"
        csv_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        LOCAL_PATH = csv_dir / f"{timestamp}.csv"
        DBFS_PATH = self.stage_path + f"{table_name}_{datetime.now()}.csv"
        self.logger.debug(f"CREATING LOCAL COPY of Df at {LOCAL_PATH}")

        df.to_csv(LOCAL_PATH, index=False)
        workspace_file_path = f"/Workspace/Users/paul@manreevsingh5gmail.onmicrosoft.com/stage_data/{table_name}.csv"
        dbfs_path = f"/dbfs/FileStore/stage_data/{table_name}.csv"
        API_URL = f"https://{getattr(self, 'server_hostname', None)}/api/2.0/fs/files{dbfs_path}"
        headers = {
            "Authorization": f"Bearer {getattr(self, 'access_token', None)}",
            "Content-Type": "application/octet-stream"
        }
        with open(LOCAL_PATH, "rb") as f:
            data = f.read()
        response = requests.put(API_URL, headers=headers, data=data, params={"overwrite": "true"})
        #print(response.json())

    def dbx_populate_table(self, table_name, dbfs_path):
        copy_into_query = f"""
            COPY INTO `{self.dbx_catalog}`.{self.sink_schema}.{table_name}
            FROM '{dbfs_path}'
            FILEFORMAT = CSV
            FORMAT_OPTIONS ('header' = 'true')
            COPY_OPTIONS ('mergeSchema' = 'true')
        """
        self.db_execute(copy_into_query)


    def pd_insert_into_table(self, table_name : str , records : list ):
        '''
        1. converts records into pandas dataframe and writes to provided table name
        2. update the timestamp of the table to current time
        3. If success returns True else False.
        '''

        self.logger.debug(f"\t\tStarting Insertion of : {len(records)} rows...")
        #print("Inside pd_insert_into_table")
        #print(f"{self.engine}\t")
        data = pd.DataFrame(records)
        engine = self.create_engine(self.sink_db)
        #print(f"Engine: {type(engine)}, Data: {type(data)}")
        try:
            if self.engine == 'bigquery':
                client = engine
                table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
                job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", create_disposition = "CREATE_IF_NEEDED") # Table will be created if not exists and data will be appended
                job = client.load_table_from_dataframe(data, table_id, job_config=job_config)
                job.result() # Wait for the job to complete.

            elif self.engine == 'databricks':
                #print("Inside databricks insertion part")
                #DBFS_PATH = self.stage_path + f"{table_name}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv"
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                DBFS_PATH = self.stage_path + f"{table_name}_{timestamp}.csv"
                cursor = engine.cursor()
                self.dbx_create_sink_table(cursor=cursor, tableName=table_name, df=data, schema=self.dbx_schema)
                self.dbx_upload_csv(df=data, table_name=table_name, dbfs_path=DBFS_PATH, timestamp=timestamp)
                self.dbx_populate_table(table_name=table_name, dbfs_path=DBFS_PATH)
                if len(data) > 0 and 'LastModifiedDate' in data.columns:
                    max_last_modified_date = pd.to_datetime(data['LastModifiedDate']).max()
                    self.update_timestamp(table_name, max_last_modified_date)
                engine.commit()
                cursor.close()
                engine.close()
                #print("Done databricks insertion part")
                """dbx_schema = getattr(self, "dbx_schema", None)
                dtypes = self.extract_dtype_from_df(data)
                with engine.connect() as conn:
                    conn.execute()
                data.to_sql(table_name, con=conn, schema=dbx_schema, if_exists='append', index=False, dtype=dtypes, method='multi')"""
                print("Databricks insertions end reached")

            elif self.engine in ['mysql', 'postgresql', 'oracle+oracledb', 'mssql+pymssql']:
                print("Inside rdbms insertion part")
                with engine.connect() as conn:
                    sink_schema = getattr(self, "sink_schema", None)
                    print(f"Data format: {data}")
                    data.to_sql(table_name, con=conn, schema=sink_schema, if_exists='append', index=False)
                    if len(data) > 0:
                        # Convert LastModifiedDate to a consistent format regardless of database type
                        if 'LastModifiedDate' in data.columns:
                            max_last_modified_date = pd.to_datetime(data['LastModifiedDate']).max()
                            if self.engine == 'mysql':
                                max_last_modified_date = max_last_modified_date.strftime("%Y-%m-%d %H:%M:%S")
                            else:
                                max_last_modified_date = max_last_modified_date.strftime("%Y-%m-%dT%H:%M:%SZ")
                            self.update_timestamp(table_name, max_last_modified_date)

                    self.logger.debug(f"\t\tSuccesssfully Completed!")
                    return True  # Indicating success

        except Exception as e:
                    self.logger.debug(f"Error inserting data: {e}")
                    print("Error inserting data",e)
                    return False  # Indicating failure