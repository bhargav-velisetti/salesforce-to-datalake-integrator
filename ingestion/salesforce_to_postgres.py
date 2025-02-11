import datetime
import psycopg2
import requests
import json

#PostgreSQL Credentials
DB_NAME='postgres'
DB_USER='postgres'
DB_USER_PASS='123'
DB_HOST='localhost'
DB_PORT='5432'

#Salesforce Credentials
SF_USERNAME='bhargav.v@selectiva.com'
SF_PASSWORD='AGTFfgtsde@1234'
SF_SECURITY_TOKEN='99QGgvg0MJTK1NsALpSRXg16'
CLIENT_ID='3MVG9XgkMlifdwVB60dyMz3jIc5E3QKxEemKvBq2SFoZgJ0QWZ0pP7Y8yulsw2t6i4O47sUXLt3awXekl_nIN'
CLIENT_SECRET='AFCC3C3C0ACC9E3B76DDEF9562C9C07F7856234A03826FF8D8C1E472F0E59101'

# Create the Postgres Database Connection object
# Return the cursor for that connection
def get_postgres_connection(db_name, db_user, db_pass, db_host, db_port):
    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_pass,
        host=db_host,
        port=db_port
    )
    return conn, conn.cursor()

# Get the bearer token AS WELL AS instance url from salesforce
# Returns -> A tuple having structure like (token, url)
def get_bearertoken_and_instanceurl(username, password, security_token, client_id, client_secret) -> tuple:
    auth_url = "https://login.salesforce.com/services/oauth2/token"
    auth_data = {
        "grant_type":"password",
        "client_id": client_id,
        "client_secret": client_secret,
        "username": username,
        "password": password + security_token,
    }
    response = requests.post(auth_url, data=auth_data).json()
    return response['access_token'], response['instance_url']

# Get the list of tables in the postgres ingestion config table
# Select table name, isEnabled, Incremental column
# Return a list of tables having structure [(name,True/False, incr_col)]
def get_table_list(cursor : psycopg2.extensions.cursor) -> list :
    cursor.execute("SELECT src_tbl_nm, isenabled, incr_col FROM sfdc_ingestion_config.ing_tbl_list;")
    return cursor.fetchall()

# Get last_fetch_ts from ing_checkpoint table
# Returns a datetime object
def get_last_fetch_ts(cursor : psycopg2.extensions.cursor, table_name) -> datetime.datetime:
    cursor.execute(f"""
        SELECT MAX(last_fetch_ts) 
        FROM sfdc_ingestion_config.ing_tbl_checkpoint 
        WHERE trg_tbl_nm = '{table_name}'
    """)
    return cursor.fetchall()[0][0]

def build_select_statement(cursor: psycopg2.extensions.cursor, table_name: str) -> str:
    """
    Builds a SELECT statement for the given table using columns from ing_tbl_columns.
    """
    cursor.execute(f"""
        SELECT src_tbl_col 
        FROM sfdc_ingestion_config.ing_tbl_columns 
        WHERE src_tbl_nm = '{table_name}'
    """)
    columns = [row[0] for row in cursor.fetchall()]
    if not columns:
        raise ValueError(f"No columns found for table {table_name} in ing_tbl_columns.")
    return f"SELECT {', '.join(columns)} FROM {table_name}"


def fetch_data_from_sfdc(instance_url: str, bearer_token: str, query: str) -> list:
    """
    Fetches all paginated data from Salesforce using the REST API.
    """
    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "Content-Type": "application/json"
    }

    records = []
    url = f"{instance_url}/services/data/v57.0/query?q={query}"

    while url:
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            raise Exception(f"Failed to fetch data from Salesforce: {response.text}")

        response_json = response.json()
        records.extend(response_json.get("records", []))

        # Check if there's a nextRecordsUrl for pagination
        next_records_url = response_json.get("nextRecordsUrl")
        url = f"{instance_url}{next_records_url}" if next_records_url else None

    return records


def write_data_to_postgres(cursor, table_name, data):
    cursor.execute(f"""
        SELECT trg_tbl_col 
        FROM sfdc_ingestion_config.ing_tbl_columns 
        WHERE src_tbl_nm = '{table_name}'
    """)
    columns = [row[0] for row in cursor.fetchall()]
    placeholders = ", ".join(["%s"] * len(columns))
    insert_query = f"INSERT INTO sfdc_stage.{table_name} ({', '.join(columns)}) VALUES ({placeholders})"
    try:
        for record in data:
            values = [json.dumps(record.get(col)) if isinstance(record.get(col), dict) else record.get(col) for col in columns]
            cursor.execute(insert_query, values)
        return True  # Indicating success
    except Exception as e:
        print(f"Error inserting data: {e}")
        return False  # Indicating failure


# Update last_fetch_ts if data insertion was successful
def update_checkpoint(cursor, table_name, last_fetch_ts):
    cursor.execute(f"""
        INSERT INTO sfdc_ingestion_config.ing_tbl_checkpoint (trg_tbl_nm, last_fetch_ts) 
        VALUES ('{table_name}', '{last_fetch_ts}')
        ON CONFLICT (trg_tbl_nm) DO UPDATE 
        SET last_fetch_ts = EXCLUDED.last_fetch_ts;
    """)


"""
print(last_ingested_ts, current_time)
if current_time >last_ingested_ts:
    print("Will ingest")
else:
    print("Not Latest Data")
"""

def main():
    # Get the PostgreSQL connection and cursor
    conn, cur = get_postgres_connection(DB_NAME, DB_USER, DB_USER_PASS, DB_HOST, DB_PORT)
    table_list = get_table_list(cur)
    response = get_bearertoken_and_instanceurl(SF_USERNAME, SF_PASSWORD, SF_SECURITY_TOKEN, CLIENT_ID, CLIENT_SECRET)
    bearer_token = response[0]
    instance_url = response[1]
    for table in table_list:
        table_name = table[0]
        incr_col = table[2]
        if table[1]:  # If table loading is enabled
            print(f"Loading is enabled for {table_name}, incremental col: {incr_col}")
            last_ts = get_last_fetch_ts(cur, table_name)
            if last_ts is None:
                 last_ts = datetime.datetime(1900, 1, 1, tzinfo=datetime.timezone.utc)
            print(f"\tLast loaded at: {last_ts}")

            select_query = build_select_statement(cur, table_name)
            current_ts = datetime.datetime.now(datetime.timezone.utc)
            # changing select query according to incremental load
            if last_ts != datetime.datetime(1900, 1, 1, tzinfo=datetime.timezone.utc):
                last_ts_iso = last_ts.isoformat()
                select_query += f" WHERE {incr_col} > {last_ts_iso}"
                print("incremental Load")
            else:
                print("Full Load")

            data = fetch_data_from_sfdc(instance_url, bearer_token, select_query)
            print(f"{len(data)} records.")
            if data:
                success = write_data_to_postgres(cur, table_name, data)
                if success:
                    update_checkpoint(cur, table_name, current_ts)
                    conn.commit()
                    print(f"Successfully updated checkpoint for {table_name}.")
                else:
                    print(f"Skipping checkpoint update due to insertion failure for {table_name}.")


        else:
            print(f"Loading Not Enabled for {table_name}")
    # Close cursor and connection
    cur.close()
    conn.close()

if __name__ == '__main__':
    main()