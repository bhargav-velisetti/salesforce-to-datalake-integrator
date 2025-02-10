import datetime
import psycopg2
import requests

#PostgreSQL Credentials
DB_NAME='ELT'
DB_USER='stagereader'
DB_USER_PASS='elt-framework'
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
    return conn.cursor()

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


"""
print(last_ingested_ts, current_time)
if current_time >last_ingested_ts:
    print("Will ingest")
else:
    print("Not Latest Data")
"""

def main():
    cur = get_postgres_connection(DB_NAME, DB_USER, DB_USER_PASS, DB_HOST, DB_PORT)
    table_list = get_table_list(cur)
    response = get_bearertoken_and_instanceurl(SF_USERNAME, SF_PASSWORD, SF_SECURITY_TOKEN, CLIENT_ID, CLIENT_SECRET)
    bearer_token = response[0]
    instance_url = response[1]
    for table in table_list:
        table_name = table[0]
        incr_col = table[2]
        if table[1]:  # If table loading is enabled
            print(f"Loading is enabled for {table_name}, incremental col: {incr_col}")
            # get last_fetch_ts for tbl from checkpoint config
            last_ts = get_last_fetch_ts(cur, table_name)
            print(f"\tLast loaded at: {last_ts}")
            # get Column list from the config db for table
            # Built Select query for salesforce
            # Send request using query, instanceUrl, and BearerToken
            # If last_ts LIKE '1900-%' -> queryall
            # Else query
            # After received response, implement UPSERT
        else:
            print(f"Loading Not Enabled for {table_name}")


if __name__ == '__main__':
    main()