import argparse
import yaml

def parse_args():
    # Create an ArgumentParser object
    parser = argparse.ArgumentParser(description="Returns Replication Config file path")

    #default="config/replication_config.yaml"
    parser.add_argument(
        "-conf", "--replication_config", type=str, help="Provide the path of the replication config file", required=True
    )

    # Parse the command-line arguments
    args = parser.parse_args()
    return args.replication_config

def get_config(data : dict) -> dict:
    conn_id = data['conn_id']

    if data['config_type'] == 'plain_text':
        with open(data['config_path'], 'r') as f:
            config = yaml.load(f, Loader=yaml.SafeLoader)
            return config[conn_id]
        
    elif data['config_type'] == 'gcp_secret_manager':
        print('Implement GCP Secret Manager')

    elif data['config_type'] == 'aws_secret_manager':
        print('Implement AWS Secret Manager')

    elif data['config_type'] == 'azure_key_vault':
        print('Implement Azure Key Vault')
    else:
        raise ValueError("Invalid config_type provided")

def build_select_statement(cursor,table_name) -> str:
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

def chunk_list(input_list, chunk_size):
    return [input_list[i:i + chunk_size] for i in range(0, len(input_list), chunk_size)]

