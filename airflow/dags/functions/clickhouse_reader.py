import clickhouse_connect
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()

# ClickHouse connection details
host = os.environ['host']
port = os.environ['port']
user = os.environ['user']
password = os.environ['password']
db = os.environ['db']

dir = os.getcwd()

input_path = dir + '/dags/queries/clickhouse_query.sql'
output_path = dir + '/dags/queries/sql_insert_query.sql'

client = clickhouse_connect.get_client(host=host, port=8443, username=user, password=password)

def read_clickhouse():
    with open(input_path,mode='r') as f: # edit the path to the location of your queries
        sql_query = f.read()

    results = client.query(sql_query)
    df = pd.DataFrame(results.result_rows, columns= results.column_names)

    print ('data extracted')

    conv_dict = {'Month':str}

    df = df.astype(conv_dict) # ensuring that the month-date column appears as is

    print('data trasformed')

    insert_query = f"INSERT INTO trips VALUES\n"

    for index, row in df.iterrows():
        values = ', '.join(map(repr, row))
        insert_query += f"({values}),\n"
    
    insert_query = insert_query.rstrip(',\n')

    # Join all INSERT statements into a single SQL script

    with open(output_path, mode='w') as f:
        f.write(insert_query)
    
    print ('insert script generated')

if __name__ == "__main__":
    read_clickhouse()