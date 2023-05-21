import pandas as pd
import pyarrow.parquet as pq
import mysql.connector

# Function to read parquet file and identify schema
def read_parquet_file(parquet_file):
    table = pq.read_table(parquet_file)
    schema = table.schema.to_pandas()
    return schema

# Function to create MySQL table
def create_mysql_table(table_name, schema):
    create_table_query = f"CREATE TABLE {table_name} ({', '.join(schema['name'] + ' ' + schema['type'])})"
    cursor.execute(create_table_query)

# Function to load data into MySQL table
def load_data_to_mysql(parquet_file, table_name):
    table = pq.read_table(parquet_file)
    df = table.to_pandas()
    columns = list(df.columns)
    values = df.values.tolist()

    insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})"
    cursor.executemany(insert_query, values)
    connection.commit()

# Connect to MySQL
connection = mysql.connector.connect(
    host='localhost',
    user='root',
    password='your_password',
    database='bajaj'
)
cursor = connection.cursor()

# Parquet file names
parquet_files = ['claim.parquet', 'product.parquet', 'policy.parquet']

# Process each parquet file
for parquet_file in parquet_files:
    # Read parquet file and identify schema
    schema = read_parquet_file(parquet_file)

    # Get table name from file name
    table_name = parquet_file.replace('.parquet', '')

    # Create MySQL table with the schema
    create_mysql_table(table_name, schema)

    # Load data from parquet file into MySQL table
    load_data_to_mysql(parquet_file, table_name)

# Close the cursor and connection
cursor.close()
connection.close()
