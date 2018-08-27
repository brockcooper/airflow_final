import time
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
import domo_python
import boto3
import pandas
#import os
#from airflow.models import Variable
import gc
import sys
import _pickle as cPickle


# Please change the variables in the following section as they apply to your specific use case
#############################################################

# Domo Variables from developer.domo.com
domo_client_id = "bddcc89c-36a7-4256-b193-43d2d3e1de8b"
domo_client_secret = "bc2aebb540b938bff30e6d178ce4340a4b7bb66c8e8f76214f0c5cc853c407e1"

# Domo Dataset ID of the Domo to Snowflake Mapping File
# The mapping file should just be two columns:
# Column 1) Snowflake Table Name (this will be the name of the table we will create in snowflake. Needs to be 1 word and all lowercase)
# Column 2) Domo dataset ID (This will be the dataset id of the data that will be inserted into that table)
domo_dataset_id = "02550785-286a-46ff-b8ae-85f7b413879b"

# Setting your Snowflake account information
snowflake_account = 'untuckit.us-east-1'
snowflake_username = 'Big_Squid'
snowflake_password = 'BigSquid1!'
snowflake_database = 'UNTUCKIT_MASTER_DB'
snowflake_schema = 'DATA_LAKE'
snowflake_warehouse = 'LOAD_WH'

s3_bucket_name = 'domo-to-snowflake-testing'
AWS_ACCESS_KEY_ID = 'AKIAI75T5HFL674ICZMA'
AWS_SECRET_ACCESS_KEY = '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'

## Domo Variables from developer.domo.com
#domo_client_id = Variable.get("domo_client_id")
#domo_client_secret = Variable.get("domo_client_secret")
#
## Domo Dataset ID of the Domo to Snowflake Mapping File
## The mapping file should just be two columns:
## Column 1) Snowflake Table Name (this will be the name of the table we will create in snowflake. Needs to be 1 word and all lowercase)
## Column 2) Domo dataset ID (This will be the dataset id of the data that will be inserted into that table)
#domo_dataset_id = Variable.get("domo_to_snowflake_dataset_id")
#
## Setting your Snowflake account information
#snowflake_account = Variable.get("untuckit_snowflake_account")
#snowflake_username = Variable.get("untuckit_snowflake_username")
#snowflake_password = Variable.get("untuckit_snowflake_password")
#snowflake_database = Variable.get("untuckit_snowflake_database")
#snowflake_schema = Variable.get("untuckit_snowflake_schema")
#snowflake_warehouse = Variable.get("untuckit_snowflake_warehouse")
#
#s3_bucket_name = Variable.get("domo_to_snowflake_s3_bucket")
#aws_access = Variable.get("brock_aws_access_key_id")
#aws_secret = Variable.get("brock_aws_secret_access_key")


#############################################################

def snowflake_connection ( account, username, password, database, schema, warehouse ) :
    engine = create_engine(URL(
        account = account,
        user = username,
        password = password,
        database = database,
        schema = schema,
        warehouse = warehouse,
        numpy=True
    ))

    return engine.connect()

def dataframe_to_snowflake ( df, table, connection ) :
      df.to_sql(table, connection, if_exists='replace', chunksize=15000, index=False)

def push_all_datasets_to_snowflake( domo_mapping_df, domo_client_id, domo_client_secret, connection ) :
    start_whole = time.time()

    for row in domo_mapping_df.itertuples():
        start_time = time.time()
        to_table_name = row[1]
        domo_dataset_id = row[2]
        dataframe_to_snowflake( domo_python.domo_csv_to_dataframe( domo_dataset_id, domo_client_id, domo_client_secret ), to_table_name, connection )
        end_time = time.time()
        table_time = end_time - start_time
        print("Time for {table}: {time} seconds".format(time = table_time, table = to_table_name))

    end_whole = time.time()
    whole_time = (end_whole - start_whole)
    print("Total time: {time} seconds".format(time = whole_time))

def automate_domo_to_snowflake_insert_into (dw_account, dw_username, dw_password, dw_database, dw_schema, dw_warehouse,
                               domo_dataset_id, domo_client_id, domo_client_secret):

    # Get Domo to Snowflake Mapping File
    df = domo_python.domo_csv_to_dataframe ( domo_dataset_id, domo_client_id, domo_client_secret )

    # Start Snowflake Connection
    connection = snowflake_connection ( dw_account, dw_username, dw_password, dw_database, dw_schema, dw_warehouse )

    # Push all datasets to Snowflake
    push_all_datasets_to_snowflake( df, domo_client_id, domo_client_secret, connection )

    connection.close()

def write_csv_to_s3 (s3_resource, bucket_name, file_name, csv):
    obj = s3_resource.Object(bucket_name, "{file_name}.csv".format(file_name=file_name))
    return s3_resource.Object(bucket_name, "{file_name}.csv".format(file_name=file_name)).put(Body=csv)

def memory_dump():
    with open("memory.pickle", 'wb') as dump:
        for obj in gc.get_objects():
            i = id(obj)
            size = sys.getsizeof(obj, 0)
            #    referrers = [id(o) for o in gc.get_referrers(obj) if hasattr(o, '__class__')]
            referents = [id(o) for o in gc.get_referents(obj) if hasattr(o, '__class__')]
            if hasattr(obj, '__class__'):
                cls = str(obj.__class__)
                cPickle.dump({'id': i, 'class': cls, 'size': size, 'referents': referents}, dump)

def push_all_csvs_to_s3( domo_mapping_df, domo_client_id, domo_client_secret, s3_bucket_name, connection, aws_access_key_id, aws_secret_access_key ) :
    start_whole = time.time()
    s3_resource = boto3.resource('s3')
    connection = connection.connect()



    for row in domo_mapping_df.itertuples():
        start_time = time.time()

        # Get table name and dataset ID from Domo Mapping
        print("Getting Domo Dataset Info for {table}...")
        to_table_name = row[1]
        domo_dataset_id = row[2]
        print("Table info retrieved")

        # Send to S3
        print("Sending {table} data from Domo to Snowflake...")
        start_s3_time = time.time()
        write_csv_to_s3 (s3_resource, s3_bucket_name, to_table_name, domo_python.export_domo_dataset( domo_dataset_id, domo_python.get_access_token( domo_client_id, domo_client_secret ), 'false'))
        end_s3_time = time.time()
        s3_time = end_s3_time - start_s3_time
        print("Time for {table} to S3: {time} seconds".format(time = s3_time, table = to_table_name))

        # Send to Snowflake
        start_sf_time = time.time()

        try:
            connection.execute("truncate table {snowflake_table_name}".format(snowflake_table_name=to_table_name))
        except:
            print('Destination table not found, Insert Into initiated(header rows only)')
            dataframe_to_snowflake( domo_python.domo_csv_to_dataframe( domo_dataset_id, domo_client_id, domo_client_secret ).head(), to_table_name, connection )
            connection.execute("truncate table {snowflake_table_name}".format(snowflake_table_name=to_table_name))


        try:
            connection.execute("""
                copy into {snowflake_table_name} from 's3://{bucket}/{file_name}.csv'
                    CREDENTIALS = (
                        aws_key_id='{aws_access_key_id}',
                        aws_secret_key='{aws_secret_access_key}')
                    FILE_FORMAT=(field_delimiter=',', RECORD_DELIMITER = '\n', FIELD_OPTIONALLY_ENCLOSED_BY = '"'  )
                """.format(
                        snowflake_table_name=to_table_name,
                        bucket=s3_bucket_name,
                        file_name=to_table_name,
                        aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_secret_access_key
                ))
            print('Copy Into from S3 successfully')
        except:
            try:
                print('Copy Into failed, Table Exists, Insert Into Initiated(full table)')
                connection.execute("drop table {snowflake_table_name}".format(snowflake_table_name=to_table_name))
                dataframe_to_snowflake( domo_python.domo_csv_to_dataframe( domo_dataset_id, domo_client_id, domo_client_secret ), to_table_name, connection )
            except:
                print('Logic Error... check snowflake. Something is wrong with data types')

        end_sf_time = time.time()
        sf_time = end_sf_time - start_sf_time
        print("Time for {table} to SF: {time} seconds".format(time = sf_time, table = to_table_name))

        # Delete from S3
        s3_resource.Object(s3_bucket_name, "{file_name}.csv".format(file_name = to_table_name)).delete()

        # Record Time
        end_time = time.time()
        table_time = end_time - start_time
        print("Total Time for {table}: {time} seconds".format(time = table_time, table = to_table_name))

        # dump memory
        memory_dump()

    end_whole = time.time()
    whole_time = (end_whole - start_whole)
    print('Complete')
    print("Total time: {time} seconds".format(time = whole_time))
    connection.close()

df = domo_python.domo_csv_to_dataframe ( domo_dataset_id, domo_client_id, domo_client_secret )
connection = snowflake_connection ( snowflake_account, snowflake_username, snowflake_password, snowflake_database, snowflake_schema, snowflake_warehouse )
push_all_csvs_to_s3( df, domo_client_id, domo_client_secret, s3_bucket_name, connection, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY )
