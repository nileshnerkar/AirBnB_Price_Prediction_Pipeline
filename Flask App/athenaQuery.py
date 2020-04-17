import boto3
import pandas as pd
import io
import re
import time

def athena_query(client, params):
    
    response = client.start_query_execution(
        QueryString=params["query"],
        QueryExecutionContext={
            'Database': params['database']
        },
        ResultConfiguration={
            'OutputLocation': 's3://' + params['bucket'] + '/' + params['path']
        }
    )
    return response

def athena_to_s3(session, params, max_execution = 5):
    client = session.client('athena', region_name=params["region"])
    execution = athena_query(client, params)
    execution_id = execution['QueryExecutionId']
    state = 'RUNNING'
#(max_execution > 0 and state in ['RUNNING'])
    while (max_execution > 0 and state in ['RUNNING']):
        response = client.get_query_execution(QueryExecutionId = execution_id)

        if 'QueryExecution' in response and \
                'Status' in response['QueryExecution'] and \
                'State' in response['QueryExecution']['Status']:
            state = response['QueryExecution']['Status']['State']
            if state == 'FAILED':
                print("State Failed...!")
                return None
            elif state == 'SUCCEEDED':
                s3_path = response['QueryExecution']['ResultConfiguration']['OutputLocation']
                print(s3_path)
                filename = s3_path.split("/")[5]
                print(filename)
                return filename
        time.sleep(1)
    print("Out of While")
    return None

def s3_to_pandas(session, params, s3_filename):    
    s3client = session.client('s3')
    print(params['path'])
    obj = s3client.get_object(Bucket=params['bucket'],
                              Key=params['path'] + '/' + s3_filename)
    df = pd.read_csv(io.BytesIO(obj['Body'].read()))
    return df

# Deletes all files in your path so use carefully!
def cleanup(session, params):
    s3 = session.resource('s3')
    my_bucket = s3.Bucket(params['bucket'])
    for item in my_bucket.objects.filter(Prefix=params['path']):
        item.delete()

def exectue(session, params):
    s3_file = athena_to_s3(session, params)
    print(s3_file)
    if s3_file:
        df = s3_to_pandas(session, params, s3_file)
    else:
        return None
    cleanup(session, params)
    return df