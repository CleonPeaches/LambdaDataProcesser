__name__ = 'lambda_function'

import json
import boto3
import awswrangler as wr
import pandas
import os
from time import gmtime, strftime, sleep

s3_resource = boto3.resource('s3')
s3_client = boto3.client('s3')
glue_client = boto3.client('glue')
ssm_client = boto3.client('ssm')

def get_resources(event):
    source_key = str(event['Records'][0]['s3']['object']['key'])
    dest_bucket_string = os.environ['destination_bucket']
    source_name = source_key.split('/')[-3]
    return {
        'source_bucket_string': os.environ['source_bucket'],
        'source_key': source_key,
        'destination_bucket': s3_resource.Bucket(dest_bucket_string),
        'dest_bucket_string': dest_bucket_string,
        'exile_bucket_string': os.environ['exile_bucket'],
        'source_name': source_name,
        'source_object_name': source_key.split('/')[-2],
        'column_partition': ['created_date'],
        'prefix': source_key.split('/')[-3] + '/' + source_key.split('/')[-2] + '/'
    }
  
def get_tags(source_name, object_name):
    tag_set = []
    path = '/' + source_name + '/' + object_name
    tags = ssm_client.get_parameters_by_path(
        Path=path,
        Recursive=False
    )
    if len(tags['Parameters']) == 0:
        raise ValueError('This object has no corresponding tags.')
    else:
        for key in tags['Parameters']:
            tag_name = key['Name'].split('/')[-1]
            tag_set.append({'Key': tag_name, 'Value': key['Value']})
        return tag_set
    
def try_get_tags(resources):
    try:
        tag_set = get_tags(resources['source_name'], resources['source_object_name'])
    except ValueError as e:
        if str(e) == 'This object has no corresponding tags.':
            exile_object(resources)
        raise e
    else:
        return tag_set

def exile_object(resources):
    copy_source = {
        'Bucket': resources['source_bucket_string'],
        'Key': resources['source_key']
    }
    exile_bucket = s3_resource(resources['exile_bucket-string'])
    exile_bucket.copy(copy_source, resources['source_key'])

    
def convert_to_data_frame(source_bucket_string, source_key):
    content_object = s3_resource.Object(source_bucket_string, source_key)
    file_content = content_object.get()['Body'].read().decode('windows-1252').strip()
    json_content = json.loads(file_content)
    data_frame = pandas.json_normalize(json_content)
    data_frame['ingestion_timestamp'] = str(strftime("%Y-%m-%d %H:%M:%S", gmtime()))
    return data_frame
    
def get_path(source_name, object_name, dest_bucket_string):
    path = ('s3://' + dest_bucket_string + '/' +
                      source_name + '/' +
                      object_name + '/')
    return path
    
def convert_to_parquet(data_frame, path, partition, database='', cpus=1):
    compression_message = wr.pandas.to_parquet(
            dataframe=data_frame,
            database=database,
            path=path,
            partition_cols=partition,
            procs_cpu_bound=cpus
        )
    return compression_message

def tag_objects(bucket_string, prefix, tag_set):
    messages = []
    for key in s3_client.list_objects(Bucket=bucket_string, Prefix=prefix)['Contents']:
        messages.append(s3_client.put_object_tagging(Bucket=bucket_string,
                                                     Key=key['Key'],
                                                     Tagging={'TagSet': tag_set}))
    return messages
        
def lambda_handler(event, context):
    resources = get_resources(event)
    tag_set = try_get_tags(resources)
    data_frame = convert_to_data_frame(resources['source_bucket_string'], resources['source_key'])
    path = get_path(resources['source_name'], resources['source_object_name'], resources['dest_bucket_string'])
    compression_message = convert_to_parquet(data_frame, path, resources['column_partition'])
    tag_messages = tag_objects(resources['dest_bucket_string'], resources['prefix'], tag_set)
    return {
        'statusCode': 200,
        'body': json.dumps({
            'Objects compressed': len(compression_message),
            'Objects tagged': len(tag_messages)
        })
    }