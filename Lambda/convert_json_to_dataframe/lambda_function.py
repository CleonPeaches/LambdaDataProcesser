import json
import boto3
import awswrangler as wr
import pandas
import re

s3_resource = boto3.resource('s3')
s3_client = boto3.client('s3')

def get_resources(event):
    destination_bucket = 'ccod-data-lake'
    source_key = str(event['Records'][0]['s3']['object']['key'])
    return {
        'source_bucket_string': str(event['Records'][0]['s3']['bucket']['name']),
        'source_key': source_key,
        'destination_bucket_string': destination_bucket,
        'destination_bucket': s3_resource.Bucket(destination_bucket),
        'source_name': source_key.split('/')[-3],
        'source_object_name': source_key.split('/')[-2],
        'column_partition': ['created_date'],
        'prefix': source_key.split('/')[-3] + '/' + source_key.split('/')[-2] + '/'
    }
    
def try_get_resources(event):
    try:
        resources = get_resources(event)
    except Exception as e:
        raise e
    return resources

def get_tags(object_name):
    tag_set = []
    object_name = object_name.lower()
    if object_name == 'cases':
        tag_set.append(
            {'Key': 'Classification', 'Value': 'Private'},
            {'Key': 'Source', 'Value': 'Salesforce'}
        )
    elif object_name == 'business_process':
        tag_set.append(
            {'Key': 'Classification', 'Value': 'Public'},
            {'Key': 'Source', 'Value': 'Salesforce'}
        )
    elif object_name == 'case_status':
        tag_set.append(
            {'Key': 'Classification', 'Value': 'Public'},
            {'Key': 'Source', 'Value': 'Salesforce'}
        )
    elif object_name == 'community':
        tag_set.append(
            {'Key': 'Classification', 'Value': 'Public'},
            {'Key': 'Source', 'Value': 'Salesforce'}
        )
    elif object_name == 'contact':
        tag_set.append(
            {'Key': 'Classification', 'Value': 'Private'},
            {'Key': 'Source', 'Value': 'Salesforce'}
        )
    elif object_name == 'record_type':
        tag_set.append(
            {'Key': 'Classification', 'Value': 'Public'},
            {'Key': 'Source', 'Value': 'Salesforce'}
        )
    else:
        raise ValueError('This object does not have any associated tags.')
    return tag_set

def try_get_tags(object_name):
    try:
        tags = get_tags(object_name)
    except ValueError as e:
        raise e
    return tags

def tag_objects(bucket_string, prefix, tag_set):
    messages = []
    for key in s3_client.list_objects(Bucket=bucket_string, Prefix=prefix)['Contents']:
        messages.append(s3_client.put_object_tagging(Bucket=bucket_string,
                                                     Key=key['Key'],
                                                     Tagging={'TagSet': tag_set}))
    return messages
                                     
def try_tag_objects(bucket, prefix, tag_set):
    try:
        messages = tag_objects(bucket, prefix, tag_set)
    except Exception as e:
        raise e
    return messages
                     
def convert_to_data_frame(resources):
    content_object = s3_resource.Object(resources['source_bucket_string'], resources['source_key'])
    file_content = content_object.get()['Body'].read()
    json_content = json.loads(file_content).decode('utf-8')
    data_frame = pandas.json_normalize(json_content)
    return data_frame
                     
def try_convert_to_data_frame(resources):
    try:
        data_frame = convert_to_data_frame(resources)
    except Exception as e:
        raise e
    return data_frame
    
def get_path(resources):
    path = ('s3://' + resources['destination_bucket_string'] + '/' +
                      resources['source_name'] + '/' +
                      resources['source_object_name'] + '/')
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
    
def try_convert_to_parquet(data_frame, path, partition):
    try:
        compression_message = convert_to_parquet(data_frame=data_frame, 
                                                 path=path, 
                                                 partition=partition)
    except Exception as e:
        raise e
    return compression_message

def lambda_handler(event, context):
    resources = try_get_resources(event)
    data_frame = try_convert_to_data_frame(resources)
    path = get_path(resources)
    tags = try_get_tags(resources['source_object_name'])
    compression_message = try_convert_to_parquet(data_frame, path, resources['column_partition'])
    tag_messages = try_tag_objects(resources['destination_bucket_string'], resources['prefix'], tags)
    return {
        'statusCode': 200,
        'body': json.dumps({
            'Compressed objects': compression_message,
            'Tagged objects': tag_messages
        })
    }