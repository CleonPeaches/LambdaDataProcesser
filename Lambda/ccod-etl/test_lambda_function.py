'''
Resources used: 
https://towardsdatascience.com/testing-serverless-services-59c688812a0d
https://github.com/vincentclaes/serverless_data_pipeline_example/blob/master/serverless_data_pipeline_tests/lambda_function/test_extract.py
'''

import boto3
import unittest
import os
from moto import mock_s3

from lambda_function import get_resources

@mock_s3
class MockS3(unittest.TestCase):
    STAGING_BUCKET = 'test_bucket_1'
    DESTINATION_BUCKET = 'test_bucket_2'
    EXILE_BUCKET = 'test_bucket_3'
    REGION = 'us-east-123'

    def setUp(self):
        conn = boto3.resource('s3', region_name=self.REGION)
        conn.create_bucket(Bucket=self.STAGING_BUCKET)
        conn.create_bucket(Bucket=self.DESTINATION_BUCKET)
        conn.create_bucket(Bucket=self.EXILE_BUCKET)

    def tearDown(self):
        self.remove_bucket(self.STAGING_BUCKET)
        self.remove_bucket(self.DESTINATION_BUCKET)
        self.remove_bucket(self.EXILE_BUCKET)

    @staticmethod
    def remove_bucket(bucket_name):
        s3_bucket = boto3.resource('s3').Bucket(bucket_name)
        s3_bucket.objects.all().delete()
        s3_bucket.delete()

    @staticmethod
    def get_s3_event(bucket, key):
        return {
            "Records": [
                {
                "eventVersion": "2.1",
                "eventSource": "aws:s3",
                "awsRegion": "us-east-123",
                "eventTime": "2020-04-24T19:07:28.579Z",
                "eventName": "ObjectCreated:Put",
                "userIdentity": {
                    "principalId": "AWS:AIDA5YUDJJJ36MLNG7KAU"
                },
                "requestParameters": {
                    "sourceIPAddress": "222.24.107.21"
                },
                "responseElements": {
                    "x-amz-request-id": "00093EEAA5C7G7F2",
                    "x-amz-id-2": "9tTklyI/OEj"
                },
                "s3": {
                    "s3SchemaVersion": "1.0",
                    "configurationId": "151dfa64",
                    "bucket": {
                    "name": bucket,
                    "ownerIdentity": {
                        "principalId": "A3HBFUQ648EBQ8"
                    },
                    "arn": "arn:aws:s3:::" + bucket
                    },
                    "object": {
                    "key": key,
                    "size": 11,
                    "eTag": "5eb63bbb",
                    "versionId": "qXEJ0x6F6hPNkH5TNJUKbXWlvQ5jIm6U",
                    "sequencer": "0057E75D80IA35C3E0"
                    }
                }
            }
        ]
    }

    def test_get_resources(self):
        source_key = 'salesforce/record_type/test.json'
        event = MockS3.get_s3_event(self.STAGING_BUCKET, source_key)

        resources = get_resources(
            event=event, 
            source_bucket=self.STAGING_BUCKET, 
            dest_bucket=self.DESTINATION_BUCKET, 
            exile_bucket=self.EXILE_BUCKET)
        
        assert resources['source_bucket'] == self.STAGING_BUCKET
        assert resources['source_key'] == source_key
        assert resources['destination_bucket'] == self.DESTINATION_BUCKET
        assert resources['exile_bucket'] == self.EXILE_BUCKET
        assert resources['source_name'] == 'salesforce'
        assert resources['source_object_name'] == 'record_type'
        assert resources['column_partition'] == ['created_date']
        assert resources['prefix'] == 'salesforce/record_type/'

    def test_get_resources_raise_on_bad_key():
        pass


if __name__ == '__main__':
    unittest.main()