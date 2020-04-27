import json
from lambda_function import main

def lambda_handler(event, context):
    main(event, context)
    return {
        'statusCode': 200,
        'body': json.dumps({
            'Message': 'Successfully executed'
        })
    }