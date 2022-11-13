# type: ignore 

import boto3
from botocore.exceptions import ClientError

if __name__ == '__main__':
    # create connection
    dynamodb = boto3.resource('dynamodb')

    # create a new table
    try:
        dynamodb.create_table( TableName='demo2',  # type: ignore
            AttributeDefinitions=[
                {
                    'AttributeName': 'TICKER',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'MONTH',
                    'AttributeType': 'N'
                }
                    ],
            KeySchema=[
                {
                    'AttributeName': 'MONTH',
                    'KeyType': 'HASH'
                },
                {
                    'AttributeName': 'TICKER',
                    'KeyType': 'RANGE'
                }
                    ],
            GlobalSecondaryIndexes=[
                {
                    'IndexName': 'IndexByTickerTime',
                    'KeySchema': [
                        {
                            'AttributeName': 'TICKER',
                            'KeyType': 'HASH'
                        },
                        {
                            'AttributeName': 'MONTH',
                            'KeyType': 'RANGE'
                        }
                        ],
                    'Projection': {
                        'ProjectionType': 'KEYS_ONLY'
                    }
                }
                    ],
            BillingMode='PAY_PER_REQUEST'
        )
        
        table = dynamodb.Table('demo2')
        table.wait_until_exists()
        print("Table created")
        
        # Initally CREATING, then ACTIVE
        print(table.table_status)
    except ClientError as e:
        if e.response['Error']['Code'] == "ResourceInUseException":
            print("Table exists")
        else:
            raise Exception(e)