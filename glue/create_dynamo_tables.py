# type: ignore 

import boto3
from botocore.exceptions import ClientError

if __name__ == '__main__':
    # create connection
    dynamodb = boto3.resource('dynamodb')

    # create a new table
    """
    Table 1 - Contains the data used for plotting the Section 1 charts in front-end
    , which is the leaderboard, where we show which stocks have more mentions etc.
    """
    try:
        TABLENAME = "pknn-twitter-capstone-project-table-1"
        print(f"Table creation in progress - {TABLENAME}")
        dynamodb.create_table( TableName=TABLENAME,  # type: ignore
            AttributeDefinitions=[
                {
                    'AttributeName': 'TICKER',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'DetailLevel',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'TickerDetail',
                    'AttributeType': 'S'
                },
                 {
                    'AttributeName': 'TIMESTAMP',
                    'AttributeType': 'S'
                }
                    ],
            KeySchema=[
                {
                    'AttributeName': 'DetailLevel',
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
                            'AttributeName': 'TickerDetail',
                            'KeyType': 'HASH'
                        },
                        {
                            'AttributeName': 'TIMESTAMP',
                            'KeyType': 'RANGE'
                        }
                        ],
                    'Projection': {
                        'ProjectionType': 'ALL'
                    }
                }
                    ],
            BillingMode='PAY_PER_REQUEST'
        )
        
        table = dynamodb.Table(TABLENAME)
        table.wait_until_exists()
        print(f"Table created - {TABLENAME}\n")
        
        # Initally CREATING, then ACTIVE
        print(table.table_status)
    except ClientError as e:
        if e.response['Error']['Code'] == "ResourceInUseException":
            print(f"Table exists - {TABLENAME}\n")
        else:
            raise Exception(e)
        
    # create a new table
    """
    Table 2 - Contains the data used for plotting the Section 1 charts in front-end
    , which is the leaderboard, where we show which stocks have more mentions etc.
    """
    try:
        TABLENAME = "pknn-twitter-capstone-project-table-2"
        print(f"Table creation in progress - {TABLENAME}")
        dynamodb.create_table( TableName=TABLENAME,  # type: ignore
            AttributeDefinitions=[
                {
                    'AttributeName': 'PARTITION',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'TIMESTAMP',
                    'AttributeType': 'S'
                }
                    ],
            KeySchema=[
                {
                    'AttributeName': 'PARTITION',
                    'KeyType': 'HASH'
                },
                {
                    'AttributeName': 'TIMESTAMP',
                    'KeyType': 'RANGE'
                }
                    ],
            BillingMode='PAY_PER_REQUEST'
        )
        
        table = dynamodb.Table(TABLENAME)
        table.wait_until_exists()
        print(f"Table created - {TABLENAME}\n")
        
        # Initally CREATING, then ACTIVE
        print(table.table_status)
    except ClientError as e:
        if e.response['Error']['Code'] == "ResourceInUseException":
            print(f"Table exists - {TABLENAME}\n")
        else:
            raise Exception(e)
    # create a new table
    """
    Table 3 - Contains the data used for metrics
    """
    try:
        TABLENAME = "pknn-twitter-capstone-project-table-3"
        print(f"Table creation in progress - {TABLENAME}")
        dynamodb.create_table( TableName=TABLENAME,  # type: ignore
            AttributeDefinitions=[
                {
                    'AttributeName': 'DetailLevel',
                    'AttributeType': 'S'
                }
                    ],
            KeySchema=[
                {
                    'AttributeName': 'DetailLevel',
                    'KeyType': 'HASH'
                }
                    ],
            BillingMode='PAY_PER_REQUEST'
        )
        
        table = dynamodb.Table(TABLENAME)
        table.wait_until_exists()
        print(f"Table created - {TABLENAME}\n")
        
        # Initally CREATING, then ACTIVE
        print(table.table_status)
    except ClientError as e:
        if e.response['Error']['Code'] == "ResourceInUseException":
            print(f"Table exists - {TABLENAME}\n")
        else:
            raise Exception(e)
        
