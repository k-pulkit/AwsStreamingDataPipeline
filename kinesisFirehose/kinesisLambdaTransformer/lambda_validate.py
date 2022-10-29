
from lambda_function import lambda_handler
import json
import base64

if __name__ == '__main__':
    
    data1 = {
            'tweet_id': 1,
            'author_id': 34354334,
            'text': "tweet.text",
            'tweet_meta': {
                'created_at': 1666915198.0,     # convert timestamp to utc epoch
                                                                                             # datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
                'tweet_type': 'original',
                'sensitive': False,
                'hashcashtags': ['V_CHRISTMASTREE', 'V', '방탄소년단뷔', 'BTSV']
            } }
    
    data2 = {
            'tweet_id': 2,
            'author_id': 34354334,
            'text': "tweet.text",
            'tweet_meta': {
                'created_at': 1666915198.0,     # convert timestamp to utc epoch
                                                                                             # datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
                'tweet_type': 'original',
                'sensitive': True,
                'hashcashtags': ['V_CHRISTMASTREE', 'V', '방탄소년단뷔', 'BTSV']
            } }
    
    data3 = {
            'tweet_id': 2,
            'author_id': 34354334,
            'text': "tweet.text",
            'tweet_meta': {
                'created_at': 1666915198.0,     # convert timestamp to utc epoch
                                                                                             # datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
                'tweet_type': 'replied_to',
                'sensitive': False,
                'hashcashtags': ['V_CHRISTMASTREE', 'V', '방탄소년단뷔', 'BTSV']
            },
            'reference_tweets': {
              'tweet_id': 'x',
              'tweet_type': 'original',
              'text': 'ref text',
              'hashcashtags': ['TSLA']
            } 
            }
    
    bdata1 = base64.b64encode(json.dumps(data1).encode('UTF-8'))
    bdata2 = base64.b64encode(json.dumps(data2).encode('UTF-8'))
    bdata3 = base64.b64encode(json.dumps(data3).encode('UTF-8'))
    
    print("Data encoded")
    
    test_event = {
            "invocationId": "invocationIdExample",
            "deliveryStreamArn": "arn:aws:kinesis:EXAMPLE",
            "region": "us-east-1",
            "records": [
                {
                "recordId": "r1",
                "approximateArrivalTimestamp": 1495072949453,
                "data": bdata1
                },
                {
                "recordId": "r2",
                "approximateArrivalTimestamp": 1495072949453,
                "data": bdata2
                },
                {
                "recordId": "r3",
                "approximateArrivalTimestamp": 1495072949453,
                "data": bdata3
                }
            ]
            }
    
    lambda_handler(test_event, None)