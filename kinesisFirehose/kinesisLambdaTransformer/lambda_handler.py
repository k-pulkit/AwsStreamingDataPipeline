import boto3
import base64
import json

def lambda_handler(event, context):
    
    output = []
    
    for record in event['records']:
        print(record)
        recordId = record['recordId']
        data = json.loads(base64.b64decode(record['data']).decode('utf-8').strip())    # loads data as a python dict
        text = data.pop('text')
        result = 'Ok'
        
        # logic to drop the frame
        if data['sensitive']:
            result = 'Dropped'
            
        if result == 'Ok':
            # call the amazon comprehend service
            data['tweet_text'] = {
                'text': text,
                'sentiment': "positive",
                'total': 0.7
            }
            data = base64.b64encode(json.dumps(data).encode('utf-8'))    # .decode('utf-8')
        
        output_record = {
            'recordId': recordId,
            'result': result,
            'data': data
        }
        
        print(output_record)
        
        output.append(output_record)
    
    print(output)
    return {'records': output}

if __name__ == '__main__':
    event = {
            "invocationId": "invocationIdExample",
            "deliveryStreamArn": "arn:aws:kinesis:EXAMPLE",
            "region": "us-east-1",
            "records": [
                {
                "recordId": "49546986683135544286507457936321625675700192471156785154",
                "approximateArrivalTimestamp": 1495072949453,
                "data": "eyJ0ZXh0IjogInRoaXMgaXMgdGV4dCIsICJzZW5zaXRpdmUiOiBmYWxzZX0="
                }
            ]
            }
    
    lambda_handler(event, None)
        
