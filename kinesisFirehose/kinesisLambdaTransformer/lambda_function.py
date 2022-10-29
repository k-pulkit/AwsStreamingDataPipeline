# import boto3
import base64
import json
from helper import get_sentiment

"""
For execution in AWS lambda env.
Set the env variable to /opt/python/nltk_data : NLTK_DATA
"""

print("Starting function execution")

def lambda_handler(event, context):
    
    output = []
    
    # Dropping the use of comprehend service as way too costly for the value it provides
        # comprehend = boto3.client(service_name='comprehend', region_name='us-east-1')
        # def get_sentiment(text):
        #     sentiment_all = comprehend.detect_sentiment(Text=text, LanguageCode='en')
        #     sentiment = sentiment_all['Sentiment']
        #     # calculation for sentiment score, as most are neutral
        #     total = sentiment_all['SentimentScore']['Positive'] - sentiment_all['SentimentScore']['Negative']
        #     # return results
        #     return sentiment, total, sentiment_all['SentimentScore']['Positive'], sentiment_all['SentimentScore']['Negative']
        
    for record in event['records']:
        recordId = record['recordId']
        print(record['data'])
        data = json.loads(base64.b64decode(record['data']).decode('utf-8').strip())    # loads data as a python dict
        text = data.pop('text')
        hashcash = data['tweet_meta']['hashcashtags'] + data.get('reference_tweets', {}).get('hashcashtags', [])
        result = 'Ok'
        
        # logic to drop the frame
        if (data['tweet_meta']['sensitive']) or (len(hashcash) == 0):
            result = 'Dropped'
            print(f"Dropping the record: tweet_id: {data['tweet_id']} of tweet_type {data['tweet_meta']['tweet_type']}")
            
        if result == 'Ok':
            # call the amazon comprehend service
            print("Invoking the sentiment function")
            # get the sentiment for the tweet text
            try:
                s, t, p, n = get_sentiment(text)
                data['tweet_text'] = {
                'text': text,
                'sentiment': s,
                'total': t,
                'positive': p,
                'negative': n
                }
                print("Sentiment Analysis Done")
                print(data['tweet_text'])
                
                # get sentiment for nested tweet
                if data.get('reference_tweets', None) is not None:
                    text = data['reference_tweets'].pop('text')
                    s, t, p, n = get_sentiment(text)
                    data['reference_tweets']['tweet_text'] = {
                        'text': text,
                        'sentiment': s,
                        'total': t,
                        'positive': p,
                        'negative': n
                        }
                    print("Sentiment Analysis Done for nested tweet")
                    print(data['reference_tweets']['tweet_text'])
                
            except Exception as e:
                print("Could not get the sentiment")
                print(e)
                result = 'ProcessingFailed'
                data['tweet_text'] = {'text': text}
            
        else:
            data['tweet_text'] = {'text': text}
        
        # make the data base64 compatible
        _data =  json.dumps(data) + '\n'   # So that all records are in different lines
        data = base64.b64encode(_data.encode('UTF-8'))    # .decode('utf-8')
        
        output_record = {
            'recordId': recordId,
            'result': result,
            'data': data
        }
        
        output.append(output_record)
    
    print(output)
    return {'records': output}
        
