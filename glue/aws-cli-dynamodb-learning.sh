#! bin/sh

// list all tables
aws dynamodb list-tables --region us-east-1

// create a table
aws dynamodb create-table \
        --table-name demo1 \
            --attribute-definitions AttributeName=month,AttributeType=N AttributeName=count,AttributeType=N \
            --key-schema AttributeName=month,KeyType=HASH AttributeName=count,KeyType=RANGE \
                --billing-mode PAY_PER_REQUEST \
                --region us-east-1

# table status
aws dynamodb describe-table --table-name demo1

# put some items
aws dynamodb put-item --table-name demo1 --item '{
"TICKER": {"S": "AAPL"},
"month": {"N": "2"},
"count": {"N": "10"},
"sentiment": {"S": "NEGATIVE"} }'

# query on HASH key
aws dynamodb query --table-name demo1 \
        --key-condition-expression "#month = :mon" \
            --expression-attribute-names '{"#month": "month"}' \
            --expression-attribute-values '{":mon": {"N": "1"} }'

# query and filter on attribute
aws dynamodb query --table-name demo1 \
        --key-condition-expression "#month = :mon" \
        --filter-expression 'TICKER = :tick' \
            --expression-attribute-names '{"#month": "month"}' \
            --expression-attribute-values '{":mon": {"N": "1"}, ":tick": {"S": "GOOGL"} }'

############# TESTING QUERY ###############
aws dynamodb query --table-name pknn-twitter-capstone-project-table-1 \
        --index-name IndexByTickerTime\
        --no-scan-index-forward\
        --limit 4\
        --key-condition-expression 'TickerDetail = :tick'\
            --expression-attribute-values '{":tick": {"S": "AAPL_HOURLY"}}'

aws dynamodb query --table-name pknn-twitter-capstone-project-table-1 \
        --index-name IndexByTickerTime\
        --no-scan-index-forward\
        --limit 4\
        --key-condition-expression 'TickerDetail = :tick AND #stamp > :time'\
            --expression-attribute-names '{"#stamp": "TIMESTAMP"}'\
            --expression-attribute-values '{":tick": {"S": "AAPL_HOURLY"}, ":time": {"S": "2022-10-30 20:02:19.1250968576"}}'