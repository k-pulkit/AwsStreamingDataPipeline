# type:ignore

"""
Name - Job-3
GlueJobName - pknn-aws-twitter-Job-3-aggregateData
Description - This python script makes use of pyspark and glue api
This data is to be executed on the stage2 data, after it is crawled.
The purpose of the job is to aggregate the data, to produce results that can be displayed to the user

We group the data at different levels to produce the following views -
1. Top Trending Stocks Per Hour / Per Day, Per Week, Per Month
        This table has the following columns
        {Month, count} : [TICKER, count_pos, count_neg, count_neu]
        Partitioned by Month, Sorted by count
2. Recent Tweets for each ticker
        {TICKER+SENTIMENT, timestamp} : [text]
        ==> We want to run a job to cleanup this table, as we only want 1 day of tweets in this table
3. Stock Timeseries
        {TICKER, Month} : [count, count_pos, count_neu, count_neg]

This script requires that the Glue catalog tables are refreshed before running the script, as 
only the partitions present in the catalog are considered for processing.
"""