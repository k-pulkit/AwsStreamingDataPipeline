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

import sys
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from awsglue.context import GlueContext
from pyspark import SparkContext

from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as f
from pyspark.sql import types as t
from pyspark.sql import Window

def quiet_logs(sc):
        logger = sc._jvm.org.apache.log4j
        logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )
        logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )

class GlueInitializer(object):
    def __init__(self, ) -> None:
            self.groupSize = 1048576
            self.groupFilesStrategy = 'inPartition'
            self.format = 'parquet'
            self.compression = 'snappy'
            self.args = getResolvedOptions(sys.argv, ['JOB_NAME'])
            self.sc = SparkContext.getOrCreate()
            quiet_logs(self.sc)    # quite the logs
            self.glueContext = GlueContext(self.sc)
            self.spark = self.glueContext.spark_session
            self.job = Job(self.glueContext)
            self.job.init(self.args['JOB_NAME'], self.args)
            print("Spark has been initialized")
        
    def commit(self):
            self.job.commit() 
    
if __name__ == "__main__":
        # Get the glue context and spark object from the initalizer class
        helper = GlueInitializer()
        spark = helper.spark
        GlueContext = helper.glueContext
        
        quiet_logs(spark.sparkContext)
        
        ## Code for Job3 below
        DATABASE_SOURCE_0 = "twitter_capstone_project_stagezone_stage2"
        TABLE_SOURCE_0_0 = "cleaned_tweets_parquet"
        
        dyf = GlueContext.create_dynamic_frame_from_catalog(database=DATABASE_SOURCE_0,\
                                                                table_name=TABLE_SOURCE_0_0,\
                                                                transformation_ctx='datasource0',\
                                                                push_down_predicate="day=30",\
                                                                additional_options={'groupFiles': helper.groupFilesStrategy, 'groupSize': helper.groupSize})
        
        dyf.printSchema() 
        dyf.toDF().select('possible_spam').distinct().show()
        
        
        
        df = dyf.toDF().groupBy('month', 'ticker')\
                .agg(\
                     f.sum(f.lit(1)).alias('mentions'), \
                     f.sum(f.when(f.col('tweet_text_sentiment') == 'POSITIVE', 1).otherwise(0)).alias('positive'),\
                     f.sum(f.when(f.col('tweet_text_sentiment') == 'NEGATIVE', 1).otherwise(0)).alias('negative'),\
                     f.sum(f.when(f.col('tweet_text_sentiment') == 'NEUTRAL', 1).otherwise(0)).alias('neutral')
                   )
                
        df.show(4)
        
        dyf2 = DynamicFrame.fromDF(df, glue_ctx=GlueContext, name='out')
        dyf2.printSchema()
        
        dyf3 = dyf2.apply_mapping([
                ('month', 'string', 'MONTH', 'long'),
                ('ticker', 'string', 'TICKER', 'string'),
                ('mentions', 'long', 'NUM_MENTIONS', 'long'),
                ('positive', 'long', 'NUM_POSITVE', 'long'),
                ('negative', 'long', 'NUM_NEGATIVE', 'long'),
                ('neutral', 'long', 'NUM_NEUTRAL', 'long')
                ])
        
        dyf3.printSchema()
        
        Datasink1 = GlueContext.write_dynamic_frame_from_options(
        frame=dyf3,
        connection_type="dynamodb",
        connection_options={
                "dynamodb.output.tableName": "demo2"
                 }
        )
        
        print("Data written to Dynamo")
        
        

        

