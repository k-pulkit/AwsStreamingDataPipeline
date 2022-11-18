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
import datetime
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from awsglue.context import GlueContext
from pyspark import SparkContext

from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import DataFrame
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
            
def transformation1(dyf: DynamicFrame, groupValList: list, level_name:str, glue_ctx: GlueContext, name: str, mapping: list) -> DynamicFrame:
        """
        @dyf: The incoming dynamic frame
        @groupVal: The column to group by
        @level_name: Can be like, monthly, weekly etc.
        @glue_ctx: The reference to GlueContext
        @name: Name needed for DataFrame to DynamicFrame Transformation
        @Return: returns the transformed dynamic frame
        This transformation takes in tha Dynamic Frame: cleaned tweets frame, and
        creates a summary table grouping it at level like "month", and calculates the number of 
        positive, negative and neutral tweet count.
        """
        for groupVal in groupValList:
            if groupVal not in ('year', 'month', 'day', 'hour'):
                    raise KeyError("groupVal not valid")
        else:
                # Aggregate the results at Month Level
                df = dyf.toDF()
                df = df.withColumn('TIMESTAMP', df.tweet_meta_created_at.cast(dataType=t.TimestampType()).cast(t.StringType()))\
                        .withColumn('_', f.col('tweet_id').cast(t.StringType()))\
                        .withColumn('_', f.substring(f.col('_'), -10, 10))\
                        .withColumn("TickerDetail", f.concat(f.col('TICKER'), f.lit("_"), f.lit(level_name.upper())))\
                        .withColumn('TIMESTAMP', f.concat(f.col('TIMESTAMP'), f.lit("."), f.col('_') ))
                df = df.groupBy(*groupValList, 'ticker', 'TickerDetail')\
                        .agg(\
                                f.min(f.col('TIMESTAMP')).alias('TIMESTAMP'),\
                                f.sum(f.lit(1)).alias('mentions'), \
                                f.sum(f.when(f.col('tweet_text_sentiment') == 'POSITIVE', 1).otherwise(0)).alias('positive'),\
                                f.sum(f.when(f.col('tweet_text_sentiment') == 'NEGATIVE', 1).otherwise(0)).alias('negative'),\
                                f.sum(f.when(f.col('tweet_text_sentiment') == 'NEUTRAL', 1).otherwise(0)).alias('neutral')
                            )\
                        .withColumn("DetailLevel", f.concat_ws("-", *(f.col(name) for name in groupValList)))
                # convert to DynamicFrame and return
                out = DynamicFrame.fromDF(df, glue_ctx=glue_ctx, name=name)
                if mapping is None:
                        return out
                else:
                        x = [("DetailLevel", "string", "DetailLevel", "string")]
                        return out.apply_mapping(x+mapping)
                
def transformation2(dyfList: list, glue_ctx: GlueContext, name: str) -> DynamicFrame:
        """
        @dyfList: The incoming dynamic frames, as a list
        @glue_ctx: The reference to GlueContext
        @name: Name needed for DataFrame to DynamicFrame Transformation
        @Return: returns the combined(union) dynamic frame
        This transformation takes in tha Dynamic Frames: performs union and returns output as single DynamicFrame
        """
        if dyfList.__class__ is not list:
            raise ValueError("Expecting list of dynamicFrames for first argument")
        else:
                # Aggregate the results at Month Level
                df = dyfList[0].toDF()  # convert to spark DF
                for i in range(1, len(dyfList)):
                    x = dyfList[i].toDF()
                    df = df.union(x)
                
                # convert to DynamicFrame and return
                return DynamicFrame.fromDF(df, glue_ctx=glue_ctx, name=name)
        
def transformation3(dyf: DynamicFrame, glue_ctx: GlueContext, name: str, mapping: list, num_partitions=4) -> DynamicFrame:
        """
        @dyfList: The incoming dynamic frame
        @glue_ctx: The reference to GlueContext
        @name: Name needed for DataFrame to DynamicFrame Transformation
        @Return: DynamicFrame for recent tweets
        This transformation takes in tha Dynamic Frames: and returns data for recent tweets viz
        """
        if dyf.__class__ is not DynamicFrame:
            raise ValueError("Expecting a dynamicFrames for first argument")
        else:
                # Aggregate the results at Month Level
                df = dyf.toDF()
                df = df.select('tweet_id', 'ticker', 'tweet_text_sentiment', \
                               'tweet_text_text', 'tweet_text_clean', 'tweet_meta_created_at', 'tweet_text_sentiment')
                # Select the top 500 tweets only, as output will show these many at most
                df = df.orderBy(f.desc('tweet_meta_created_at')).limit(500)
                # Create 2 columns, TIMESTAMP AND PARTITION
                # FOR TIMESTAMP, to make it unique we append the twitter id's last 10 digits
                df_ = df.withColumn('TIMESTAMP', df.tweet_meta_created_at.cast(dataType=t.TimestampType()).cast(t.StringType()))\
                        .withColumn('_', f.col('tweet_id').cast(t.StringType()))\
                        .withColumn('_', f.substring(f.col('_'), -10, 10))\
                        .withColumn('TIMESTAMP', f.concat(f.col('TIMESTAMP'), f.lit("."), f.col('_') ))\
                        .withColumn('PARTITION', (f.col('tweet_id').cast(t.LongType()) % f.lit(num_partitions)).cast(t.StringType()))
                
                # Map to types needed and return DynamicFrame            
                out = DynamicFrame.fromDF(df_, glue_ctx=glue_ctx, name=name)
                return out.apply_mapping(mapping)
        
def transformation4(dyf: DynamicFrame, groupValList: list, level_name:str, glue_ctx: GlueContext, name: str, mapping: list) -> DynamicFrame:
        """
        @dyf: The incoming dynamic frame
        @groupVal: The column to group by
        @level_name: Can be like, monthly, weekly etc.
        @glue_ctx: The reference to GlueContext
        @name: Name needed for DataFrame to DynamicFrame Transformation
        @Return: returns the transformed dynamic frame
        This transformation forms the base table for metric level calculations
        Similar to transformation1, but less groups
        """
        for groupVal in groupValList:
            if groupVal not in ('year', 'month', 'day', 'hour'):
                    raise KeyError("groupVal not valid")
        else:
                # Aggregate the results at Month Level
                df = dyf.toDF()
                df = df.withColumn('LEVEL', f.lit(level_name.upper()))\
                        .groupBy(*groupValList, 'LEVEL')\
                        .agg(\
                                f.sum(f.lit(1)).alias('mentions'), \
                                f.sum(f.when(f.col('tweet_text_sentiment') == 'POSITIVE', 1).otherwise(0)).alias('positive'),\
                                f.sum(f.when(f.col('tweet_text_sentiment') == 'NEGATIVE', 1).otherwise(0)).alias('negative'),\
                                f.sum(f.when(f.col('tweet_text_sentiment') == 'NEUTRAL', 1).otherwise(0)).alias('neutral')
                            )\
                        .withColumn("DetailLevel", f.concat_ws("-", *(f.col(name) for name in groupValList)))
                # convert to DynamicFrame and return
                out = DynamicFrame.fromDF(df, glue_ctx=glue_ctx, name=name)
                if mapping is None:
                        return out
                else:
                        x = [("LEVEL", "string", "LEVEL", "string"), ("DetailLevel", "string", "DetailLevel", "string")]
                        return out.apply_mapping(x+mapping)
        
        
if __name__ == "__main__":
        # Get the glue context and spark object from the initalizer class
        helper = GlueInitializer()
        spark = helper.spark
        GlueContext = helper.glueContext
        
        # Quite the spark logs
        quiet_logs(spark.sparkContext)
        
        ############################################ Code for Job3 below ##################################################
        ### DATASOURCES BELOW
        DATABASE_SOURCE_0 = "twitter_capstone_project_stagezone_stage2"
        TABLE_SOURCE_0_0 = "cleaned_tweets_parquet"
        ## DYNAMO TABLES
        DDB_TABLE1 = "pknn-twitter-capstone-project-table-1"
        DDB_TABLE2 = "pknn-twitter-capstone-project-table-2"
        DDB_TABLE3 = "pknn-twitter-capstone-project-table-3"
        
        try:
                ### TRANSFORMATIONS BELOW
                #@ TableName - cleaned_tweets_parquet
                # LOADTYPE - INCREMENTAL
                # Using pushdown predicate to only process upto Current Hour - 2
                pred = datetime.datetime.utcnow().strftime("year <= %Y and month <= %m and day <= %d and hour < %H")
                print("Using the predicate while reading - ", pred)
                dyf = GlueContext.create_dynamic_frame_from_catalog(database=DATABASE_SOURCE_0,\
                                                                        table_name=TABLE_SOURCE_0_0,\
                                                                        transformation_ctx='datasource0',\
                                                                        push_down_predicate=pred,\
                                                                        additional_options={'groupFiles': helper.groupFilesStrategy, 'groupSize': helper.groupSize})
                
                # Check if no new data is available
                if dyf.toDF().rdd.isEmpty():
                        print("Exiting as no new data to process")
                        sys.exit()
                
                print("Schema of the input dataframes")
                dyf.printSchema()
                
                """
                PART 1 of Job3, for the trending charts
                Grouped at levels: [year], [year, month], [year, month, day], [year, month, day, hour]
                """
                print("Creating Summary Tables at different DetailLevels")
                base_mapping = [('ticker', 'string', 'TICKER', 'string'),
                                ('TickerDetail', 'string', 'TickerDetail', 'string'),
                                ('TIMESTAMP', 'string', 'TIMESTAMP', 'string'),
                                ('mentions', 'long', 'NUM_MENTIONS', 'long'),
                                ('positive', 'long', 'NUM_POSITIVE', 'long'),
                                ('negative', 'long', 'NUM_NEGATIVE', 'long'),
                                ('neutral', 'long', 'NUM_NEUTRAL', 'long')]
                dyf_1_year = transformation1(dyf, ["year"], "yearly", GlueContext, "transform1", base_mapping)
                dyf_1_month = transformation1(dyf, ["year", "month"], "monthly", GlueContext, "transform1", base_mapping)
                dyf_1_day = transformation1(dyf, ["year", "month", "day"], "daily", GlueContext, "transform1", base_mapping)
                dyf_1_hour = transformation1(dyf, ["year", "month", "day", "hour"], "hourly", GlueContext, "transform1", base_mapping)
                
                # Combine these DynamicFrames
                _ = [dyf_1_year, dyf_1_month, dyf_1_day, dyf_1_hour]
                dyf_1 = transformation2(_, GlueContext, "transform2")
                
                print("All summarized tables created for Chart-1")
                
                """
                PART 2 of Job3, for the recent tweets charts
                Will be partitioned by PARTITION, and sorted at TIMESTAMP
                """
                NUM_PARTITIONS = 5
                mapping = [ ('PARTITION', 'string', 'PARTITION', 'string'),
                ('TIMESTAMP', 'string', 'TIMESTAMP', 'string'),
                ('tweet_id', 'string', 'ID', 'string'),
                ('ticker', 'string', 'TICKER', 'string'),
                ('tweet_text_sentiment', 'string', 'SENTIMENT', 'string'),
                ('tweet_text_text', 'string', 'TEXT', 'string'),
                ('tweet_text_clean', 'string', 'CLEANTEXT', 'string')]
                dyf_2 = transformation3(dyf, GlueContext, "transformation3", mapping, NUM_PARTITIONS)
                dyf_2.printSchema()
                print("Created table for Chart-2")
                
                """
                PART 3 of Job3, for the top dashboard metrics
                Fetch the metrics associated with tweets
                """
                base_mapping = [
                                ('mentions', 'long', 'NUM_MENTIONS', 'long'),
                                ('positive', 'long', 'NUM_POSITIVE', 'long'),
                                ('negative', 'long', 'NUM_NEGATIVE', 'long'),
                                ('neutral', 'long', 'NUM_NEUTRAL', 'long')]
                dyf_3_year = transformation4(dyf, ["year"], "yearly", GlueContext, "transform1", base_mapping)
                dyf_3_month = transformation4(dyf, ["year", "month"], "monthly", GlueContext, "transform1", base_mapping)
                dyf_3_day = transformation4(dyf, ["year", "month", "day"], "daily", GlueContext, "transform1", base_mapping)
                dyf_3_hour = transformation4(dyf, ["year", "month", "day", "hour"], "hourly", GlueContext, "transform1", base_mapping)
                # Combine into 1 DynamicFrame
                _ = [dyf_3_year, dyf_3_month, dyf_3_day, dyf_3_hour] 
                df_3 = transformation2(_, GlueContext, "transform2_").toDF()
                # Window function
                window = Window.partitionBy(f.col('LEVEL')).orderBy(f.desc("DetailLevel"))
                df_3 = df_3.withColumn("sno", f.row_number().over(window))\
                                .filter('sno <= 2')\
                                .withColumn("D_NUM_MENTIONS", f.coalesce(f.lead(f.col('NUM_MENTIONS')).over(window), f.lit(0)))\
                                .withColumn("D_NUM_POSITIVE", f.coalesce(f.lead(f.col('NUM_POSITIVE')).over(window), f.lit(0)))\
                                .withColumn("D_NUM_NEGATIVE", f.coalesce(f.lead(f.col('NUM_NEGATIVE')).over(window), f.lit(0)))\
                                .withColumn("D_NUM_NEUTRAL", f.coalesce(f.lead(f.col('NUM_NEUTRAL')).over(window), f.lit(0)))\
                                .filter('sno = 1')\
                                .drop('sno', 'DetailLevel')
                dyf_3 = DynamicFrame.fromDF(df_3, glue_ctx=GlueContext, name="metric")
                print("Created table for Metrics")
                
                
                # DATASINK 1
                #@ TARGET - DDBTABLE-1
                #About - Allows search for a particular month, year, or hour, based on input query
                print("Sinking the calculated tables to target DDB")
                
                Datasink1 = GlueContext.write_dynamic_frame_from_options(
                                                frame=dyf_1,
                                                connection_type="dynamodb",
                                                connection_options={
                                                        "dynamodb.output.tableName": DDB_TABLE1
                                                        }
                                                )
                print("Data written to Dynamo, table 1")
                
                # DATASINK 2
                #@ TARGET - DDBTABLE-2
                #About - Allows search for a particular month, year, or hour, based on input query
                Datasink2 = GlueContext.write_dynamic_frame_from_options(
                                                frame=dyf_2,
                                                connection_type="dynamodb",
                                                connection_options={
                                                        "dynamodb.output.tableName": DDB_TABLE2
                                                        }
                                                )
                print("Data written to Dynamo, table 2")
                
                # DATASINK 3
                #@ TARGET - DDBTABLE-2
                #About - Allows search for a particular month, year, or hour, based on input query
                Datasink2 = GlueContext.write_dynamic_frame_from_options(
                                                frame=dyf_3,
                                                connection_type="dynamodb",
                                                connection_options={
                                                        "dynamodb.output.tableName": DDB_TABLE3
                                                        }
                                                )
                print("Data written to Dynamo, table 3")
                
                # glue commit for bookmark progress
                helper.commit()
        except SystemExit:
                print("Exit from program. Execution stopped.")
                
                

                

