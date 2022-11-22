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

from time import sleep
from py4j.protocol import Py4JJavaError  # Error when the table in S3 is not created yet
from botocore.exceptions import ClientError
import sys
import boto3
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
            self.runid = self.args["JOB_RUN_ID"]
            self.sc = SparkContext.getOrCreate()
            quiet_logs(self.sc)    # quite the logs
            self.glueContext = GlueContext(self.sc)
            self.spark = self.glueContext.spark_session
            self.job = Job(self.glueContext)
            self.job.init(self.args['JOB_NAME'], self.args)
            print("Spark has been initialized")
            print(f"JOB RUN ID is {self.runid}")
        
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
                df = df.withColumn('RUN_ID', f.lit(helper.runid))
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
                df = df.orderBy(f.desc('tweet_meta_created_at'))                        # .limit(500)
                # Create 2 columns, TIMESTAMP AND PARTITION
                # FOR TIMESTAMP, to make it unique we append the twitter id's last 10 digits
                df_ = df.withColumn('TIMESTAMP', df.tweet_meta_created_at.cast(dataType=t.TimestampType()).cast(t.StringType()))\
                        .withColumn('_', f.col('tweet_id').cast(t.StringType()))\
                        .withColumn('_', f.substring(f.col('_'), -10, 10))\
                        .withColumn('TIMESTAMP', f.concat(f.col('TIMESTAMP'), f.lit("."), f.col('_') ))\
                        .withColumn('PARTITION', (f.col('tweet_id').cast(t.LongType()) % f.lit(num_partitions)).cast(t.StringType()))\
                        .withColumn('tweet_id', f.col('tweet_id').cast(t.StringType()))\
                        .withColumn('RUN_ID', f.lit(helper.runid))
                
                # Map to types needed and return DynamicFrame            
                out = DynamicFrame.fromDF(df_, glue_ctx=glue_ctx, name=name)
                x = [('RUN_ID', 'string', 'RUN_ID', 'string')]
                return out.apply_mapping(mapping+x)
        
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
        ## S3 incremental data groups
        MARKER = "BOOKMARK"
        CRAWLER_SOURCE_1 = "pknn-twitter-capstone-project-crawler-analytics_tables" 
        DATABASE_SOURCE_1 = "twitter_capstone_project_analyticszone"
        # Tables in s3 are store with same name as Dynamo
        TABLE_SOURCE_1_0, S3_LOCATION_10 = DDB_TABLE1, f"s3://pknn-aws-twitter-analytics-zone/data/{DDB_TABLE1}"
        TABLE_SOURCE_1_1, S3_LOCATION_11 = DDB_TABLE2, f"s3://pknn-aws-twitter-analytics-zone/data/{DDB_TABLE2}"
        TABLE_SOURCE_1_2, S3_LOCATION_12 = DDB_TABLE3, f"s3://pknn-aws-twitter-analytics-zone/data/{DDB_TABLE3}"
        
        # FLAG - whether to combine history data with current batch or not (will be false for first run when tables do not exist)
        FLAG_COMBINE_WITH_S3 = False # Turns true when bookmark is present
        FLAG_ENABLE_DYNAMO_LOAD = True # for testing is false
        
        try:
                """
                We are not using incrmental processing for this Job.
                As a last priority, will try to include the incremental logic.
                Idea -
                For previous RUNS - Read the incremental data (Data stored as TableName/RunID)
                For current RUN - Create tables
                                  Store the data for current Run to S3
                Group current and historic Data
                Write to Dynamo Tables
                Run Crawler for incremental tables in S3
                """
                ### TRANSFORMATIONS BELOW
                #@ TableName - cleaned_tweets_parquet
                # LOADTYPE - INCREMENTAL 
                # Using pushdown predicate to only process upto Current Hour - 2
                # pred = datetime.datetime.utcnow().strftime("year <= %Y and month <= %m and day < %d")   #  and hour < %H
                # Predicate to get all historic month data, and for until previous data in current month
                pred = datetime.datetime.utcnow().strftime("(year <= %Y AND month < %m) OR (year = %Y AND month = %m AND day < %d)")   #  and hour < %H
                #pred = pred if "test" not in helper.runid else "day = 30 and hour = 16"
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
                else:
                        print("Schema of the input dataframes")
                        dyf.printSchema()
                        
                """
                Data for previous runs contain batch summaries that we need to aggregate.
                So, we need to update the tables for new or updated information before running
                """
                
                # Now load the summary for past runs
                # TYPE - CONDITIONAL
                try:
                        s3 = boto3.client("s3")
                        bookmark = s3.get_object(Bucket="pknn-aws-twitter-analytics-zone", Key = MARKER)['Body'].read().decode("utf-8")
                        print(f"Reading from bookmark - {bookmark}")
                        # read the required partition
                        dyf_table1 = GlueContext.create_dynamic_frame.from_options(connection_type="s3",
                                                        connection_options={"path": f"{S3_LOCATION_10}/RUN_ID={bookmark}/",
                                                                        'groupFiles': helper.groupFilesStrategy,
                                                                        'groupSize': helper.groupSize},
                                                        format='parquet',
                                                        format_options={'compression': helper.compression})
                        dyf_table2 = GlueContext.create_dynamic_frame.from_options(connection_type="s3",
                                                        connection_options={"path": f"{S3_LOCATION_11}/RUN_ID={bookmark}/",
                                                                        'groupFiles': helper.groupFilesStrategy,
                                                                        'groupSize': helper.groupSize},
                                                        format='parquet',
                                                        format_options={'compression': helper.compression})
                        dyf_table3 = GlueContext.create_dynamic_frame.from_options(connection_type="s3",
                                                        connection_options={"path": f"{S3_LOCATION_12}/RUN_ID={bookmark}/",
                                                                        'groupFiles': helper.groupFilesStrategy,
                                                                        'groupSize': helper.groupSize},
                                                        format='parquet',
                                                        format_options={'compression': helper.compression})
                        FLAG_COMBINE_WITH_S3 = True
                        print("Data loaded from S3, will be used for aggregation")
                except (Py4JJavaError, ClientError) as e:
                        
                        if e.__class__ == Py4JJavaError:
                                pass
                        elif e.__class__ == ClientError:
                                if e["Error"]["Code"] != "NoSuchKey":
                                        raise e
                        print("No incremental summary tables exist in S3")
                
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
                                ('neutral', 'long', 'NUM_NEUTRAL', 'long'),
                                ]
                dyf_1_year = transformation1(dyf, ["year"], "yearly", GlueContext, "transform1", base_mapping)
                dyf_1_month = transformation1(dyf, ["year", "month"], "monthly", GlueContext, "transform1", base_mapping)
                dyf_1_day = transformation1(dyf, ["year", "month", "day"], "daily", GlueContext, "transform1", base_mapping)
                dyf_1_hour = transformation1(dyf, ["year", "month", "day", "hour"], "hourly", GlueContext, "transform1", base_mapping)
                
                # Combine these DynamicFrames
                _ = [dyf_1_year, dyf_1_month, dyf_1_day, dyf_1_hour]
                dyf_1 = transformation2(_, GlueContext, "transform2")
                dyf_1.printSchema()
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
                ('tweet_text_clean', 'string', 'CLEANTEXT', 'string'),
                        ]
                dyf_2 = transformation3(dyf, GlueContext, "transformation3", mapping, NUM_PARTITIONS)
                dyf_2.printSchema()
                print("Created table for Chart-2, that is recent tweets view")
                
                """
                PART 3 of Job3, for the top dashboard metrics
                Fetch the metrics associated with tweets
                Make use of FULL DATA
                """
                base_mapping = [
                                ('mentions', 'long', 'NUM_MENTIONS', 'long'),
                                ('positive', 'long', 'NUM_POSITIVE', 'long'),
                                ('negative', 'long', 'NUM_NEGATIVE', 'long'),
                                ('neutral', 'long', 'NUM_NEUTRAL', 'long'),
                                ]
                dyf_3_year = transformation4(dyf, ["year"], "yearly", GlueContext, "transform1", base_mapping)
                dyf_3_month = transformation4(dyf, ["year", "month"], "monthly", GlueContext, "transform1", base_mapping)
                dyf_3_day = transformation4(dyf, ["year", "month", "day"], "daily", GlueContext, "transform1", base_mapping)
                dyf_3_hour = transformation4(dyf, ["year", "month", "day", "hour"], "hourly", GlueContext, "transform1", base_mapping)
                # Combine into 1 DynamicFrame
                _ = [dyf_3_year, dyf_3_month, dyf_3_day, dyf_3_hour] 
                df_3 = transformation2(_, GlueContext, "transform2_").toDF()
                dyf_3 = DynamicFrame.fromDF(df_3, glue_ctx=GlueContext, name="metric")
                dyf_3.printSchema()
                print("Created table for Metrics")
                
                # Print counts
                print(dyf_1.toDF().count(), dyf_2.toDF().count(), dyf_3.toDF().count())
                
                # Simple Function to Union 2 similar tables, and add RUN_ID column
                def union_(dyf1: DynamicFrame, dyf2: DynamicFrame) -> DataFrame:
                        df_1 = dyf1.toDF().withColumn('RUN_ID', f.lit(helper.runid))
                        df_2 = dyf2.toDF().withColumn('RUN_ID', f.lit(helper.runid))
                        #df_1.printSchema()
                        #df_2.printSchema()
                        # union the two dataframes, and add RUN_ID
                        df = df_1.union(df_2)
                        return df
                
                # We will load current new tweets to DDB directly
                dyf_2_ddb = dyf_2
                
                # Combine the historic data with current batch
                # We create final aggregation tables
                if FLAG_COMBINE_WITH_S3:
                        print("Aggregating historic data with current")
                        # dyf_1 and dyf_table1
                        print("Aggregation-1")
                        df_1 = union_(dyf_1, dyf_table1)
                        df_1 = df_1.groupBy('DetailLevel', 'TICKER', 'TickerDetail', 'RUN_ID')\
                                        .agg(   f.min(f.col('TIMESTAMP')).alias('TIMESTAMP'),
                                                f.sum('NUM_MENTIONS').alias('NUM_MENTIONS'),
                                                f.sum('NUM_POSITIVE').alias('NUM_POSITIVE'),
                                                f.sum('NUM_NEUTRAL').alias('NUM_NEUTRAL'),
                                                f.sum('NUM_NEGATIVE').alias('NUM_NEGATIVE')
                                                )
                        dyf_1 = DynamicFrame.fromDF(df_1, GlueContext, "union_dyf_1")
                        
                        # dyf_2 and dyf_table2
                        # No grouping for this table, so we can just append the data directly for current interval to DDB to save write operations
                        print("Aggregation-2")
                        df_2 = union_(dyf_2, dyf_table2)
                        dyf_2 = DynamicFrame.fromDF(df_2, GlueContext, "union_dyf_2")
                        
                        # dyf_3 and dyf_table3
                        print("Aggregation-3")
                        df_3 = union_(dyf_3, dyf_table3)
                        df_3 = df_3.groupBy('DetailLevel', 'LEVEL', 'RUN_ID')\
                                        .agg(   f.sum('NUM_MENTIONS').alias('NUM_MENTIONS'),
                                                f.sum('NUM_POSITIVE').alias('NUM_POSITIVE'),
                                                f.sum('NUM_NEUTRAL').alias('NUM_NEUTRAL'),
                                                f.sum('NUM_NEGATIVE').alias('NUM_NEGATIVE')
                                                )
                        dyf_3 = DynamicFrame.fromDF(df_3, GlueContext, "union_dyf_3_s3")
                        
                        print("Aggregation Complete")
                        # counts after union
                        print(dyf_1.toDF().count(), dyf_2.toDF().count(), dyf_3.toDF().count())
                
                # Window function
                # This section calculated the lagged version of counts, which is used to calculate delta for each timeperiod
                window = Window.partitionBy(f.col('LEVEL')).orderBy(f.desc("DetailLevel"))
                df_3_1 = dyf_3.toDF()
                df_3_1 = df_3_1.withColumn("D_NUM_MENTIONS", f.coalesce(f.lead(f.col('NUM_MENTIONS')).over(window), f.lit(0)))\
                                .withColumn("D_NUM_POSITIVE", f.coalesce(f.lead(f.col('NUM_POSITIVE')).over(window), f.lit(0)))\
                                .withColumn("D_NUM_NEGATIVE", f.coalesce(f.lead(f.col('NUM_NEGATIVE')).over(window), f.lit(0)))\
                                .withColumn("D_NUM_NEUTRAL", f.coalesce(f.lead(f.col('NUM_NEUTRAL')).over(window), f.lit(0)))
                
                # Dynamicframe for DDB
                dyf_3_ddb = DynamicFrame.fromDF(df_3_1, GlueContext, "union_dyf_3_ddb")
                
                print("Schemas of elements sinking to DDB")
                dyf_1.printSchema()
                dyf_2.printSchema()
                dyf_3_ddb.printSchema()
                
                # DATASINK 1
                #@ TARGET - DDBTABLE-1
                #About - Allows search for a particular month, year, or hour, based on input query
                if FLAG_ENABLE_DYNAMO_LOAD:
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
                                                        frame=dyf_2_ddb,
                                                        connection_type="dynamodb",
                                                        connection_options={
                                                                "dynamodb.output.tableName": DDB_TABLE2
                                                                }
                                                        )
                        print("Data written to Dynamo, table 2")
                        
                        # DATASINK 3
                        #@ TARGET - DDBTABLE-2
                        #About - Allows search for a particular month, year, or hour, based on input query
                        Datasink3 = GlueContext.write_dynamic_frame_from_options(
                                                        frame=dyf_3_ddb,
                                                        connection_type="dynamodb",
                                                        connection_options={
                                                                "dynamodb.output.tableName": DDB_TABLE3
                                                                }
                                                        )
                        print("Data written to Dynamo, table 3")
                        
                        
                # DATASINK 0
                #@ TARGET - S3
                #About - Sinks the data for the current batch to S3
                # TABLE1
                S3DataSink0 = GlueContext.write_dynamic_frame.from_options(frame=dyf_1.coalesce(10),
                                                                connection_type="s3",
                                                                connection_options={"path": S3_LOCATION_10, 
                                                                                    'partitionKeys': ["RUN_ID"]},
                                                                transformation_ctx='s3datasink0',
                                                                format=helper.format,
                                                                format_options={'compression': helper.compression})
                # TABLE2
                S3DataSink1 = GlueContext.write_dynamic_frame.from_options(frame=dyf_2.coalesce(10),
                                                                connection_type="s3",
                                                                connection_options={"path": S3_LOCATION_11, 
                                                                                    'partitionKeys': ["RUN_ID"]},
                                                                transformation_ctx='s3datasink1',
                                                                format=helper.format,
                                                                format_options={'compression': helper.compression})
                # TABLE3
                S3DataSink2 = GlueContext.write_dynamic_frame.from_options(frame=dyf_3.coalesce(10),
                                                                connection_type="s3",
                                                                connection_options={"path": S3_LOCATION_12, 
                                                                                    'partitionKeys': ["RUN_ID"]},
                                                                transformation_ctx='s3datasink2',
                                                                format=helper.format,
                                                                format_options={'compression': helper.compression})
                
                # update the bookmark
                print("Updating the bookmark")
                s3.put_object(Bucket="pknn-aws-twitter-analytics-zone", Key = MARKER, Body = helper.runid.encode('utf-8'))              
                print("Bookmark set to - ", s3.get_object(Bucket="pknn-aws-twitter-analytics-zone", Key = MARKER)['Body'].read().decode("utf-8"))
                
                # Run the crawler to update Run paritions for tables
                glue = boto3.client("glue")
                if glue.get_crawler(Name=CRAWLER_SOURCE_1)["Crawler"]["State"] == "READY":
                        print(f"Started S3 crawler: {CRAWLER_SOURCE_1}")
                        # glue.start_crawler(Name = CRAWLER_SOURCE_1)
                while glue.get_crawler(Name=CRAWLER_SOURCE_1)["Crawler"]["State"] != "READY":
                        # Wait for crawler to finish
                        sleep(5)
                print(f"Crawler run finished for - {CRAWLER_SOURCE_1}")
                
                # glue commit for bookmark progress
                helper.commit()
        except SystemExit:
                print("Exit from program. Execution stopped.")
                
                

                

