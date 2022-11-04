"""
Name - Job-2
GlueJobName - pknn-aws-twitter-Job-2-cleanData
Description - This python script makes use of pyspark and glue api
This data is to be executed on the stage1 data, after it is crawled.
The purpose of the job is to clean the data, by combining the information from raw tweets with the sp500 file.

We only are considering the original tweets for analysis, the replies are only used to add extra information in the original tweets
Objective 1 - Normalize the data, remove the nested structure
Objective 2 - Remove tweets when the hashtags and cashtags are not relevant
Objective 3 - Remove the spam tweets
Objective 4 - Save the cleaned data files to stage2 of the staging tables
Result - We can use these cleaned tables to create the aggregated tables in the final bucket, called analytics bucket



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
    
    # read the staged tables from the staging area
    
    # Read transform : datasource0
    #@ TABLE_NAME = raw_tweets_parquet
    #@ DATABASE = twitter_capstone_project_stagezone_stage1
    # TYPE = INCREMENTAL LOAD
    DATABASE_SOURCE_0 = "twitter_capstone_project_stagezone_stage1"
    TABLE_SOURCE_0_0 = "raw_tweets_parquet"
    TABLE_SOURCE_0_1 = "sp500_parquet"
    # push_down_predicate="day=30 and hour=01",
    datasource0 = GlueContext.create_dynamic_frame_from_catalog(database=DATABASE_SOURCE_0,
                                                                table_name=TABLE_SOURCE_0_0,
                                                                transformation_ctx='datasource0',
                                                                additional_options={'groupFiles': helper.groupFilesStrategy, 'groupSize': helper.groupSize})
    
    if datasource0.toDF().rdd.isEmpty():
        print("Exiting as no new data to process")
        sys.exit()
    
    # Read transform : datasource1
    #@ TABLE_NAME = sp500_parquet
    #@ DATABASE = twitter_capstone_project_stagezone_stage1
    # TYPE = FULL LOAD
    datasource1 = GlueContext.create_dynamic_frame_from_catalog(database=DATABASE_SOURCE_0,
                                                                table_name=TABLE_SOURCE_0_1,
                                                                additional_options={'groupFiles': helper.groupFilesStrategy, 'groupSize': helper.groupSize})
    
    # Based on interactive notebook analysis
    # Resolve choice for the author_id columne
    datasource0_1 = ResolveChoice.apply(datasource0, specs=[("author_id", "make_struct")], transformation_ctx = 'ds_0_1')
    
    # Print the schema of the start dataframes
    print("Schema of the dynamic frames loaded")
    datasource0_1.printSchema()
    datasource0.printSchema()
    
    """
    Objective 1 - Normalize the data, remove the nested structure
    Now, we create dataframes from the dynamicframe objects to do operations to clean the dataframe
    ticker_df: contains the valid tickers, top 100 ordered
    tweets_df: contains the unnested json tweet objects, with all the columns renamed
    """
    ticker_df = Filter.apply(datasource1, lambda row: row["sno"] <= 100, transformation_ctx = 'filter_ticker_top_100')\
                        .toDF()\
                            .orderBy('sno')\
                                .select('ticker', 'ticker_name')
                                
    tweets_df = UnnestFrame.apply(datasource0_1, transformation_ctx = 'unnest_tweets').toDF()
    tweets_df_1 = tweets_df.toDF(*[i.replace('.', '_') for i in tweets_df.columns])\
                            .withColumn("author_id", f.coalesce(f.col('author_id_struct_int'), f.col('author_id_struct_long')))\
                                .drop('author_id_struct_int', 'author_id_struct_long', 'reference_tweets')\
                                    .withColumn('is_original', f.isnull('reference_tweets_tweet_type'))      # add indicator for reference columns
                            
    print("Schema of the dataframes, that will be used for cleaning")
    ticker_df.printSchema()
    tweets_df_1.printSchema()
    print("Number of ticks - ", ticker_df.count())
    print("Number of tweets - ", tweets_df_1.count())
    
    """
    Objective 2 - Remove tweets when the hashtags and cashtags are not relevant
    tweet_tags: contains mapping of tweet_id 
    """
    x = f.array().cast(t.ArrayType(t.StringType()))
    tweet_tags = tweets_df_1.withColumn("hashcashtags"\
                       , f.array_union(f.coalesce('tweet_meta_hashcashtags', x)\
                       , f.coalesce('reference_tweets_hashcashtags', x)))\
                            .select('tweet_id', f.explode('hashcashtags').alias('tags'))
    
    # only keep the ticker symbols that are in sp500 list
    tweet_tags_filtered = tweet_tags.join(f.broadcast(ticker_df), tweet_tags.tags==ticker_df.ticker, 'inner')\
                            .drop('tags')\
                                .withColumn("ticker_ls", f.collect_list('ticker').over(Window.partitionBy("tweet_id")))\
                                .withColumn("ticker_sz", f.size('ticker_ls'))                  
    print(f"After only keeping valid tags, we have these many tweets,  {tweet_tags_filtered.select('tweet_id').distinct().count()}")                 
    tweet_tags_filtered.show(4)
    
    # consolidate the information with original tweets data
    tweets_valid = tweet_tags_filtered.join(tweets_df_1, "tweet_id", "inner")\
                    .drop('tweet_meta_hashcashtags', 'reference_tweets_hashcashtags')
    
    """
    Objective 3 - Identify possible spams by making use of the first few characters of a tweet (first 25)
    tweets_valid_2: Adds 3 new columns
        @tweet_text_clean => cleaned text, with remove url, tags, spaces, trimming etc.
        @tweet_text_clean_sub25 => contains the first 25 characters of the clean text string
        @possible_spam => if first 25 characters are repeated more than 1 time, then it is a possible spam
    """
    THRESHOLD_1 = 1
    tweets_valid_2 = tweets_valid.withColumn('tweet_text_clean', f.regexp_replace("tweet_text_text", "(http.+) ", " "))\
                                .withColumn('tweet_text_clean', f.regexp_replace("tweet_text_clean", "[@|#|\$]\w+", ""))\
                                .withColumn('tweet_text_clean', f.regexp_replace("tweet_text_clean", "[^a-zA-Z0-9 -]", " "))\
                                .withColumn('tweet_text_clean', f.regexp_replace("tweet_text_clean", "^\s|\s+", " "))\
                                .withColumn('tweet_text_clean', f.trim('tweet_text_clean'))\
                                .withColumn('tweet_text_clean_sub25', f.lower(f.substring('tweet_text_clean', 1, 25)))\
                                .withColumn('possible_spam'\
                                            , f.when(f.count('tweet_text_clean_sub25')\
                                                      .over(Window.partitionBy('tweet_text_clean_sub25')) > THRESHOLD_1, True)\
                                               .otherwise(False)
                                                    )
                                
    print("Amount of possible spam tweets in the data")
    tweets_valid_2.select('possible_spam', 'tweet_id').distinct().groupBy('possible_spam').count().show()
    
    """
    Objective 4 - Save the cleaned data to S3 location, in stage2 in staging bucket
    Also, save the spam tweets for possible inspection or use in the future.
    At, this point, we want to repartition the data, so that we use ticker in the partition key for later use
    
    FILTERED BY - possible_spam
    PARTITION KEY - [TICKER < YEAR < MONTH < DAY < HOUR]
    """
    
    print("Writing the non-spam tweets data to stage1")
    partitionKeys = ['year', 'month', 'day', 'hour']
    output_path_1 = 's3://pknn-aws-twitter-stage-zone/stage2/data/cleaned_tweets_parquet'
    output_dyf_1 = DynamicFrame.fromDF(tweets_valid_2.filter('possible_spam=false'), glue_ctx=GlueContext, name='cleaned_tweets').coalesce(10)
    datasink0 = GlueContext.write_dynamic_frame.from_options(frame=output_dyf_1,
                                                             connection_type="s3",
                                                             connection_options={"path": output_path_1, 
                                                                                 'partitionKeys': partitionKeys},
                                                             transformation_ctx='datasink0',
                                                             format=helper.format,
                                                             format_options={'compression': helper.compression})
    
    print("Writing the spam tweets data to stage1")
    output_dyf_2 = DynamicFrame.fromDF(tweets_valid_2.filter('possible_spam=true'), glue_ctx=GlueContext, name='spam_tweets').coalesce(10)
    output_path_2 = 's3://pknn-aws-twitter-stage-zone/stage2/data/spam_tweets_parquet'
    datasink1 = GlueContext.write_dynamic_frame.from_options(frame=output_dyf_2,
                                                             connection_type="s3",
                                                             connection_options={"path": output_path_2, 
                                                                                 'partitionKeys': partitionKeys},
                                                             transformation_ctx='datasink1',
                                                             format=helper.format,
                                                             format_options={'compression': helper.compression})
    
    # glue commit for bookmark progress
    helper.commit()
    
    
    
    
    
    








