"""
Name - Job-2
GlueJobName - pknn-aws-twitter-Job-2-cleanData
Description - This python script makes use of pyspark and glue api
This data is to be executed on the stage1 data, after it is crawled.
The purpose of the job is to clean the data, by combining the information from raw tweets with the sp500 file.

We only are considering the original tweets for analysis, the replies are only used to add extra information in the original tweets
Objective 1 - Normalize the data, remove the nested structure
Objective 2 - Remove tweets when the hashtags and cashtags are not relevant
Objective 3 - Segregate the original tweets from the un-original for seperate analysis
Objective 4 - Enrich the original tweets using the sentiment of their replies or comments
Objective 5 - Remove the spam tweets
Objective 6 - Save the cleaned data files to stage2 of the staging tables
Result - We can use these cleaned tables to create the aggregated tables in the final bucket, called analytics bucket



This script requires that the Glue catalog tables are refreshed before running the script, as 
only the partitions present in the catalog are considered for processing.
"""

import sys
from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job
from pyspark.sql import functions as f
from pyspark.sql import types as t

class GlueInitializer(object):
    def __init__(self) -> None:
        self.args = getResolvedOptions(sys.argv, ['JOB_NAME'])
        self.sc = SparkContext.getOrCreate()
        self.glueContext = GlueContext(self.sc)
        self.spark = self.glueContext.spark_session
        self.job = Job(self.glueContext)
        self.job.init(self.args['JOB_NAME'], self.args)
        print("Spark has been initialized")
    
    def read_dyf(self):
        pass    
    
if __name__ == "__main__":
    # Get the glue context and spark object from the initalizer class
    helper = GlueInitializer()
    spark = helper.spark
    gc = helper.glueContext
    
    # read the staged tables from the staging area
    
    # Read transform 1
    #@ TABLE_NAME = 
    
    
    
    
    








