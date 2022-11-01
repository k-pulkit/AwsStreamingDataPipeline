# type: ignore

"""
Name - Job-1
GlueJobName - pknn-aws-twitter-Job-1-toParquet
Description - This python script makes use of pyspark and glue api
to read the data from input files located in data catalogs in AWS Glue Tables and
and loads the data in incremental fashion in parquet format.py

This script requires that the Glue catalog tables are refreshed before running the script, as 
only the partitions present in the catalog are considered for processing.
"""

import sys
from awsglue.transforms import *                
from awsglue.dynamicframe import DynamicFrame   
from awsglue.utils import getResolvedOptions   
from pyspark.context import SparkContext             
from awsglue.context import GlueContext             
from awsglue.job import Job                

class InitializeGlue(object):
    def __init__(self):
        self.args = getResolvedOptions(sys.argv, ["JOB_NAME"])
        self.sc = SparkContext.getOrCreate()
        self.glueContext = GlueContext(self.sc)
        self.spark = self.glueContext.spark_session
        self.job = Job(self.glueContext)
        self.job.init(self.args['JOB_NAME'], self.args) # To start record keeping the bookmarks
        ## Glue partitions are using the partition information for bookmark keys, so
        #       there can be error when the Glue table refresh and job run is less than an hour
        #           not checked the same, as currently it is more than 1 hour
        
class GlueETL_toParquet(InitializeGlue):
    """
    Our transformation class, which will read the  
    input from S3, the Json files and then convert them to parquet format,
    then, save the files to final destination, in staging bucket
    """
    def __init__(self):
        super().__init__()
        self.compression = 'snappy'
        self.format = "parquet"
        self.groupSize = 1048576  
        
    def __step1_read_files(self, database_name, table_name, push_down_predicate = "", transformation_ctx=""):
        datasource0 = self.glueContext.create_dynamic_frame_from_catalog(database=database_name,
                                                                 table_name=table_name,
                                                                 transformation_ctx=transformation_ctx,
                                                                 additional_options={"groupFiles": "inPartition", "groupSize": self.groupSize},
                                                                 push_down_predicate = push_down_predicate)
        
        # Check if the table is empty, that is no new records are available
        if datasource0.toDF().rdd.isEmpty():
            return None
        else:
            datasource0.toDF().show(4)
            return datasource0

    def __step2_write_files(self, datasource: DynamicFrame, s3_output_path, partitions=[], transformation_ctx=""):
        if datasource:
            datasink0 = self.glueContext.write_dynamic_frame.from_options(frame=datasource,
                                                                        connection_type="s3",
                                                                        connection_options={"path": s3_output_path,
                                                                                            'partitionKeys': partitions},
                                                                        transformation_ctx=transformation_ctx,
                                                                        format = self.format,
                                                                        format_options={"compression": self.compression})
        
    def run(self, database_name, table_name, s3_output_path, partitions=[], push_down_predicate = "", transformation_ctx_1="", transformation_ctx_2=""):
        df = self.__step1_read_files(database_name, table_name, push_down_predicate, transformation_ctx_1)
        self.__step2_write_files(df, s3_output_path, partitions, transformation_ctx_2)
        
    def commit(self):
        # Commit for glue bookmarks
        self.job.commit()
        
if __name__ == "__main__":
    etl = GlueETL_toParquet()
    etl.run(database_name="twitter_capstone_project_rawzone", \
                table_name="raw_tweets_json", \
                    s3_output_path="s3://pknn-aws-twitter-stage-zone/stage1/data/raw_tweets_parquet", \
                        partitions=['year', 'month', 'day', 'hour'], transformation_ctx_1="datasource01", transformation_ctx_2="datasink01")
    
    etl.run(database_name="twitter_capstone_project_rawzone", \
                table_name="sp500", \
                        s3_output_path="s3://pknn-aws-twitter-stage-zone/stage1/data/sp500_parquet"\
                            , transformation_ctx_1="datasource02", transformation_ctx_2="datasink02")
    
    etl.commit()
    
    
        
        
        
    
    
