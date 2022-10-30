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
        self.job.init(self.args['JOB_NAME'])
        
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
        
    def __step1_read_files(self, database_name, table_name, push_down_predicate = ""):
        datasource0 = self.glueContext.create_dynamic_frame_from_catalog(database=database_name,
                                                                 table_name=table_name,
                                                                 transformation_ctx="datasource0",
                                                                 additional_options={"groupFiles": "InPartition", "groupSize": self.groupSize},
                                                                 push_down_predicate = push_down_predicate)
        
        datasource0.toDF().show(4)
        
        return datasource0

    def __step2_write_files(self, datasource: DynamicFrame, s3_output_path, partitions=[]):
        datasink0 = self.glueContext.write_dynamic_frame.from_options(frame=datasource,
                                                                      connection_type="s3",
                                                                      connection_options={"path": s3_output_path,
                                                                                          'partitionKeys': partitions},
                                                                      transformation_ctx="datasink0",
                                                                      format = self.format,
                                                                      format_options={"compression": self.compression})
        
    def run(self, database_name, table_name, s3_output_path, partitions=[], push_down_predicate = ""):
        df = self.__step1_read_files(database_name, table_name, push_down_predicate)
        self.__step2_write_files(df, s3_output_path, partitions)
        
    def commit(self):
        self.job.commit()
        
if __name__ == "__main__":
    etl = GlueETL_toParquet()
    etl.run(database_name="twitter_capstone_project_rawzone", \
                table_name="raw_tweets_json", \
                    s3_output_path="s3://pknn-test-bucket-1/raw_tweets_parquet", \
                        partitions=['year', 'month', 'day'])
    
    etl.run(database_name="twitter_capstone_project_rawzone", \
                table_name="sp500", \
                        s3_output_path="s3://pknn-test-bucket-1/sp500")
    
    etl.commit()
    
    
        
        
        
    
    
