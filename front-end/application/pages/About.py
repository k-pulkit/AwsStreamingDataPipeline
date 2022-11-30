import streamlit as st
import pandas as pd
import pickle
import configparser
import json
from PIL import Image

st.set_page_config(
    page_title="Homepage",
    page_icon="tada",
    layout="wide"
)

st.title("AWS Data Pipeline for Twitter Data")
st.caption("Lambton College - Pulkit, Zarna, Yasin, Deep, Nikita")

image1 = Image.open('./pages/arch.png')

st.markdown("""
            ## About this project
            
            Big data and Cloud computing technologies have become mainstream in the past two decades, and are being used by every organization to store, process and analyze large volumes of data.   
            The tools for big data have evolved as well, with organizations using a wide range of big-data technologies for a variety of use-cases. Furthermore, with the adoption of cloud technologies, the big-data landscape has also transformed with these organizations moving their big-data operations from on-premises architecture to cloud-native solutions. 
              
            Our capstone project deals with implementing a cloud based big-data pipeline to analyze the stock market tweets on Twitter.   
            The main objective of this project is to use multiple cloud native services to familiarize oneself with the application and interaction of the services to create a working big-data pipeline.
              
            The big-data pipeline implemented in this project is a batch processing ETL pipeline, where the data source is a real-time streaming source: the Twitter streaming API endpoint, which allows developers to get live tweets as they are posted to the platform.   
            The project makes use of variety of cloud services to ingest streaming data, store the data, and process the data using Spark distributed computing framework, and store the end results of analysis in a NoSQL database that is used by front-end dashboard application. The front-end dashboard application enables the user to change variables to deep dive into the collected data and gain useful insights. 
            
            ## Architecture diagram
            
            The architecture of the project is based on the ETL methodology that is used very commonly for Big Data pipelines. ETL (Extract, Transform, Load) is about extracting data sources, transforming the data by using business logic, and loading the data into target databases or data warehouses
            """)

st.image(image1, caption="Architecture Diagram for the project")

st.markdown("""
            ## Which AWS cloud services are we using?
            
            We have used a variety of AWS services with the objective to get familiar with technologies that are used in Big Data projects. The project uses the following tech stack:
            - Compute : EC2, Lambda functions, ECS & ECR
            - Analytics : Kinesis Firehose
            - Big Data Processing : Glue Pyspark ETL
            - Data storage : S3, DynamoDB
            - Data cataloging : Glue Crawlers, databases, and tables
            - Data Query : Athena
            - Orchestration : Glue Workflows (for ETL)
            - Authorization and Authentication / Security : IAM, Security Groups
            - Monitoring : CloudWatch
            
            In addition, the core technologies / libraries used for the development of this project are as follows:
            - Languages : Python
            - Frameworks / Libraries : Boto3, Pyspark, NLTK, Streamlit, Pandas, Tweepy
            - Containerization : Docker 
            - Source code version control : GIT
            - APIs : Twitter API v2
            """)







