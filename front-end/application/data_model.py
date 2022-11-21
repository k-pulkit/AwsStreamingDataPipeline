# type: ignore

import streamlit as st
import configparser
import requests
import json
from dynamodb_json import json_util as ddb_json
import pandas as pd
import os
import boto3
from functools import partial

class ChartData(object):
    def __init__(self, BASE_PATH) -> None:
        # create a dynamodb resource
        self.db = boto3.resource("dynamodb")
        self.table1 = self.db.Table("pknn-twitter-capstone-project-table-1")
        self.table2 = self.db.Table("pknn-twitter-capstone-project-table-2")
        self.table3 = self.db.Table("pknn-twitter-capstone-project-table-3")
        
    def table3_query1(self):
        table3 = self.table3
        @st.cache
        def _table3_query1():
            out = table3.scan()["Items"]
            return pd.json_normalize(ddb_json.loads(out))
        return _table3_query1()
        
    def table1_query1(self, dateTimeStr, ticker=None):
        """
        pknn-twitter-capstone-project-table-2
        Third pattern, is to recent tweets
        Query Arguments - DataTimePattern, [Ticker]
        """
        table1 = self.table1
        base_exp = "DetailLevel = :level"
        base_kv = {":level":dateTimeStr}
        nextToken = None
        if ticker is not None:
            base_exp += " and TICKER = :tick"
            base_kv[":tick"] = ticker
        result = []
        query = partial(table1.query, KeyConditionExpression = base_exp,
                ExpressionAttributeValues = base_kv,
                ScanIndexForward=False
                )
        while True:
            query_r = query(ExclusiveStartKey=nextToken) if nextToken else query()
            items = query_r["Items"]
            result.extend(items)
            nextToken = query_r.get("LastEvaluatedKey", None)
            if nextToken is None: break        
        return result
    
    def get_data_for_month(self, query, sortBy="NUM_MENTIONS", top=10):
        @st.cache
        def _get_data_for_month(query):
            res = self.table1_query1(query)
            if len(res) == 0:
                return None
            out =  pd.json_normalize(ddb_json.loads(res)) 
            return out.assign(NUM_POSITIVE_R=lambda x: x["NUM_POSITIVE"]/x["NUM_MENTIONS"]\
                                , NUM_NEGATIVE_R=lambda x: x["NUM_NEGATIVE"]/x["NUM_MENTIONS"]\
                                , NUM_NEUTRAL_R=lambda x: x["NUM_NEUTRAL"]/x["NUM_MENTIONS"])
        # Get results from DDB or CACHE 
        df = _get_data_for_month(query)
        if df is not None:  
            sortBy = sortBy if sortBy == "NUM_MENTIONS" else sortBy+"_R"
            df_t = df.sort_values(sortBy, ascending=False).iloc[:top]
            return df_t
        else:   
            return None
        
    def get_recent_tweets(self, ticker=None):
        """
        This method return recent tweets, either across all available,
        or, if ticker is given, then gives the recent tweets for those symbols
        """
        dummy_data = pd.DataFrame({
            'TICKER': ["X", "XX", "X", "XX", "X", "XX", "X", "XX", "X", "XX", "X", "XX"],\
            "TEXT": ["6 8 32 de DESCUENTO Autumn Winter Cold-proof Running Gloves Windproof Non-slip Keep Warm Touch Screen Outdoor Sports Cycling Gloves Men And Women URL https t co 6xN3HBxvLo"] + ["""A+ assurance in your essay(s).
Excel in:
#Nursing
#Politicalscience
#Nursing
#English
#Anatomy
#PsychologyPaper
#Researchpaper
#BostonCollege
#Modric
#TSLA
KINDLY DM https://t.co/VtChPWA1WW"""   ]*11,\
            "SENTIMENT": ["POSITIVE", "POSITIVE", "NEUTRAL", "POSITIVE", "NEGATIVE", "POSITIVE", "POSITIVE", "POSITIVE", "POSITIVE", "POSITIVE", "POSITIVE", "NEGATIVE"]
        })
        
        return dummy_data