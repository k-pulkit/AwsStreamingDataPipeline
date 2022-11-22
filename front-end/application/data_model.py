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
        """
        For metrics
        """
        table3 = self.table3
        @st.cache
        def _table3_query1():
            out = table3.scan()["Items"]
            return pd.json_normalize(ddb_json.loads(out))
        return _table3_query1()
        
    def table1_query1(self, dateTimeStr, ticker=None):
        """
        For Trending Stocks chart
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
    
    def table1_query2(self, ticker, level, startTime=None, endTime=None):
        """
        For getting trends for particular ticker
        """
        table1 = self.table1
        indexName = "IndexByTickerTime"
        base_exp = "TickerDetail = :tick"
        base_kv = {":tick":ticker.upper()+"_"+level.upper()}
        nextToken = None
        startTime = startTime or "2022-10-15 00:00:00"
        if endTime is None :
            base_exp += " and #stamp > :time1"
            base_kv[":time1"] = startTime
        else:
            base_exp += " and #stamp between :time1 and :time2"
            base_kv[":time2"] = endTime 
            base_kv[":time1"] = startTime 
        result = []
        query = partial(table1.query, IndexName=indexName, KeyConditionExpression = base_exp,
                ExpressionAttributeValues = base_kv,
                ScanIndexForward=False,
                ExpressionAttributeNames = {"#stamp": "TIMESTAMP"}
                )
        while True:
            query_r = query(ExclusiveStartKey=nextToken) if nextToken else query()
            items = query_r["Items"]
            result.extend(items)
            nextToken = query_r.get("LastEvaluatedKey", None)
            if nextToken is None: break
                
        out = pd.json_normalize(ddb_json.loads(result))
        out["TIMESTAMP"] = pd.to_datetime(out.TIMESTAMP.str.split(".", expand=True)[0], format="%Y-%m-%d %H:%M:%S")
        
        return out
    
    
    def table2_query1(self, limit=100, startTime=None, endTime=None, filter_ticker_name=None):
        """
        For recent tweets
        """
        table2 = self.table2
        base_exp = "#part = :part"
        base_kv = {":part": None}
        startTime = startTime or "2022-10-15 00:00:00"
        if endTime is None :
            base_exp += " and #stamp > :time1"
            base_kv[":time1"] = startTime
        else:
            base_exp += " and #stamp between :time1 and :time2"
            base_kv[":time2"] = endTime 
            base_kv[":time1"] = startTime 
        if filter_ticker_name:
            base_kv[":tick"] = filter_ticker_name.upper()
        corpus = []
        nextToken = None
        for part in range(4):
            _ = []
            base_kv[":part"] = str(part)
            query = partial(table2.query, KeyConditionExpression = base_exp, ExpressionAttributeNames = {"#part": "PARTITION", "#stamp": "TIMESTAMP"}, ExpressionAttributeValues = base_kv, ScanIndexForward=False, Limit=limit)
            query = query if filter_ticker_name is None else partial(query, FilterExpression="TICKER = :tick")
            while True:
                query_r = query(ExclusiveStartKey=nextToken) if nextToken else query()
                items = query_r["Items"]
                _.extend(items)
                nextToken = query_r.get("LastEvaluatedKey", None)
                if nextToken is None or len(_) >= limit:
                    nextToken = None
                    break
            corpus.extend(_)
    
        return pd.json_normalize(ddb_json.loads(corpus)).sort_values("TIMESTAMP", ascending=False).iloc[:limit].reset_index(drop=True)
        
    
    def get_data_for_month(self, query, sortBy="NUM_MENTIONS", top=10):
        @st.cache
        def _helper(query):
            res = self.table1_query1(query)
            if len(res) == 0:
                return None
            out =  pd.json_normalize(ddb_json.loads(res)) 
            return out.assign(NUM_POSITIVE_R=lambda x: x["NUM_POSITIVE"]/x["NUM_MENTIONS"]\
                                , NUM_NEGATIVE_R=lambda x: x["NUM_NEGATIVE"]/x["NUM_MENTIONS"]\
                                , NUM_NEUTRAL_R=lambda x: x["NUM_NEUTRAL"]/x["NUM_MENTIONS"])
        # Get results from DDB or CACHE 
        df = _helper(query)
        if df is not None:  
            sortBy = sortBy if sortBy == "NUM_MENTIONS" else sortBy+"_R"
            df_t = df.sort_values(sortBy, ascending=False).iloc[:top]
            return df_t
        else:   
            return None
        
    def get_recent_tweets(self, limit=10, start=None, end=None, ticker=None):
        """
        This method return recent tweets, either across all available,
        or, if ticker is given, then gives the recent tweets for those symbols
        """
        @st.cache
        def _helper(limit, start=None, end=None):
            return self.table2_query1(limit, start, end)
        df = _helper(limit, start, end)
        if ticker:
            return df.loc[df.TICKER == ticker].reset_index(drop=True)
        else:
            return df
    
    def get_recent_filtered_tweets(self, limit=10, start=None, end=None, ticker=None):
        """
        This method return recent tweets, either across all available,
        or, if ticker is given, then gives the recent tweets for those symbols
        """
        @st.cache
        def _helper(limit, start=None, end=None, ticker=None):
            return self.table2_query1(limit, start, end, ticker)
        df = _helper(limit, start, end, ticker)
        return df
    
            
       
       