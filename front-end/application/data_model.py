import streamlit as st
import configparser
import requests
import json
from dynamodb_json import json_util as ddb_json
import pandas as pd
import os

class ChartData(object):
    def __init__(self, BASE_PATH) -> None:
        # read secret from conf file
        parser = configparser.ConfigParser()
        parser.read(os.path.join(BASE_PATH, "data/conf.ini"))
        # read api.json for links
        with open("../application/data/api.json", "r") as _:
            x = json.load(_)
        
        self.secret = parser["DYNAMO"]["secret"]
        self.json = x["API_GATEWAY"]
        
    def _query(self, url):
        _ = requests.get(url).text
        return ddb_json.loads(_)
    
    @st.cache
    def _get_data_for_month(self, month):
        url = self.json["month"]["url"].format(secret=self.secret, month=month)
        res = self._query(url)["Items"]
        # convert to a python dict
        if len(res) == 0:
            return None
        df = pd.json_normalize(res)  
        df_s = df.sort_values("NUM_MENTIONS", ascending=False)
        return df_s
    
    def get_data_for_month(self, month, top=10):
        df = self._get_data_for_month(month)
        if df is not None:  
            df_t = df.iloc[:top]
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