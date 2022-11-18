import streamlit as st
import pandas as pd
import pickle
import configparser
import json

st.set_page_config(
    page_title="Homepage",
    page_icon="tada"
)

st.title("AWS Data Pipeline for Twitter Data")
st.caption("Lambton College - Pulkit, Zarna, Yasin, Deep, Nikita")

st.markdown("""
            ## About
            
            In this project, we are building a data pipeline
            """)

with open("../application/data/sp100.pickle", "rb") as fx:
    st.write(pickle.load(fx))

parser = configparser.ConfigParser()
parser.read("../application/data/conf.ini")
st.write(parser["DYNAMO"]["secret"])

with open("../application/data/api.json", "r") as x:
    st.json(json.load(x))

