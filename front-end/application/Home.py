# type:ignore

import streamlit as st
import datetime
import pickle
import os
from data_model import ChartData
import altair as alt
import json
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from plotly.colors import n_colors
from wordcloud import WordCloud
from warnings import filterwarnings

filterwarnings('ignore')

st.set_page_config(
    page_title="Homepage",
    page_icon="smile",
    layout="wide"
)

############## FUNCTIONS HERE #######################
BASE_PATH = os.path.dirname(os.path.abspath(__name__))
MIN_DATE = datetime.datetime(2022, 10, 27)
MAX_DATE = datetime.datetime.today() - datetime.timedelta(days=1)
DEFAULT_DATE = MAX_DATE
# st.write(MAX_DATE)

@st.cache
def load_tickers():
    # Gets the first 100 labels from sp500 ticker file
    x = os.path.join(BASE_PATH, "./data/sp100.pickle")
    with open(x, "rb") as file:
        return pickle.load(file)

@st.experimental_singleton
def get_chart_data_obj():
    obj = ChartData(BASE_PATH)
    return obj

#####################################################

st.title("AWS Data Pipeline for Twitter Data")
st.caption("Lambton College - Pulkit, Zarna, Yasin, Deep, Nikita")

## Configuring the sidebar, to act as a settings pane
with st.sidebar:
    st.header("Index")
    st.markdown("[Section 1](#section-1)")
    st.markdown("- [Metrics](#section-1)")
    st.markdown("- [Trending Stocks](#section-1)")
    st.markdown("[Section 2](#section-1)")
    st.markdown("- [Text](#section-1)")
    st.markdown("- [Ticker Trends](#section-1)")
    st.markdown("-----")
    refresh_page = st.columns([1,30,1])[1].button("Refresh Page 🔁", help="Refreshes the page by pulling data and updating charts")


########################### CHARTS ###################################

def plot_trending(data):
    fig = px.bar(data, y='TICKER', x='NUM_MENTIONS', title="Top ticker leaderboard", orientation="h")
    fig.update_layout({
    'plot_bgcolor': 'rgba(0,0,0,0)',
    'paper_bgcolor': 'rgba(0,0,0,0)'
})
    return fig

def plot_trending_2(data):
    data = data.assign(NUM_POSITIVE=lambda x: x["NUM_POSITIVE"]/x["NUM_MENTIONS"]\
        , NUM_NEGATIVE=lambda x: x["NUM_NEGATIVE"]/x["NUM_MENTIONS"]\
        , NUM_NEUTRAL=lambda x: x["NUM_NEUTRAL"]/x["NUM_MENTIONS"])
    fig = px.bar(data, y='TICKER', x=['NUM_NEGATIVE', 'NUM_POSITIVE', 'NUM_NEUTRAL'], title="Number of positive, negative and neutral tweets", orientation="h")
    fig.update_layout({
    'plot_bgcolor': 'rgba(0,0,0,0)',
    'paper_bgcolor': 'rgba(0,0,0,0)'
     })  
    return fig

def plot_recent_tweets_text(data):
    sentiment_ = {"POSITIVE": "green", "NEGATIVE": "red", "NEUTRAL": "lightskyblue"}
    x = list(map(lambda s: sentiment_[s], data.SENTIMENT))
    fig = go.Figure(data=[go.Table(
                            columnwidth = [80,400],
                            header=dict(
                                values=['TICKER', 'Tweet text for recent tweets'],
                                line_color='white', fill_color='white',
                                align='center',font=dict(color='black', size=20)
                            ),
                            cells=dict(
                                values=[data.TICKER, data.TEXT],
                                line_color=["white"],
                                fill_color=[x, x],
                                align='left', font=dict(color='black', size=13, family="lato"),
                                height=35
                                ))
                            ])
    
    fig.update_layout(margin=dict(l=10,r=30,t=3,b=10))
    return fig

def plot_timeline(data):   
    subfig = make_subplots(specs=[[{"secondary_y": True}]])
    fig = px.area(data, x="TIMESTAMP", y=["NUM_POSITIVE", "NUM_NEGATIVE", "NUM_NEUTRAL"])
    fig2 = px.bar(data, x="TIMESTAMP", y="NUM_MENTIONS")
    fig2.update_traces(yaxis="y2",showlegend=True,name='NUM_MENTIONS')
    if data.shape[0] > 40:
        subfig.add_traces(fig.data + fig2.data)
    else:
        subfig.add_traces(fig.data)
    subfig.update_layout({
    'plot_bgcolor': 'rgba(0,0,0,0)',
    'paper_bgcolor': 'rgba(0,0,0,0)'
     })  
    return subfig

charts = get_chart_data_obj()

with st.container():
    st.markdown("-----")
    st.header("Section 1")
    st.markdown("In the first section, we look at the trends and compare the different stocks based on <br>the number of mentions. We have built a leaderboard, and also charing the break-up of individual mentions into positive, negative and neutral tweets.")
    page_metrics_s2 = st.container()
    
# Containers to hold sub-sections for section 1    
con2 = st.empty().container()

with con2:
    st.subheader("Trending Stocks")
    # Controls for this section
    pm_month, pm_date, pm_time = None, None, None
    _1, _2, _3, _4 = st.columns(4)
    top = _1.selectbox("Top", [3, 5, 10, 15], 0) 
    sortBy = _1.selectbox("Sort By", ["NUM_MENTIONS", "NUM_POSITIVE", "NUM_NEGATIVE", "NUM_NEUTRAL"], 0) 
    aggregationType = _2.selectbox("AggregationType", ("Yearly", "Monthly", "Daily", "Hourly"), 0)
    if aggregationType == "Monthly":
        query = _3.selectbox("Month", ["2022-10", "2022-11"])
    elif aggregationType == "Daily":
        query = _3.date_input("Date", DEFAULT_DATE,min_value=MIN_DATE, max_value=MAX_DATE).strftime("%Y-%m-%d")
    elif aggregationType == "Hourly":
        c1, c2 = st.columns(2)
        query = _3.date_input("Date", DEFAULT_DATE, min_value=MIN_DATE, max_value=MAX_DATE).strftime("%Y-%m-%d")
        query += "-" + _4.selectbox("Time", [f"{str(i).zfill(2)}" for i in range(24)])
    else:
        query="2022"
    
    con1 = st.empty().container()
    # Section logic
    data = charts.get_data_for_month(query, sortBy, top)
    
    if data is not None:
        tab1, tab2 = st.tabs(["Chart", "Data"])
        with tab1: 
            col1, col2 = st.columns([1, 1])
            with col1:
                st.plotly_chart(plot_trending(data), use_container_width=True)
            with col2:
                st.plotly_chart(plot_trending_2(data), use_container_width=True)
        with tab2:
            st.write(data)
            #st.download_button("Download", data)
    else:
        st.warning("Data does not exist")

with con1:
    st.subheader("Metrics")
    col1, col2, col3, col4 = st.columns(4)
    data = charts.table3_query1()
    data_ = data.loc[data.DetailLevel == query]
    #st.write(data_)
    #st.write(data.loc[data.LEVEL == aggregationType.upper()])
    col1.metric("Total Tweets", data_.NUM_MENTIONS,  data_.NUM_MENTIONS.to_list()[0] - data_.D_NUM_MENTIONS.to_list()[0])
    col2.metric("+ve Tweets", data_.NUM_POSITIVE, data_.NUM_POSITIVE.to_list()[0] - data_.D_NUM_POSITIVE.to_list()[0])
    col3.metric("Neu Tweets", data_.NUM_NEUTRAL,  data_.NUM_NEUTRAL.to_list()[0] - data_.D_NUM_NEUTRAL.to_list()[0])
    col4.metric("-ve Tweets", data_.NUM_NEGATIVE, data_.NUM_NEGATIVE.to_list()[0] - data_.D_NUM_NEGATIVE.to_list()[0])
 
with st.container():    
    st.markdown("-----")
    st.header("Section 2")
    st.markdown("In the second section, we want to dive deeper into individual ticker symbols. We do so by asking user input for more than one TICKER symbol and show the vizualizations to compare.")
    page_metrics_s2 = st.container() 

con3 = st.empty().container()
con4 = st.empty().container()

with con3:
    st.subheader("Text data")
    # limit and filter
    _1, _2, _3 = st.columns([1, 1, 2])
    top2 = _1.selectbox("Recent tweets", [10, 25, 50, 100, 500], 0, key="top2")
    ticker = _2.empty().container()
    # start and end-dates
    dts = _3.date_input(label='Date Range: ',
                value=(MIN_DATE, MAX_DATE),
                key='#date_range',
                help="The start and end date time",
                min_value=MIN_DATE, max_value=MAX_DATE)
    
    try:
        recent_tweets = charts.get_recent_tweets(top2, start=dts[0].strftime('%Y-%m-%d 00:00:00'), end=dts[1].strftime('%Y-%m-%d 24:00:00'))
        
        ticker = ticker.selectbox("Filter By Ticker", ["None"]+list(set(recent_tweets.TICKER.to_list())), 0)
        if ticker != 'None':
            st.write('yesss')
            recent_tweets =  charts.get_recent_tweets(top2, ticker=ticker, start=dts[0].strftime('%Y-%m-%d 00:00:00'), end=dts[1].strftime('%Y-%m-%d 24:00:00'))
        
    
        tab1, tab2 = st.tabs(["Chart", "Data"])
        
        with tab1:
            col1, col2 = st.columns([1.6, 2.5])  
            col1.subheader("Wordcloud of these tweets")
            
            with col2:
                
                st.markdown("""
                        <style>
                            .cell-rect[style] {
                                opacity: 0.2 !important;
                            }
                        </style>
                        """, unsafe_allow_html=True)
                
                st.plotly_chart(plot_recent_tweets_text(recent_tweets))
                
            with col1:
                # Chart the wordcloud
                text = " ".join(recent_tweets.CLEANTEXT)
                w = WordCloud(background_color="white", colormap="hot").generate(text)
                plt.figure(figsize=(5,20))
                fig, ax = plt.subplots()
                ax.imshow(w, interpolation="bilinear")
                ax.set_axis_off()      
                st.set_option('deprecation.showPyplotGlobalUse', False)
                st.pyplot()
                # Sentiment count
                temp = recent_tweets.groupby('SENTIMENT').size().to_dict()
                st.text(f"{temp.get('POSITIVE', 0)} Positive tweets, {temp.get('NEGATIVE', 0)} Negative tweets")
                st.text(f"{recent_tweets.shape[0]} Total tweets")          
        with tab2:
            st.write(recent_tweets)
        #st.download_button("Download", recent_tweets, key="recentTweets")
    except IndexError:
        pass    
  
with con4:
    st.subheader("Ticker Trends")
    _1, _2, _3, _4 = st.columns([1, 1, 2, 2])
    tick1 = _1.selectbox("Ticker (Primary)", load_tickers(), 0, key="tick1")
    tick2 = _2.selectbox("Ticker (Secondary)", ["None"]+load_tickers(), 0, key="tick2")
    level = _3.selectbox("Level", ["Monthly", "Daily", "Hourly"], 0, key="level")
    # start and end-dates
    if level in ('Hourly', 'Daily'):
        dts = _4.date_input(label='Date Range: ',
                    value=(MIN_DATE, MAX_DATE),
                    key='#date_range2',
                    help="The start and end date time",
                    min_value=MIN_DATE, max_value=MAX_DATE)
        
    try:
        data = charts.table1_query2(tick1, level)
        if level in ('Hourly', 'Daily'):
            data = data.loc[data.TIMESTAMP.dt.date >= dts[0]].loc[data.TIMESTAMP.dt.date <= dts[1]]
        st.plotly_chart(plot_timeline(data))
        with st.expander("Click to read recent tweets in selected range"):
            st.text(tick1)
            limit = st.selectbox("Recent tweets", [10, 25, 50, 100, 500], 0, key="lim1")
            fig = plot_recent_tweets_text(charts.get_recent_filtered_tweets(limit=limit, start=dts[0].strftime('%Y-%m-%d 00:00:00'), end=dts[1].strftime('%Y-%m-%d 24:00:00'), ticker=tick1))
            st.plotly_chart(fig)
        if tick2 != 'None':
            data = charts.table1_query2(tick2, level)
            st.plotly_chart(plot_timeline(data))
            with st.expander("Click to read recent tweets in selected range"):
                st.text(tick2)
                limit = st.selectbox("Recent tweets", [10, 25, 50, 100, 500], 0, key="lim2")
                fig = plot_recent_tweets_text(charts.get_recent_filtered_tweets(limit=limit, start=dts[0].strftime('%Y-%m-%d 00:00:00'), end=dts[1].strftime('%Y-%m-%d 24:00:00'), ticker=tick2))
                st.plotly_chart(fig)
    except IndexError:
        pass
        
        
        

        