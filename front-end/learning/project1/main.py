# type:ignore

import streamlit as st
import pandas as pd

import plotly.express as px
import plotly.graph_objs as go
import altair as alt

header = st.container()
dataset = st.container()
features = st.container()
model_training = st.container()
interactive = st.container()
charts = st.container()

#st.markdown(""" 
#            <style>
#                .main {
#                    background-color: pink;   
#                }
#            </style>
#            """, unsafe_allow_html=True)


@st.cache
def read_data():
    return pd.read_parquet("./data/yellow_tripdata_2022-01.parquet")

with header:
    # code
    st.title("Welcome to the dashboard")
    st.text('pulkit')
    
with dataset:
    st.header("This is a header")
    st.text('pulkittt')
    
    # read from files
    df = read_data()
    st.write(df.head())
    
    # group
    st.subheader("This is a bar chart")
    st.bar_chart(pd.DataFrame(df["PULocationID"].value_counts()).head(10))

with features:
    st.header("This is a header")
    st.text('pulkittt')
    
    st.markdown("""
                1. Point 1: And this is _italic_ and **bold**
                2. Point 2
                """)

with model_training:
    st.header("This is a header")
    st.text('pulkittt')
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("Col1")
        sl_v = st.slider("A slider", 0, 100, 5, 5)
        sb_v = st.selectbox("A box", ["A", "C", "none of above"], 0)
        ti_v = st.text_input("Type something", "type someth")
        
    with col2:
        st.subheader("Col2")
        st.metric("Temprature", sl_v, "-1")
        
with interactive:
    st.title("A closer look at data")
    st.header("this is interactive section")
    
    #_ = [df.tpep_pickup_datetime, df.trip_distance, df.total_amount]
    #st.write(_)
    
    #fig = go.Figure(data=go.Table(header={'values':["A", "B", "B"]}, cells={'values': _}))
    #st.write(fig)
    
    with st.expander("Click to expand"):
        if st.checkbox("show"):
            def fun():
                interactive.camera_input("Click me")
            
            button = st.button("Open camera", on_click=fun)
    
    st.balloons()
    
with charts:
    ch = alt.Chart(data=df.head(100)).mark_point().encode( 
        x = "passenger_count",
        y = "trip_distance"
    )
    st.altair_chart(ch)