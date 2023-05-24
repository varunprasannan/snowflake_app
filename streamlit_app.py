import toml
import streamlit as st
import pandas as pd
import snowflake.connector as sf
from datetime import date
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns


sidebar = st.sidebar

def connect_to_snowflake(acct, usr, pwd, rl, wh, db):
    ctx = sf.connect(user=usr, account=acct, password=pwd, role=rl, warehouse=wh, database=db)
    cs = ctx.cursor()
    st.session_state['snow_conn'] = cs
    st.session_state['is_ready'] = True
    return cs

def format_results(columns: list, results: list) -> list:
    result_data = []
    for row in results:
        row_dict = dict()
        for i in range(len(columns)):
            row_dict[columns[i]] = row[i]
        result_data.append(row_dict)
    return result_data


#Sample CODE

@st.cache_data
def get_data():
    query = 'SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS limit 100;'
    results = st.session_state['snow_conn'].execute(query)
    results = st.session_state['snow_conn'].fetchall()
    cols_metadata = st.session_state['snow_conn'].description
    cols = [each_cols.name for each_cols in cols_metadata]
    final_result = format_results(cols, results)
    return final_result

with sidebar:
    account = st.text_input("Account")
    username = st.text_input("Username")
    password = st.text_input("Password")
    role = st.text_input("Role")
    wh = st.text_input("Warehouse")
    db = st.text_input("Database")
    connect = st.button("Connect to Snowflake", on_click=connect_to_snowflake, args=[account, username, password, role, wh, db])

if 'is_ready' not in st.session_state:
    st.session_state['is_ready'] = False

if st.session_state['is_ready'] == True:
    data = get_data()
    types = pd.DataFrame(data)

    st.subheader('Raw data')
    if st.checkbox('Show raw data'):
        st.write(types)


    # To show all orders within a particular range
    orders = types["O_TOTALPRICE"].agg(['min','max'])
    min,max = st.slider("Price Range", min_value=float(orders['min']),\
                        max_value=float(orders['max']),
                        value=[float(orders['min']),float(orders['max'])])

    types.loc[types['O_TOTALPRICE'].between(min,max)]

    # use a checkbox if u want to reset the priority sorting
    st.subheader("Priority Sorting:")
    option = st.selectbox(
    'Select the Priority:',
     types['O_ORDERPRIORITY'].unique())

    'You selected the order priority:: ', option
    if option:
        types.loc[types['O_ORDERPRIORITY'] == option]

    # hist_values = np.histogram(
    # types['O_ORDERPRIORITY'], bins=5, range=(0,4))[0]
    # st.bar_chart(hist_values)

    new_list = []
    for i in types['O_ORDERPRIORITY']:
        new_list.append(i)
    a = len(new_list)

    fig, ax = plt.subplots()
    ax.hist(new_list, bins=5)

    st.subheader('Visualizing the frequency of data wrt. Priority')
    st.pyplot(fig)

    id = st.number_input("Put ID:",min_value=1)
    if id:
        st.write(types.iloc[id])
