import concurrent.futures
import time
from typing import List, Tuple

import pandas
import pandas as pd
import streamlit as st

from utils import parse_webpage, switcher

# needs to be the first thing after the streamlit import
st.set_page_config(page_title="Scraper App", layout="wide")
st.title("Scraper App")


@st.cache_resource(show_spinner=False, ttl=5*60)
# @st.cache_resource(show_spinner=False, ttl=5*60)
def parse_urls(urls: List[str]) -> Tuple[List, List, List]:
    # download all webpages
    t1 = time.perf_counter()
    order = list(range(len(urls)))
    results_ = dict.fromkeys(order, None)
    with st.spinner('Download web pages..'):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for url, item in zip(urls, order):
                # Add debug print statement before calling parse_webpage
                #print(f"Calling parse_webpage with URL: {url}")
                results_[item] = executor.submit(parse_webpage, url)

            for item in results_.keys():
                # Retrieve the result and add a debug print statement
                result = results_[item].result()
                #print(f"Result from parse_webpage for item {item}: {result}")
                results_[item] = result
    st.success('Web pages downloaded!')
    step = 100 / len(urls) * .01
    bar_value = 0
    my_bar = st.progress(bar_value)

    stocks = []
    prices = []
    compares = []
    for idx, item in enumerate(results_.keys()):
        stock, p_c = switcher(results_[item], urls[idx])
        stocks.append(stock)
        prices.append(p_c[0])
        compares.append(p_c[1])
        bar_value += step
        my_bar.progress(round(bar_value, 2))

    print(f"Finished in: {(time.perf_counter() - t1):.2f} seconds")
    return stocks, prices, compares


def process_df(df_: pandas.DataFrame, results_: Tuple[List, List, List]) -> pandas.DataFrame:
    # In Stock
    df_['In Stock'] = results_[0]
    df_['In Stock'] = df_['In Stock'].astype(int)
    # Prices
    df_['Price'] = results_[1]
    df_['Compare To'] = results_[2]

    # order the columns
    columns = list(df_.columns)
    init_columns = ['Item', 'In Stock', 'Price', 'Compare To']
    for col in init_columns:
        columns.remove(col)
    df_ = df_[init_columns + columns]
    df_out = df_.copy()
    bool_dictionary = {1: 'in stock', 0: 'out of stock', 2: "Quote"}
    df_out['In Stock'] = df_out['In Stock'].map(bool_dictionary)
    df_out['Price'] = df_out['Price'].astype(float)
    df_out['Compare To'] = df_out['Compare To'].astype(float)
    df_out.to_excel("output.xlsx", index=False)
    df_['In Stock'] = df_['In Stock'].astype(bool)
    df_['Price'] = df_['Price'].astype(float)
    df_['Compare To'] = df_['Compare To'].astype(float)
    return df_


# sidebar
# st.sidebar.title("Upload")
excel_file = st.sidebar.file_uploader(
    "Excel file containing urls", type=['xlsx'])
if excel_file:
    df = pd.read_excel(excel_file)
    st.dataframe(df)
    # check if there's an url column
    if "URL" in df.columns:
        urls_list = df['URL'].tolist()
        if urls_list:
            results = parse_urls(df['URL'].tolist())
            df = process_df(df, results)
            st.header("Results")
            st.dataframe(df)
            with open("output.xlsx", 'rb') as my_file:
                st.download_button(label='Download results',
                                   data=my_file, file_name='output.xlsx')
        else:
            st.error("The ***URL*** column is empty !!!")
    else:
        st.error("there's no ***URL*** column in the excel file")
