import streamlit as st
import numpy as np
import pandas as pd
from fuzzywuzzy import process, fuzz
import re
import base64
# import io
# from pyspark.sql.functions import col, when, lit
# import pyspark
# from pyspark.sql import SparkSession
# from pyspark.sql.types import *

# spark = SparkSession.builder.appName('NetworkOpt').getOrCreate()

st.set_option('deprecation.showfileUploaderEncoding', False)

st.title("Network Optimization App")
st.write("PLEASE READ STEPS BELOW")
st.write("FIRST: Upload LinkedIn Connections file.")
st.write("SECOND: Verify that the file contains the correct information.")
st.write("- It must include the following fields: 'First Name', 'Last Name', 'Company', and 'Position'.")
st.write("- These fields must be named EXACTLY as listed above.")
st.write("- The file may contain additional fields so long as the above fields are included.")
st.write("THIRD: Once information is verified, hit 'Run'.")

# POSSIBLE PROBLEM - need to have encoding of utf-8
uploaded_file = st.file_uploader("Select file for upload", type="csv", encoding = "utf-8")
accounts = pd.read_csv("https://raw.githubusercontent.com/mmobley8/Contact-Network-Optimization-App/master/export%20(2).csv")

def title_search():
    title_string = '''CTO OR CIO OR 'Chief Data Officer' OR 'Chief Privacy Officer' OR 'Chief Analytics Officer' OR 'Chief Digital Officer' OR 'Data Engineer' OR 'Data Strategy' OR 'Data Lake' OR 'Data Integration' OR 'Principal Architect' OR 'Big Data Architect' OR 'Cloud Architect' OR 'Data Architect' OR 'Enterprise Architecture' OR 'Chief Data Architect' OR 'Cloud Architect' OR 'Digital Transformation' OR 'Cloud Security' OR Infrastructure OR 'Cloud security engineer' OR 'cloud analytics' OR CISO OR 'Information Security Architect' OR 'Data Protection' OR 'Data Security' OR 'Security Architect' OR 'Privacy Officer' OR 'cyber security architect' OR 'Chief Privacy Officer' OR 'Director of Risk Management' OR 'Compliance Director' OR 'IT Risk' OR Analytics OR 'Data Management' OR BI OR 'Director of Big Data' OR 'Data Governance' OR 'Data Platform' OR 'VP IT' OR 'Director of IT' OR 'Information Management' OR 'IT Audit' OR CDO OR CAO'''
    temp_title_search = title_string.split(" OR ")
    title_search = []
    for item in temp_title_search:
        title_search.append(item.strip("'"))
    return title_search

def company_search():
    temp_company_list = list(set(accounts["AccountName"]))
    company_search = ["".join(re.findall(r"['!&.,A-Za-z0-9 _-]", " ".join([char.strip() for char in company.split()]).strip())) for company in temp_company_list]
    return company_search

def fuzz_m(col, company_list, score_t):
    new_company, score = process.extractOne(col, company_list, scorer=score_t)
    if score<95:
        return col
    else:
        return new_company

def opt():
    st.write("Running...")
    st.write("This may take a few minutes.")
    connections['Company '] = connections['Company'].apply(fuzz_m, company_list=company_search(), score_t=fuzz.ratio)
    series_list = [pd.Series(connections["First Name"].astype("object")), pd.Series(connections["Last Name"]), pd.Series(connections["Company"]), pd.Series(connections["Position"])]
    connections_new = pd.concat(series_list, axis = 1)
    connections_new["Recommendation"] = connections_new["Position"].apply(lambda x: "Yes" if x in title_search() else "No")
#     connections_new = connections_new[connections_new["Recommendation"] == "Yes"]
    st.write("")
    st.write("Success. View your results below.")
    st.write(connections_new)
    # st.write(connections)
    st.markdown(get_table_download_link(connections_new), unsafe_allow_html=True)

def get_table_download_link(df):
    """Generates a link allowing the data in a given panda dataframe to be downloaded
    in:  dataframe
    out: href string
    """
    csv = df.to_csv(index=False)
    b64 = base64.b64encode(
        csv.encode()
    ).decode()  # some strings <-> bytes conversions necessary here
    return f'<a href="data:file/csv;base64,{b64}" download="Connections_recs.csv">Download .csv file</a>'

if uploaded_file:
    connections = pd.read_csv(uploaded_file)
    st.write(connections)
    connections_comp = ["".join(re.findall(r"['!&.,A-Za-z0-9 _-]", " ".join([char.strip() for char in str(company).split()]).strip())) for company in connections["Company"]]
    connections["Company"] = pd.Series(connections_comp)
    if st.button("Run"):
        opt()

    # myschema = StructType([StructField("First Name", StringType(), True) \
    #                     , StructField("Last Name", StringType(), True) \
    #                     , StructField("Company", StringType(), True) \
    #                     , StructField("Position", StringType(), True) ])

    # spark_connections = spark.createDataFrame(connections_new, schema = myschema)
    # spark_connections = spark_connections.withColumn("First Name", col("First Name").cast(StringType))
                                        # .withColumn("Last Name", col("Last Name").cast(StringType)) \
                                        # .withColumn("Company", col("Company").cast(StringType)) \
                                        # .withColumn("Position", col("Position").cast(StringType))

    # st.write(spark_connections.show())

       # st.write(spark_connections.select("*",
    #         when(col("Position").rlike("|".join(title_search())), lit("Yes")).otherwise(lit("No")).alias("title_relevant"),
    #         when(col("Company").rlike("|".join(company_search())), lit("Yes")).otherwise(lit("No")).alias("company_relevant")
    #         ).show())




        # spark_connections = spark.read.csv(path = uploaded_file, header = True)
