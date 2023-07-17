"""
- sheet này tạo ra hàm trích xuất toàn bộ tập dữ liệu thành DataFrame 
- kết quả return ra các DataFrame 
- cần có các hàm: 
    + Extract Mapping txt uissing pandas 
    + Extract I94   using Spark 
    + Extract Temp  using Spark
    + Extract Airport  using Spark
    + Extract city_demo  using Spark

"""
import pandas as pd 
import pyspark
from pyspark.sql import SparkSession
import psycopg2
from Connect import *
from queries_db import * 
from Parameter import * 

# connection to postgres:
conn,cur = connect_postgres()

#creat spark sesion :
# .config("spark.jars", "D:\Spark\jdbc\ojdbc8.jar") \
# .config("spark.jars", "D:\Spark\jdbc\postgresql-42.6.0.jar"
# thử nghiệm trên cùng 1 phiên sparksession tạo connect tới cả postgres và oracle 
def create_sparksession():
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("read data") \
        .config("spark.jars", "D:\Spark\jdbc\ojdbc8.jar,D:\Spark\jdbc\postgresql-42.6.0.jar") \
        .getOrCreate()
    
    spark.conf.set("spark.sql.execution.arrow.enabled", "true")
    print("already created sparksession")
    return spark

# Extract mapping data from DB and return Dataframe for each mapping txt:
def extract_mapping(query_cmd, col):
    cur.execute(query = query_cmd)
    result_queries = cur.fetchall()
    conn.commit()

    df = pd.DataFrame(data = result_queries, columns = col)
    print("already extract mapping file to dataframe")
    return df

# Etract I94 Immigration from DB and return DataFrame.
def extract_i94(query_cmd):
    spark = create_sparksession()

    df_i94 = spark.read.format('jdbc') \
    .option("url", url) \
    .option("query", query_cmd) \
    .option("user", user) \
    .option("password", password) \
    .option("driver", driver) \
    .load()
    
    print("already extract i94 data to dataframe")
    return df_i94

# extract airport, city_demo, temp from local disk and return DataFrames: temp, city_demo, airport 
def extract_disk(file_name , file_path):

    spark = create_sparksession()

    if file_name == "temp" or file_name == "airport":
        df = spark.read.csv(file_path,sep = ',', header= True)

    elif file_name == "city_demo":
        df = spark.read.csv(file_path,sep = ';', header= True)

    print("already extracted file from local disk to dataframe")
    return df 

def Main_extract_to_df():
    # dim datafram from database (mapping):
    df_port = extract_mapping(queries_port, port_col)
    df_visa = extract_mapping(queries_visa, visa_col)
    df_mode = extract_mapping(queries_mode, mode_col)
    df_country = extract_mapping(queries_cntr, cntr_col)
    df_state = extract_mapping(queries_state, state_col)

    # dim dataframe from disk:
    df_temp = extract_disk(file_name_temp, file_path_temp)
    df_airport = extract_disk(file_name_airport, file_path_airport)
    df_city_demo = extract_disk(file_name_city_demo, file_path_city_demo)

    # fact dataframe from i94:
    df_i94 = extract_i94(queries_i94)
    return df_port, df_visa, df_mode, df_country, df_state, df_temp, df_airport, df_city_demo










