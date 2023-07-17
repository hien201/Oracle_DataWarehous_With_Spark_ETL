from queries_db import *
import pandas as pd 
import pyspark
from pyspark.sql import SparkSession
import os
import glob
import psycopg2
from datetime import date
from pyspark.sql.functions import lit
from Connect import connect_postgres



# connect DB: 
conn, cur = connect_postgres()

# insert file txt

def insert_port_txt():
    df_port = pd.read_csv("D:\Spark\Project_01\Data_Source\Mapping_txt\port.txt", sep ='\t=\t', header= None, engine= 'python', names=['port_cd', 'port_nm'],skipinitialspace= True, index_col= False )
    df_port.iloc[ : , 1 ] = df_port.iloc[ : , 1 ].str.replace("'", "")
    df_port.iloc[ : , 0 ] = df_port.iloc[ : , 0 ].str.replace("'", "")
    df_port['DATA_LST_MOD_TM'] = date.today().strftime("%Y%m%d")


    for i, row in df_port.iterrows():
        cur.execute(insert_db_port, list(row))
        conn.commit()
   

def insert_country_txt():
    df_cntr = pd.read_csv("D:\Spark\Project_01\Data_Source\Mapping_txt\country.txt", sep ='=', header= None, engine= 'python', names=['id', 'country'],skipinitialspace= True, index_col= False )
    df_cntr.iloc[ : , 1 ] = df_cntr.iloc[ : , 1 ].str.replace("'", "")
    df_cntr['DATA_LST_MOD_TM'] = date.today().strftime("%Y%m%d")

    for i, row in df_cntr.iterrows():
        cur.execute(insert_db_cntr, list(row))
        conn.commit()


def insert_state_txt():
    df_state = pd.read_csv("D:\Spark\Project_01\Data_Source\Mapping_txt\i94addrl.txt", sep = '=', header= None, engine= 'python', names=['id', 'state'],skipinitialspace= True, index_col= False )
    df_state.iloc[ : , 1 ] = df_state.iloc[ : , 1 ].str.replace("'", "")
    df_state.iloc[ : , 0 ] = df_state.iloc[ : , 0 ].str.replace("'", "")
    df_state['DATA_LST_MOD_TM'] = date.today().strftime("%Y%m%d")

    for i, row in df_state.iterrows():
        cur.execute(insert_db_state, list(row))
        conn.commit()

def insert_visa_txt():
    df_visa = pd.read_csv("D:\Spark\Project_01\Data_Source\Mapping_txt\I94VISA.txt", sep ='=', header= None, engine= 'python', names=['id', 'type'],skipinitialspace= True, index_col= False )
    df_visa['DATA_LST_MOD_TM'] = date.today().strftime("%Y%m%d")
    for i, row in df_visa.iterrows():
        cur.execute(insert_db_vis, list(row))
        conn.commit()

def insert_mode_txt():
    df_mode = pd.read_csv("D:\Spark\Project_01\Data_Source\Mapping_txt\mode.txt", sep = '=', header= None, engine= 'python', names= ['id', 'mode'], skipinitialspace= True, index_col= False )
    df_mode.iloc[ : , 1 ] = df_mode.iloc[ : , 1 ].str.replace("'", "")
    df_mode['DATA_LST_MOD_TM'] = date.today().strftime("%Y%m%d")
    for i, row in df_mode.iterrows():
        cur.execute(insert_db_mode, list(row))
        conn.commit()

def insert_i94_immigration():
    # create sparksession
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("insert into i94 table in db") \
        .config("spark.jars", "D:\Spark\jdbc\postgresql-42.6.0.jar") \
        .getOrCreate()
    
    # Declare local variable:
    dbname = "Immigration"
    user = "postgres"
    password = "postgres"
    server = "localhost"
    port = "5432"
    url = f"jdbc:postgresql://{server}:{port}/{dbname}"
    driver = "org.postgresql.Driver"

    # read csv using spark 
    df_i94 = spark.read.parquet("D:\Spark\Project_01\Data_Source\Immigration\part_01.parquet")
    df_i94=df_i94.withColumn("lst_mdf_data", lit(date.today().strftime("%Y%m%d")))

    # save data from df into table i94_immigration
    df_i94.write.format('jdbc') \
    .option("url", url) \
    .mode("append") \
    .option("user", user) \
    .option("password", password) \
    .option("dbtable", "db_i94_immigration") \
    .option("driver", driver) \
    .save()






def main():
    insert_country_txt()
    insert_port_txt()
    insert_state_txt()
    insert_mode_txt()
    insert_visa_txt()
    insert_i94_immigration()


if __name__ =="__main__":
    main()

