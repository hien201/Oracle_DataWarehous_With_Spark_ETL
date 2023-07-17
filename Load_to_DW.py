# 1. load các df_dim_mapping vào trực tiếp Mart: vì dữ liệu dim, đã qua xử lý
# 2. load các bảng df_dim_airport, temp, demo vào staging và fact_i94 vào staging
# 3. viết procedure luân chuyển dữ liệu từ staging đến mart với các yêu cầu và điều kiện xác định 
from Parameter import *
from Process import * 
from pyspark.sql import SparkSession
from Parameter import *

##################################
# LOAD MAPPING DIM INTO DATAMART #
##################################
def load_mapping(df_dim_list,dw_table_dim):

    """
    THIS FUNCTION WILL LOAD MAPPING DIM TO MART TABLE
    """
    # call spark_oracle:

    load_table_list = zip(df_dim_list, dw_table_dim)
    # create list table dim mapping need to load: 
    # cần xử lý đoạn df_port/visa/mode_dim
    # load mapping dim tabel into dw (mart):
    for df, dim in load_table_list:
        df.write.format('jdbc') \
            .option("url", url_dw) \
            .mode("append") \
            .option("user", user_dw) \
            .option("password", password_dw) \
            .option("dbtable", dim) \
            .option("driver", driver_dw) \
            .save()
        print(f"already load {dim} to Mart Area")
        
#############
###############
def load_immigration(df, staging_table):
    df.write.format('jdbc') \
            .option("url", url_dw) \
            .mode("append") \
            .option("user", user_dw) \
            .option("password", password_dw) \
            .option("dbtable", staging_table) \
            .option("driver", driver_dw) \
            .save()
    print("already Load i94 to TS_IMGT table in Staging Area")


######

######
def load_airport(df,staging_table):

    df.write.format('jdbc') \
            .option("url", url_dw) \
            .mode("append") \
            .option("user", user_dw) \
            .option("password", password_dw) \
            .option("dbtable", staging_table) \
            .option("driver", driver_dw) \
            .save()
    print("already Load airport  to TS_AIRPORT table in Staging Area")

####
#####
def load_temp(df,staging_table):
    df.write.format('jdbc') \
            .option("url", url_dw) \
            .mode("append") \
            .option("user", user_dw) \
            .option("password", password_dw) \
            .option("dbtable", staging_table) \
            .option("driver", driver_dw) \
            .save()
    print("already Load Temp to TS_TEMP table in Staging Area")
#####

#######
def load_city_demo(df, staging_table):
    df.write.format('jdbc') \
            .option("url", url_dw) \
            .mode("append") \
            .option("user", user_dw) \
            .option("password", password_dw) \
            .option("dbtable", staging_table) \
            .option("driver", driver_dw) \
            .save()
    print("already Load City_demo to TS_CITY_DEMO table in Staging Area")





