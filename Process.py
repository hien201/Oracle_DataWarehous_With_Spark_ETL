"""
- This sheet will process and clean data in DataFrame follow standard
- After that, return df_clean 

"""
from Extract_To_DataFrame import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, lit
from queries_db import *
from Parameter import *
from datetime import date
from pyspark.sql.functions import udf
import datetime as dt

def Process_Mapping_Dim(df_port,df_state, df_mode, df_visa, df_country, Dim):
    """
    - process mapping dim 
    - input dim data frame from extract module
    - output: clean dim data Frame after process
    """
    Dimmesion = Dim
    spark = create_sparksession()
    
    if Dimmesion == 'port_dim':
        # read dimmension df:

        # clean df_port: split port_cd -->  update port_cd and add new column name "sta_cd"
        split_value = df_port['port_nm'].str.split(", ", n = 1, expand = True) 
        df_port['port_nm'] = split_value[0]
        df_port['sta_cd']  = split_value[1]

        # clean record have value not in US or not defined: join with df_state 
        df_state_join = df_state['sta_cd'].str.replace("\t", "")
        df_port_clean = pd.merge(df_port, df_state_join, left_on= 'sta_cd', right_on='sta_cd', how='inner')

        # final --> select cloumn: 'port_cd', 'port_nm','sta_cd', 'lst_mdf_data_x'
        df_port_clean = df_port_clean[['port_cd', 'port_nm','sta_cd', 'lst_mdf_data']]
        df_port_clean = spark.createDataFrame(df_port_clean)

        return df_port_clean
    

    elif Dimmesion == 'state_dim':
        df_state['sta_cd'] = df_state['sta_cd'].str.replace("\t", "")
        df_state_clean = spark.createDataFrame(df_state)
        return df_state_clean
    
    elif Dimmesion == 'mode_dim':
        df_mode_clean = spark.createDataFrame(df_mode)
        return df_mode_clean
    # clean visa dim:
    elif Dimmesion == 'visa_dim':
        df_visa_clean = spark.createDataFrame(df_visa)
        return df_visa_clean
    
    # clean country: có nhiều mã quốc gia không hợp lệ, không tồn tại ==> chuyển về 'OTHER'
    elif Dimmesion == 'country_dim':
        df_country.loc[df_country['cntr_nm'].str.contains('No Country'), 'cntr_nm'] = 'OTHER'
        df_country.loc[df_country['cntr_nm'].str.contains('INVALID'), 'cntr_nm'] = 'OTHER'
        df_cntr_clean = spark.createDataFrame(df_country)

        return df_cntr_clean
    
    
def Process_Temp(df_temp):
    """
    - xử lý và làm sạch dữ liệu Temp theo phương án làm sạch đã đưa ra 
    - sau đó, trả về df_temp_clean 

    """
    # time in 2016:
    # only US: 
    df_temp_clean = df_temp.filter(df_temp.Country =='US')\
                           .filter(df_temp.Year == '2016')\
                           .withColumn("lst_mdf_data", lit(date.today().strftime("%Y%m%d")))\
                           .withColumnRenamed("State", "sta_nm")\
                           .withColumnRenamed("City", "cit")\
                           .withColumnRenamed("Month", "mon")\
                           .withColumnRenamed("Year", "yr")\
                           .withColumnRenamed("AvgTemperature", "avg_temp")\
                           .drop("Region")\
                           .drop("Country")
    print("already cleaned temp data")
    return df_temp_clean


def Process_Airport(df_airport):
    """
    process airport data 
    """
    # clean data: 
    df_airport_clean = df_airport.filter(df_airport.iso_country == 'US')\
                             .withColumn("sta_cd", split(col("iso_region"), "-")[1])\
                             .withColumn("lst_mdf_data", lit(date.today().strftime("%Y%m%d")))\
                             .withColumnRenamed("ident", "arpt_cd")\
                             .withColumnRenamed("elevation_ft", "ele_ft")\
                             .withColumnRenamed("iso_country", "cntr")\
                             .withColumnRenamed("name", "arpt_nm")\
                             .withColumnRenamed("municipality", "mun")\
                             .withColumnRenamed("type", "arpt_tp")\
                             .withColumnRenamed("gps_code", "gps_cd")\
                             .withColumnRenamed("local_code", "local_cd")\
                             .drop("iso_region")\
                             .drop("coordinates")\
                             .drop("continent")\
                             .drop("iata_code")
    print("already cleaned airport data")
    return df_airport_clean

def Process_city_demo(df_city_demo):
    
    # process:delelte record has state field null. After that clean duplicate record at 3 field: state, city, race:
    df_city_demo_clean = df_city_demo.filter(df_city_demo.State.isNotNull())\
                                     .withColumn("LST_MDF_DATA", lit(date.today().strftime("%Y%m%d")))\
                                     .dropDuplicates(subset = ['State', 'City', 'Race'])\
                                     .withColumnRenamed("City", "CIT")\
                                     .withColumnRenamed("Median Age", "MD_AG")\
                                     .withColumnRenamed("Male Population", "M_PPLT")\
                                     .withColumnRenamed("Female Population", "FM_PPLT")\
                                     .withColumnRenamed("Total Population", "TT_PPLT")\
                                     .withColumnRenamed("Number of Veterans", "NUM_VTR")\
                                     .withColumnRenamed("Foreign-born", "FRN_BR")\
                                     .withColumnRenamed("Average Household Size", "AVR_HOSE_SZ")\
                                     .withColumnRenamed("State Code", "STA_CD")\
                                     .withColumnRenamed("Race", "RAC")\
                                     .withColumnRenamed("Count", "CNT")\
                                     .drop("State")
    print("already cleaned city_demo data")
    return df_city_demo_clean
    


def Process_i94_immigration(df_i94,df_state_clean, df_visa_clean, df_mode_clean):
    """
    - INPUT: dataframe df_94, df_state, df_visa, df_mode
    - OUTPUT: df_i94_clean
    
    """
    # call datafram extract
    spark = create_sparksession()
    # make temp table
    df_state_clean.createOrReplaceTempView("df_state_clean")
    df_visa_clean.createOrReplaceTempView("df_visa_clean")
    df_mode_clean.createOrReplaceTempView("df_mode_clean")
    df_i94.createOrReplaceTempView("df_i94")

    # clean data: 
    df_i94_clean = spark.sql("""
                            select
                                   i.cicid as id
                                  ,i.i94yr as yr
                                  ,i.i94mon as mon
                                  ,i.i94cit as cit
                                  ,i.i94res as res
                                  ,i.i94port as port
                                  ,i.arrdate as arrdt
                                  ,coalesce(m.mode_cd, 'other') as mode
                                  ,coalesce(s.sta_cd, '99') as state
                                  ,i.depdate as depdt
                                  ,i.i94bir as bir
                                  ,coalesce(v.visa_cd, 'other') as visa_cd
                                  ,i.dtadfile as dtadfile
                                  ,i.occup as occup
                                  ,i.gender as gender
                                  ,i.airline as airline
                                  ,i.fltno as fltno
                                  ,i.visatype as visa_tp
                                  ,i.lst_mdf_data as lst_mdf_data


                            from df_i94 i left join df_mode_clean m on i.i94mode = m.mode_cd
                                      left join df_visa_clean v on i.i94visa = v.visa_cd
                                      left join df_state_clean s on i.i94addr = s.sta_cd

                            """)
    get_date = udf(lambda x: (dt.datetime(1960, 1, 1).date() + dt.timedelta(x)).isoformat() if x else None)
    # clean data: only date add data in file > 20160101 and drop duplicate value of some field
    df_i94_clean = df_i94_clean.filter(df_i94_clean.dtadfile > '20160101')\
                            .dropDuplicates(subset = ['id','yr', 'mon', 'cit', 'port', 'arrdt'])\
                            .withColumnRenamed("id", "i94_id")\
                            .withColumnRenamed("port", "port_cd")\
                            .withColumnRenamed("state", "sta_cd")\
                            .withColumnRenamed("mode", "mode_cd")\
                            .withColumn("arrdt", get_date(df_i94_clean.arrdt))
    print("already processed i94 data")

    
    return df_i94_clean



        


        






