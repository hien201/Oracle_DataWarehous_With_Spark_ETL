from Process import *
from Parameter import *
from queries_db import *
from Extract_To_DataFrame import *
from Load_to_DW import *


# result of extract_to_df:
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
    return df_port, df_visa, df_mode, df_country, df_state, df_temp, df_airport, df_city_demo, df_i94

# result of extract_to_df:
df_port, df_visa, df_mode, df_country, df_state, df_temp, df_airport, df_city_demo, df_i94 = Main_extract_to_df()


def Main_process():
    df_port_clean = Process_Mapping_Dim(df_port,df_state, df_mode, df_visa, df_country, port_dim)
    df_state_clean = Process_Mapping_Dim(df_port,df_state, df_mode, df_visa, df_country, state_dim)
    df_mode_clean = Process_Mapping_Dim(df_port,df_state, df_mode, df_visa, df_country, mode_dim)
    df_visa_clean = Process_Mapping_Dim(df_port,df_state, df_mode, df_visa, df_country, visa_dim)
    df_country_clean = Process_Mapping_Dim(df_port,df_state, df_mode, df_visa, df_country, country_dim)
    df_temp_clean = Process_Temp(df_temp)
    df_airport_clean = Process_Airport(df_airport)
    df_city_demo_clean = Process_city_demo(df_city_demo)
    df_i94_clean = Process_i94_immigration(df_i94, df_state_clean, df_visa_clean, df_mode_clean)
    return df_port_clean, df_state_clean, df_mode_clean, df_visa_clean, df_country_clean, df_temp_clean, df_airport_clean, df_city_demo_clean, df_i94_clean

#result of processing:
df_port_clean, df_state_clean, df_mode_clean, df_visa_clean, df_country_clean, df_temp_clean, df_airport_clean, df_city_demo_clean, df_i94_clean = Main_process()

df_dim_list = [df_port_clean,df_state_clean, df_visa_clean,df_mode_clean,df_country_clean]
def Main_load():
    load_mapping(df_dim_list,dw_table_dim)
    load_airport(df_airport_clean,ts_airport)
    load_immigration(df_i94_clean, ts_immigration)
    load_city_demo(df_city_demo_clean, ts_city_demo)
    load_city_demo(df_temp_clean, ts_temp)


Main_load()