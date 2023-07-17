
# Postgres:
dbname = "Immigration"
user = "______"
password = "______"
server = "localhost"
port = "5432"
url = f"jdbc:postgresql://{server}:{port}/{dbname}"
driver = "org.postgresql.Driver"


# Declare oracle variable:
dbname_dw = "Immigration"
table_dw_port = "TM_PORT_D"
user_dw = "______"
password_dw = "______"
server_dw = "127.0.0.1"
port_dw = 1521
service_name_dw = "ORCL"
url_dw = f"jdbc:oracle:thin:@{server_dw}:{port_dw}:{service_name_dw}"
driver_dw = "oracle.jdbc.OracleDriver"

# mart table: 
table_dw_port = "TM_PORT_D"
table_dw_visa = "TM_VISA_D"
table_dw_mode = "TM_MODE_D"
table_dw_country = "TM_COUNTRY_D"
table_dw_state = "TM_STATE_D"

# file_path:
file_path_temp = "D:\Spark\Project_01\Data_Source\city_temperature.csv"
file_path_city_demo = "D:\Spark\Project_01\Data_Source/us_cities_demographics.csv"
file_path_airport = "D:\Spark\Project_01\Data_Source/airport.csv"

 # file_name:
file_name_temp = "temp"
file_name_city_demo = "city_demo"
file_name_airport = "airport"

# Dim mapping: 
state_dim = "state_dim"
visa_dim = "visa_dim"
mode_dim = "mode_dim"
port_dim = "port_dim"
country_dim = "country_dim"

# table in DW:
tm_port_d = "tm_port_d"
tm_state_d ="tm_state_d"
tm_visa_d ="tm_visa_d"
tm_mode_d = "tm_mode_d"
tm_country_d ="tm_country_d"
ts_immigration = "TS_IMGT"
ts_airport     = "TS_AIRPORT"
ts_temp = "TS_TEMP"
ts_city_demo = "TS_CITY_DEMO"

dw_table_dim = [tm_port_d,tm_state_d, tm_visa_d,tm_mode_d,tm_country_d]

