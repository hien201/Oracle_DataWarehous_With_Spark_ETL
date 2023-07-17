
#2. create table from Mapping File:

create_db_cntr= ("""
create table if not exists db_cntr (
    cntr_cd float,
    cntr_nm varchar,
    lst_mdf_data varchar
);
""")


create_db_visa =("""
create table if not exists db_visa (
    vis_cd float,
    vis_tp varchar,
    lst_mdf_data varchar

)
""")

create_db_state = ("""
create table if not exists db_state (
    sta_cd varchar,
    sta_nm varchar,
    lst_mdf_data varchar
)
""")

create_db_port = ("""
create table if not exists db_port (
    port_cd varchar,
    port_nm varchar,
    lst_mdf_data varchar
)
""")


create_db_mode =( """
create table if not exists db_mode (
    mode_cd float,
    mode_nm varchar,
    lst_mdf_data varchar
)
""")

create_db_i94_immigration =( """
create table if not exists db_i94_immigration (
cicid       float,
i94yr       float,
i94mon      float,
i94cit      float,
i94res      float,
i94port      varchar,
arrdate     float,
i94mode     float,
i94addr      varchar,
depdate     float,
i94bir      float,
i94visa     float,
count       float,
dtadfile     varchar,
visapost     varchar,
occup        varchar,
entdepa      varchar,
entdepd      varchar,
entdepu      varchar,
matflag      varchar,
biryear     float,
dtaddto      varchar,
gender       varchar,
insnum       varchar,
airline      varchar,
admnum      float,
fltno        varchar,
visatype     varchar,
lst_mdf_data varchar
)
""")

create_table_list = [create_db_cntr, create_db_visa, create_db_state, create_db_port, create_db_mode, create_db_i94_immigration]


#insert into table Immigration:
insert_i94_immigration = ("""insert into table db_i94_immigration values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s ,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""")

insert_db_port = (""" insert into db_port values(%s, %s, %s)""")

insert_db_cntr = (""" insert into db_cntr values(%s, %s, %s)""")

insert_db_state = ("""insert into db_state values(%s, %s, %s)""")

insert_db_mode = (""" insert into db_mode values(%s, %s, %s)""")

insert_db_vis =(""" insert into db_visa values (%s, %s, %s)""")

insert_table_list =[insert_db_port,insert_db_cntr,insert_db_state, insert_db_mode, insert_db_vis, insert_i94_immigration]



# Query data from table mapping DB:
queries_port = (""" select * from db_port""")
queries_cntr = (""" select * from db_cntr""")
queries_state = (""" select * from db_state""")
queries_mode = (""" select * from db_mode""")
queries_visa = (""" select * from db_visa""")
queries_i94 = (""" select * from db_i94_immigration""")

i94_queries = "select * from db_i94_immigration"


# column of table:
port_col = ['port_cd', 'port_nm', 'lst_mdf_data']
cntr_col = ['cntr_cd', 'cntr_nm', 'lst_mdf_data']
state_col = ['sta_cd', 'sta_nm', 'lst_mdf_data']
mode_col = ['mode_cd', 'mode_nm', 'lst_mdf_data']
visa_col = ['visa_cd', 'visa_tp', 'lst_mdf_data']






