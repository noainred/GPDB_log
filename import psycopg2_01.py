import psycopg2
from typing import Iterator, Optional
import io
from typing import Any
import pandas as pd
import sys


yearmonth = "gpdb202107"

############################
project = "sbp_oss1"
nodecount = 8
mode = "dev"
#mode = "prd"

### Operating Path

# PRD mode ##########################################################
prd_options_read_dir    = '/Users/junho/Downloads/parsing/data/'
prd_options_write_dir   = '/Users/junho/Downloads/parsing/'

### Dev mode ###
dev_options_read_dir    = '/Users/junhopark/OneDrive/DevData/GPDB_dstat_log/test01/'
dev_options_write_dir   = '/Users/junhopark/OneDrive/DevData/GPDB_dstat_log/test01-result/'
### Dev mode ### Greenplum ###
database                = 'greenplum'
greenplum_hostname      = "localhost"
greenplum_port_no       = "5432"
greenplum_user_name     = "postgres"
greenplum_user_pass     = "pivotal"
#greenplum_database_name = 'infralogdb'
greenplum_database_name = 'postgres'
greenplum_schemanameNo1 = "esko_sbp_gpdb1"
####################################################################################

if "dev" in mode:
    options_read_dir = dev_options_read_dir
    #options_read_filename = 'sys.20210401.log'
    options_write_dir = dev_options_write_dir
    if 'mariadb' in database:
        mariadb = pymysql.connect(
                host='localhost', 
                port=3306, user='root',
                passwd='my-secret-pw', 
                #db='gpdb_infra_mon',     
                charset='utf8', autocommit=True)
    elif 'greenplum' in database:
        greenplumdb = psycopg2.connect(
            host=       greenplum_hostname,
            port=       greenplum_port_no,
            database=   greenplum_database_name,
            user=       greenplum_user_name,
            password=   greenplum_user_pass
            )

elif "prd" in mode:
    options_read_dir = prd_options_read_dir
    #options_read_filename = 'sys.20210401.log'
    options_write_dir = prd_options_write_dir
    if 'mariadb' in database:
        mariadb = pymysql.connect(
                host='localhost', 
                port=3306, user='root',
                passwd='my-secret-pw', 
                #db='gpdb_infra_mon',     
                charset='utf8', autocommit=True)
    elif 'greenplum' in database:
        greenplumdb = psycopg2.connect(
            host='localhost',
            port=5432,
            database='infralogdb',
            user='postgres',
            password='1q2w3e4r'
            )
else:
    print("ERROR!! please Defind operation mode")
    sys.exit()


###########
def make_greenplum_database_schema():
    if 'greenplum' in database:
        try:
            curs = greenplumdb.cursor()
            sql = "CREATE SCHEMA  " + greenplum_schemanameNo1  + ";"
            curs.execute(sql)
            greenplumdb.commit()
            print("Create Schema OK")
        except:
            print("Schema :" + greenplum_schemanameNo1 + " is Exist!")

def make_greenplum_database_schema_table(yearmonth):
    # try:
    tablename = str(greenplum_schemanameNo1 + '.' + "\"" + str(yearmonth) + "\""  )
    curs = greenplumdb.cursor()
    print(tablename)
    sql = """ 
        CREATE TABLE IF NOT EXIST """ + str(tablename) + """ (
        field_name varchar NULL,
        field_date varchar NULL,
        field_time varchar NULL,
        field_cpu_usr varchar NULL,
        field_cpu_sys varchar NULL,
        field_cpu_idle varchar NULL,
        field_cpu_wai varchar NULL,
        field_cpu_hiq varchar NULL,
        field_cpu_siq varchar NULL,
        field_dsk_read varchar NULL,
        field_dsk_writ varchar NULL,
        field_net_recv varchar NULL,
        field_net_send varchar NULL,
        field_memory_used varchar NULL,
        field_memory_buff varchar NULL,
        field_memory_cach varchar NULL,
        field_memory_free varchar NULL
        ) ;"""
    print(sql)
    curs.execute(sql)
    greenplumdb.commit()
    # except:
    #     print("GPDB make tables ERROR")
    #     print(sql)


make_greenplum_database_schema()
make_greenplum_database_schema_table(yearmonth)


curs = greenplumdb.cursor()
#print(str(field_name), str(field_date), str(field_time), str(field_cpu_usr), str(field_cpu_sys), str(field_cpu_idle), str(field_cpu_wai), str(field_cpu_hiq), str(field_cpu_siq), str(field_dsk_read), str(field_dsk_writ), str(field_net_recv), str(field_net_send), str(field_memory_used), str(field_memory_buff), str(field_memory_cach), str(field_memory_free))
tablename = str(greenplum_schemanameNo1 + '.'  + str("gpdb202107"))
sql = """INSERT INTO """ + tablename + """ (field_name) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
sql_values = (('smdw1', '1111', '11', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL'))
# print(tablename)
# print(sql_values)
# print()
