import csv
import re
import os
import pymysql
import logging
import sys
import psycopg2


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
greenplum_hostname      = "172.16.198.4"
greenplum_port_no       = "5432"
greenplum_user_name     = "postgres"
greenplum_user_pass     = "pivotal"
greenplum_database_name = 'infralogdb'
#greenplum_database_name = 'postgres'
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

def make_greenplum_database_schema(greenplum_schemanameNo1):
    if 'greenplum' in database:
        try:
            curs = greenplumdb.cursor()
            sql = "CREATE SCHEMA  "  + greenplum_schemanameNo1  + ";"
            curs.execute(sql)
            greenplumdb.commit()
            print("Create Schema OK")
        except:
            print("Schema :" + greenplum_schemanameNo1 + " is Exist!")


def make_greenplum_database_schema_table(yearmonth):
    # try:
    curs = greenplumdb.cursor()
    sql = """ 
        CREATE TABLE infralogdb.esko_sbp_gpdb1.newtable2 ();"""
    print(sql)
    curs.execute(sql)
    greenplumdb.commit()
    # except:
    #     print("GPDB make tables ERROR")
    #     print(sql)


yearmonth = "gpdb202107"
#make_greenplum_database_schema(greenplum_schemanameNo1)
make_greenplum_database_schema_table(yearmonth)