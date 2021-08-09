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
#dev_options_write_dir   = '/Users/junhopark/OneDrive/DevData/GPDB_dstat_log/test01-result/'
dev_options_write_dir   = '/Users/junhopark/Downloads/tmp/test01-result/'
### Dev mode ### Greenplum ###
database                = 'greenplum'
greenplum_hostname      = "172.16.198.4"
#greenplum_hostname      = "localhost"
greenplum_port_no       = "5432"
greenplum_user_name     = "postgres"
greenplum_user_pass     = "pivotal"
greenplum_database_name = 'infralogdb'
greenplum_schemanameNo1 = "sbp_gpdb"
greenplum_tablenameNo1  = "oss"
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


def make_greenplum_schema():
    try:
        if 'greenplum' in database:
            try:
                curs = greenplumdb.cursor()
                #sql = "CREATE SCHEMA " + greenplum_database_name + "." + greenplum_schemanameNo1 + ";"
                sql = "CREATE SCHEMA " + greenplum_schemanameNo1 + ";"
                curs.execute(sql)
                greenplumdb.commit()
            except:
                print("Already databse Exist!")
        elif 'mariadb' in database:
            pass
            # curs = mariadb.cursor()
            # sql = "CREATE DATABASE IF NOT EXISTS `schemainfralogdb`"
            # curs.execute(sql)
            # mariadb.commit()
    except:
        print("Schema EXIST: " + sql)


def make_greenplum_schema_table(yearmonth):
    try:
        tablename = str( greenplum_schemanameNo1 + '.' + project + '_' + str(yearmonth))
        curs = greenplumdb.cursor()
        sql = """ 
            CREATE TABLE IF NOT EXISTS """ + str(tablename) + """ (
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
        curs.execute(sql, (tablename))
        greenplumdb.commit()
        print("Made schema table: " + make_greenplum_schema_table)
    except:
        print("GPDB make tables ERROR")
        print(sql)


def incloud_x(readdata):
    tmp = readdata
    if "B" in readdata:
        readdata = re.sub("B","",readdata)
    if "k" in readdata:
        readdata = re.sub("k","",readdata)
        readdata = round(int(readdata)) * 1024
        readdata = str(readdata)
    if "M" in readdata:
        readdata = re.sub("M","",readdata)
        readdata = round(int((readdata))) * 1024 * 1024
        readdata = str(readdata)
    if "G" in readdata:
        if "." in readdata:            
            readdata = int(re.sub("G","",readdata).replace('.',''))
            readdata = (int(round(readdata)) * 1024 * 1024 * 1024 / 10)
        else:
            readdata = re.sub("G","",readdata)
            readdata = int(readdata) * 1024 * 1024 * 1024
            readdata = str(readdata)
    
    returndata = readdata
    if "." in str(round(int(returndata))):
        print(tmp)
        print(readdata)
    return str(round(int(returndata)))


def insert_gpdb():
    curs = greenplumdb.cursor()
    tablename = str( greenplum_schemanameNo1 + '.' + project + '_' + str(yearmonth))
    sql = """insert into """ + tablename + """(field_name, field_date, field_time, 
                field_cpu_usr, field_cpu_sys, field_cpu_idle, field_cpu_wai, field_cpu_hiq, field_cpu_siq,
                field_dsk_read, field_dsk_writ,
                field_net_recv, field_net_send,
                field_memory_used, field_memory_buff, field_memory_cach, field_memory_free) 
        values (%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    sql_value = (field_name, field_date, field_time, \
            field_cpu_usr, field_cpu_sys, field_cpu_idle, field_cpu_wai, field_cpu_hiq, field_cpu_siq,\
            field_dsk_read, field_dsk_writ, \
            field_net_recv, field_net_send, \
            field_memory_used, field_memory_buff, field_memory_cach, field_memory_free)
    curs.execute(sql, sql_value)
    #print(sql)
    greenplumdb.commit()

print("Let's Start!")

for file_name in os.listdir(dev_options_write_dir):
    if ".csv" in file_name:
        csvdata = csv.reader(file_name, delimiter=',')
        for row in csvdata:
            print(csvdata)



print("Finished")