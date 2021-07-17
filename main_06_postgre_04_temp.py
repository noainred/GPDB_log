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
greenplum_hostname      = "localhost"
greenplum_port_no       = "5432"
greenplum_user_name     = "gpadmin"
greenplum_user_pass     = "pivotal"
greenplum_database_name = 'infralogdb'
greenplum_schemanameNo1 = "enko"
greenplum_tablenameNo1  = "sbp_gpdb"
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


def make_greenplum_database():
    if 'greenplum' in database:
        try:
            curs = greenplumdb.cursor()
            sql = "CREATE SCHEMA  " + greenplum_database_name + ";"
            curs.execute(sql)
            greenplumdb.commit()
        except:
            print("Already databse Exist!")
    elif 'mariadb' in database:
        curs = mariadb.cursor()
        sql = "CREATE DATABASE IF NOT EXISTS `schemainfralogdb`"
        curs.execute(sql)
        mariadb.commit()


def make_greenplum_schema(yearmonth):
    try:
        tablename = str("infralogdb.schemainfralogdb" + '.' + project + '_' + str(yearmonth))
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

def insert_mariadb():
    curs = mariadb.cursor()
    tablename = "gpdb_infra_mon." + str(yearmonth)
    sql = """insert into """ + tablename + """(field_name, field_date, field_time, 
                    field_cpu_usr, field_cpu_sys, field_cpu_idle, field_cpu_wai, field_cpu_hiq, field_cpu_siq,
                    field_dsk_read, field_dsk_writ,
                    field_net_recv, field_net_send,
                    field_memory_used, field_memory_buff, field_memory_cach, field_memory_free) 
            values (%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) 
            ON DUPLICATE KEY UPDATE 
                        field_name=%s, field_date=%s,field_time=%s,field_cpu_usr=%s,field_cpu_sys=%s,field_cpu_idle=%s,field_cpu_wai=%s,field_cpu_hiq=%s,field_cpu_siq=%s,field_dsk_read=%s,field_dsk_writ=%s,field_net_recv=%s,
                        field_memory_used=%s,field_memory_buff=%s,field_memory_buff=%s,field_memory_cach=%s,field_memory_free=%s """
    curs.execute(sql, (field_name, field_date, field_time, \
                    field_cpu_usr, field_cpu_sys, field_cpu_idle, field_cpu_wai, field_cpu_hiq, field_cpu_siq,\
                    field_dsk_read, field_dsk_writ, \
                    field_net_recv, field_net_send, \
                    field_memory_used, field_memory_buff, field_memory_cach, field_memory_free, \
                    field_name, field_date, field_time, \
                    field_cpu_usr, field_cpu_sys, field_cpu_idle, field_cpu_wai, field_cpu_hiq, field_cpu_siq,\
                    field_dsk_read, field_dsk_writ, \
                    field_net_recv, field_net_send, \
                    field_memory_used, field_memory_buff, field_memory_cach, field_memory_free))
    mariadb.commit()
    logging.debug("SQL End")

# def insert_gpdb():
#     try:
#         curs = greenplumdb.cursor()
#         tablename = str("infralogdb.schemainfralogdb" + '.' + project + '_' + str(yearmonth))
#         sql = """insert into """ + tablename + """(field_name, field_date, field_time, 
#                     field_cpu_usr, field_cpu_sys, field_cpu_idle, field_cpu_wai, field_cpu_hiq, field_cpu_siq,
#                     field_dsk_read, field_dsk_writ,
#                     field_net_recv, field_net_send,
#                     field_memory_used, field_memory_buff, field_memory_cach, field_memory_free) 
#             values (%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
#         sql_value = (field_name, field_date, field_time, \
#                 field_cpu_usr, field_cpu_sys, field_cpu_idle, field_cpu_wai, field_cpu_hiq, field_cpu_siq,\
#                 field_dsk_read, field_dsk_writ, \
#                 field_net_recv, field_net_send, \
#                 field_memory_used, field_memory_buff, field_memory_cach, field_memory_free)
#         curs.execute(sql, sql_value)
#         #print(sql)
#         greenplumdb.commit()
#     except:
#         print("GPDB insert ERROR")


def insert_gpdb():
    curs = greenplumdb.cursor()
    schemaname = str(greenplum_schemanameNo1 + "." + greenplum_database_name)
    sql = """insert into """ + schemaname + """(field_name, field_date, field_time, 
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






# def make_schema():

#     curs = greenplumdb.cursor()
#     sql = "CREATE SCHEMA  a115;"
#     curs.execute(sql)
#     greenplumdb.commit()
#     print("Schema Exits")


print("Let's Start!")

make_greenplum_database()
make_greenplum_schema("202107")
#make_schema()


for i in range (1, nodecount + 1):
    globals()['sdw{}'.format(i)] = "sdw" + str((i))

#for j in range(1, nodecount + 1):
    #globals()['sdw{}'.format(j) + '_parsed'] = open(options_write_dir + globals()['sdw{}'.format(j)] + '-' + date_year + '-' + date_moth + '-' + date_day, 'w')
    #globals()['sdw{}'.format(j) + '_parsed'] = open(options_write_dir + globals()['sdw{}'.format(j)] + '_parsed', 'w')
    #globals()['sdw{}'.format(j) + '_parsed_deli'] = open(options_write_dir + globals()['sdw{}'.format(j)] + '_parsed_01_deli', 'w')


for file_name in os.listdir(options_read_dir):
    if "sys." in file_name:
        yearmonth = file_name[4:10]
        make_greenplum_schema(yearmonth)

for file_name in os.listdir(options_read_dir):
    if "sys." in file_name:
        make_greenplum_schema(yearmonth)
        yearmonth = (str(file_name[4:8])) + (str(file_name[8:10]))
        date_year =  (str(file_name[4:8]))
        date_moth =  (str(file_name[8:10]))
        date_day =   (str(file_name[10:12]))
        for j in range(1, nodecount + 1):
            globals()['sdw{}'.format(j) + '_parsed'] = open(options_write_dir + globals()['sdw{}'.format(j)] + '-' + date_year + '-' + date_moth + '-' + date_day, 'w')

        source_file = open(options_read_dir + file_name, "r")  

        print(source_file)
        
        for data in source_file:
            for k in range(1, nodecount + 1):
                if globals()['sdw{}'.format(k)] in data:
                    x = []
                    x = (data.split('|'))
                    field_1st       = x[0].split()
                    field_name      = field_1st[0][1:-1]


                    field_2nd       = x[1].split()
                    field_date      = field_2nd[0]
                    field_time      = field_2nd[1]

                    field_3rd       = x[2].split()
                    field_cpu_usr   = (field_3rd[0])
                    field_cpu_sys   = (field_3rd[1])
                    field_cpu_idle  = (field_3rd[2])
                    field_cpu_wai   = (field_3rd[3])
                    field_cpu_hiq   = (field_3rd[4])
                    field_cpu_siq   = (field_3rd[5])

                    field_4th       = x[3].split()
                    field_dsk_read  = incloud_x(str(field_4th[0]))
                    field_dsk_writ  = incloud_x(str(field_4th[1]))
                
                    field_5th = x[4].split()
                    field_net_recv  = field_5th[0]
                    field_net_recv  = incloud_x(str(field_5th[0]))
                    field_net_send  = incloud_x(str(field_5th[1]))

                    field_6th = x[5].split()
                    field_memory_used   = incloud_x(str(field_6th[0]))
                    field_memory_buff   = incloud_x(str(field_6th[1]))
                    field_memory_cach   = incloud_x(str(field_6th[2]))
                    field_memory_free   = incloud_x(str(field_6th[3]))

                    writedata = field_name + "," + field_date + "," + field_time + "," \
                    + field_cpu_usr + "," + field_cpu_sys + "," + field_cpu_idle + "," + field_cpu_wai + ',' + field_cpu_hiq + "," + field_cpu_siq + ","  \
                    + field_dsk_read + "," + field_dsk_writ  + "," \
                    + field_net_recv + "," + field_net_send + "," \
                    + field_memory_used + "," + field_memory_buff + "," + field_memory_cach + "," + field_memory_free + "\n"

                    
                    #print("Data: " + date_year + "-" + date_moth + "-" + date_day + '-' + field_name)
                    
                    globals()['sdw{}'.format(k) + '_parsed'].writelines(writedata)

                    # writefilename = open((options_write_dir +  date_year + "-" + date_moth + "-" + date_day + '-' + field_name) + ".log", 'w')
                    # writefilename.writelines(writedata)


                    #insert_mariadb()                    
                    insert_gpdb()

# for j in range(1, nodecount + 1):
#     globals()['sdw{}'.format(k) + '_parsed'].close 

print("Finished")