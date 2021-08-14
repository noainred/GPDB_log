import io
import csv
import os
import sys
import psycopg2
import socket
import pandas as pd
from sqlalchemy import create_engine

dev_name = "JunHoui-MacBookPro.local"
prd_name = "lesa"
ppp = "-"
############################
project = "ensol_dvc"
nodename = "sbp_oss_01"
nodecount = 8
############################

##########################################################
### PRD mode ###
prd_options_read_dir    = '/Users/junho/Downloads/parsing/data/'
prd_options_write_dir   = '/Users/junho/Downloads/parsing/'
### DEV mode ###
dev_options_read_dir    = '/Users/junhopark/OneDrive/GitHubSync/GPDB_log/dev_data/sampledata1/'
dev_options_write_dir   = '/Users/junhopark/OneDrive/GitHubSync/GPDB_log/dev_data/sampledata1_output/'
##########################################################

### Dev mode ### Greenplum ###
dev_database                = 'greenplum'
dev_greenplum_hostname      = "172.16.198.4"
#dev_greenplum_hostname      = "localhost"
dev_greenplum_port_no       = int("5432")
dev_greenplum_user_name     = "postgres"
dev_greenplum_user_pass     = "pivotal"
dev_greenplum_database_name = 'infralogdb'
dev_greenplum_schemanameNo1 = "sbp"
dev_greenplum_tablenameNo1  = "gpdb_oss1"
####################################################################################
### PRD mode ### Greenplum ###
prd_database                = 'greenplum'
prd_greenplum_hostname      = "172.16.198.4"
#prd_greenplum_hostname      = "localhost"
prd_greenplum_port_no       = int("5432")
prd_greenplum_user_name     = "postgres"
prd_greenplum_user_pass     = "pivotal"
prd_greenplum_database_name = 'infralogdb'
prd_greenplum_schemanameNo1 = "sbp"
prd_greenplum_tablenameNo1  = "gpdb_oss1"
####################################################################################

def current_hostname():
    a = socket.gethostname()
    return a

hostname = current_hostname()
print(hostname)

def check_operating_env():
    try:
        if dev_name in hostname:
            mode = "dev_mode"
            options_read_dir    = dev_options_read_dir
            options_write_dir   = dev_options_write_dir
            # database
            run_database                = dev_database
            run_greenplum_hostname      = dev_greenplum_hostname
            run_greenplum_port_no       = dev_greenplum_port_no
            run_greenplum_user_name     = dev_greenplum_user_name
            run_greenplum_user_pass     = dev_greenplum_user_pass
            run_greenplum_database_name = dev_greenplum_database_name
            run_greenplum_schemanameNo1 = dev_greenplum_schemanameNo1
            run_greenplum_tablenameNo1  = dev_greenplum_tablenameNo1\
            
            print(mode)

        elif prd_name is hostname:
            mode = "prd_mode"
            options_read_dir    = prd_options_read_dir
            options_write_dir   = prd_options_write_dir

            # database
            run_database                = prd_database
            run_greenplum_hostname      = prd_greenplum_hostname
            run_greenplum_port_no       = prd_greenplum_port_no
            run_greenplum_user_name     = prd_greenplum_user_name
            run_greenplum_user_pass     = prd_greenplum_user_pass
            run_greenplum_database_name = prd_greenplum_database_name
            run_greenplum_schemanameNo1 = prd_greenplum_schemanameNo1
            run_greenplum_tablenameNo1  = prd_greenplum_tablenameNo1
            print(mode)

        else:
            print("check hostname")
            sys.exit
    except:
        print("check hostname")
        sys.exit

    return options_read_dir, options_write_dir, run_database, run_greenplum_hostname, run_greenplum_port_no, run_greenplum_user_name, run_greenplum_user_pass, run_greenplum_database_name, run_greenplum_schemanameNo1, run_greenplum_tablenameNo1

def parsing_save():
    for file_name in os.listdir(options_read_dir):
        if ".csv" in file_name:
            print(file_name)

            with open(options_read_dir + file_name) as read_csv_file:
                with open(options_write_dir + file_name, 'w', newline='') as f:
                    writer = csv.writer(f)
                    csv_reader = csv.reader(read_csv_file, delimiter=',')
                    data = list(csv_reader)
                    for i in range(0, len(data)):
                        try:
                            #print(i)
                            list1 = list(data[i])
                            if  ppp in  list1[0]:
                                print(data[i])
                                writer.writerow(data[i])

                            #print(list1[0])
                        except:
                            #print("")
                            pass
                f.close

def connect_to_gpdb():
    greenplumpdb = psycopg2.connect(
        host        =   run_greenplum_hostname, 
        port        =   run_greenplum_port_no, 
        database    =   run_greenplum_database_name,     
        user        =   run_greenplum_user_name,
        password    =   run_greenplum_user_pass)
    return greenplumpdb


def make_greenplum_schema(greenplumpdb,run_greenplum_schemanameNo1):
    print(greenplumpdb)
    print(run_greenplum_schemanameNo1)
    try:
        curs = greenplumpdb.cursor()
        #sql = "CREATE SCHEMA " + greenplum_database_name + "." + greenplum_schemanameNo1 + ";"
        sql = "CREATE SCHEMA  " + run_greenplum_schemanameNo1 + ";"
        curs.execute(sql)
        curs.close
        greenplumpdb.commit()
        print("made schema : " + run_greenplum_schemanameNo1)
    except:
        print("Already schema Exist! or ERROR")
        curs.close
        greenplumpdb.commit()
        print(sql)


def make_greenplum_schema_table(greenplumpdb, run_greenplum_schemanameNo1, run_greenplum_tablenameNo1):
    # try:

    curs = greenplumpdb.cursor()
    sql = """CREATE TABLE IF NOT EXISTS  """ + run_greenplum_schemanameNo1 + "." + run_greenplum_tablenameNo1 +  "(" + """
            	"date" date NULL,
	            cpu_usr float8 NULL,
                cpu_sys float8 NULL,
                cpu_idl float8 NULL,
                cpu_wai float8 NULL,
                cpu_siq float8 NULL,
                dsk_read float8 NULL,
                dsk_writ float8 NULL,
                io_read float8 NULL,
                io_writ float8 NULL,
                paging_in float8 NULL,
                paging_out float8 NULL,
                interrupts_42 float8 NULL,
                interrupts_45 float8 NULL,
                interrupts_46 float8 NULL,
                load_avg_1m float8 NULL,
                load_avg_5m float8 NULL,
                load_avg_15m float8 NULL,
                memory_usage_used float8 NULL,
                memory_usage_cache float8 NULL,
                memory_usage_free float8 NULL,
                net_recv float8 NULL,
                net_sent float8 NULL,
                swap_used float8 NULL,
                swap_free float8 NULL
            );
    """
    print(sql)
    curs.execute(sql)
    curs.close
    greenplumpdb.commit()
    # except:
    #     print("Already databse Exist! or ERROR")
    #     print(sql)

def insert_to_gpdb(greenplumpdb):
    curs = greenplumpdb.cursor()
    f = open('SampleData1.csv')
    curs.copy_from(f, '"Real".sampledataone', sep=',', null='')

####
def load_csv_then_df_then_import_to_gpdb(options_write_dir, run_greenplum_user_name, run_greenplum_user_pass, run_greenplum_hostname, run_greenplum_database_name, run_greenplum_tablenameNo1):
    engine = create_engine("postgresql://" + run_greenplum_user_name + ':' + run_greenplum_user_pass + '@' + run_greenplum_hostname + ':' + '5432/' + run_greenplum_database_name)

    for file_name in os.listdir(options_write_dir):
        if ".csv" in file_name:
            print(file_name)

            with open(options_write_dir + file_name) as read_csv_file:
                csv_reader = csv.reader(read_csv_file, delimiter=',')
                data = list(csv_reader)
                #print(data)
                df = pd.DataFrame(data)
                df.to_sql(run_greenplum_tablenameNo1, engine)
    # for file_name in os.listdir(options_write_dir):
    #     if ".csv" in file_name:
    #         print(file_name)

    #         with open(options_write_dir + file_name) as read_csv_file:
    #             csv_reader = csv.reader(read_csv_file, delimiter=',')
    #             data = list(csv_reader)
    #             #print(data)
    #             df = pd.DataFrame(data)
    #             print(df)
    #                 # for i in range(0, len(data)):
    #                 #     try:
    #                 #         #print(i)
    #                 #         list1 = list(data[i])
    #                 #         if  ppp in  list1[0]:
    #                 #             print(data[i])




##############
print("Start!")
# Check Operating Environment
options_read_dir, options_write_dir, run_database, run_greenplum_hostname, run_greenplum_port_no, run_greenplum_user_name, run_greenplum_user_pass, run_greenplum_database_name, run_greenplum_schemanameNo1, run_greenplum_tablenameNo1 = check_operating_env()

print("Read  Dir. is : " + options_read_dir)
print("Write Dir. is : " + options_write_dir)
# parsing_save()

load_csv_then_df_then_import_to_gpdb(options_write_dir, run_greenplum_user_name, run_greenplum_user_pass, run_greenplum_hostname, run_greenplum_database_name, run_greenplum_tablenameNo1)
# greenplumpdb = connect_to_gpdb()
# make_greenplum_schema(greenplumpdb,run_greenplum_schemanameNo1)
# make_greenplum_schema_table(greenplumpdb, run_greenplum_schemanameNo1, run_greenplum_tablenameNo1)


print("Finished!")