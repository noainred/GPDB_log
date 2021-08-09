import csv
import re
import os
import pymysql
import logging
import sys
import psycopg2
import socket

new = newlist[]

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
### PRD mode ### Greenplum ###
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

def current_hostname():
    a = socket.gethostname()
    return a

hostname = current_hostname()
print(hostname)
try:
    if dev_name in hostname:
        mode = "dev_mode"
        options_read_dir    = dev_options_read_dir
        options_write_dir   = dev_options_write_dir
        print(mode)

    elif prd_name is hostname:
        mode = "prd_mode"
        options_read_dir    = prd_options_read_dir
        options_write_dir   = prd_options_write_dir
        print(mode)

    else:
        print("check hostname")
        sys.exit
except:
    print("check hostname")
    sys.exit



for file_name in os.listdir(options_read_dir):
    if ".csv" in file_name:
        print(file_name)

        with open(options_read_dir + file_name) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            data = list(csv_reader)
            for i in range(0, len(data)):
                try:
                    list1 = list(data[i])
                    if  ppp in  list1[0]:
                        print(data[i])
                        
                    #print(list1[0])
                except:
                    #print("")
                    pass


            
            
    

print("Finished")