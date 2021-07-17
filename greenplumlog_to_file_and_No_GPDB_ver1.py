import csv
import re
import os
import logging
import sys
import socket



############################
#1. 프로젝트 이름(파일 이름 앞에 헤더/Prefix 로 붙음)
project = "sbp_oss1"

#2. Datanode 수량(GPDB의 경우만 해당됨, SDW 로 시작하는 로그 수량)
nodecount = 8

#3. 개발/운영 환경에 따라서 동작 변수 변경
dev_name = "JunHoui-MacBookPro.local"

#4. 데이터를 넣을 DBMS 종류, 현재 버전에서는 패키지 import 문제로 일단 삭제했음. 
### CASE: mariadb
#database = 'mariadb'
database = 'greenplum'
############################

if "MAC" in dev_name:
    options_read_dir    = '/Users/junho/Downloads/parsing/data/'
    options_write_dir   = '/Users/junho/Downloads/parsing/'

# elif "dev_machine" in dev_name: # 다른 개발 장비 처리하기 위한 Case
#     options_read_dir = '/Users/junho/Downloads/parsing/data/'
#     options_write_dir = '/Users/junho/Downloads/parsing/'
else:
    print("ERROR!! please Defind operation mode")
    sys.exit()

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


print("Let's Start!")


for i in range (1, nodecount + 1):
    globals()['sdw{}'.format(i)] = "sdw" + str((i))

for file_name in os.listdir(options_read_dir):
    if "sys." in file_name:
        yearmonth = file_name[4:10]


for file_name in os.listdir(options_read_dir):
    if "sys." in file_name:
        yearmonth =   (str(file_name[4:8])) + (str(file_name[8:10]))
        date_year =   (str(file_name[4:8]))
        date_moth =   (str(file_name[8:10]))
        date_day  =   (str(file_name[10:12]))

        for j in range(1, nodecount + 1):
            if os.path.exists(options_write_dir + globals()['sdw{}'.format(j)] + '-' + date_year + '-' + date_moth + '-' + date_day):
                os.remove(options_write_dir + globals()['sdw{}'.format(j)] + '-' + date_year + '-' + date_moth + '-' + date_day)
                print("file deleted : " + options_write_dir + globals()['sdw{}'.format(j)] + '-' + date_year + '-' + date_moth + '-' + date_day)

        for j in range(1, nodecount + 1):
            globals()['sdw{}'.format(j) + '_parsed'] = open(options_write_dir + date_year + '-' + date_moth + '-' + date_day + '-' + globals()['sdw{}'.format(j)], 'w')

        source_file = open(options_read_dir + file_name, "r")  

        print(source_file)
        
        for data in source_file:
            for k in range(1, nodecount + 1):
                try:
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
                        
                        globals()['sdw{}'.format(k) + '_parsed'].writelines(writedata)
                except:
                    #print("Parsing Error : " + source_file)
                    continue

                    #insert_gpdb()

print("Finished")