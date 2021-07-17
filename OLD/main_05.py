import csv
import re
import os
import pymysql
import logging

nodecount = 9

options_read_dir = '/Users/junho/Downloads/parsing/data/'
options_read_filename = 'sys.20210401.log'
options_write_dir = '/Users/junho/Downloads/parsing/'

vari_write_filename_sdw = "write_filename_sdw"
vari_write_filename_sdw1 = options_read_filename + "_rewrite_" + "sdw" + "_"


mariadb = pymysql.connect(
            host='localhost', 
            port=3306, user='root',
            passwd='my-secret-pw', 
            #db='gpdb_infra_mon', 
            charset='utf8', autocommit=True)


def make_database():
    curs = mariadb.cursor()
    sql = "CREATE DATABASE IF NOT EXISTS `gpdb_infra_mon`"
    curs.execute(sql)
    mariadb.commit()


def make_table(yearmonth):
    tablename = "gpdb_infra_mon." + str(yearmonth)
    curs = mariadb.cursor()
    sql = """ CREATE TABLE IF NOT EXISTS """ + str(tablename) + """ (
  `field_name` varchar(100) DEFAULT NULL,
  `field_date` varchar(100) DEFAULT NULL,
  `field_time` varchar(100) DEFAULT NULL,
  `field_cpu_usr` varchar(100) DEFAULT NULL,
  `field_cpu_sys` varchar(100) DEFAULT NULL,
  `field_cpu_idle` varchar(100) DEFAULT NULL,
  `field_cpu_wai` varchar(100) DEFAULT NULL,
  `field_cpu_hiq` varchar(100) DEFAULT NULL,
  `field_cpu_siq` varchar(100) DEFAULT NULL,
  `field_dsk_read` varchar(100) DEFAULT NULL,
  `field_dsk_writ` varchar(100) DEFAULT NULL,
  `field_net_recv` varchar(100) DEFAULT NULL,
  `field_net_send` varchar(100) DEFAULT NULL,
  `field_memory_used` varchar(100) DEFAULT NULL,
  `field_memory_buff` varchar(100) DEFAULT NULL,
  `field_memory_cach` varchar(100) DEFAULT NULL,
  `field_memory_free` varchar(100) DEFAULT NULL
) ;"""
    #tablename = "gpdb_infra_mon." + yearmonth
    curs.execute(sql)
    mariadb.commit()
    logging.debug("SQL End")
    mariadb.commit()

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

    
make_databse()
for i in range (1, nodecount + 1):
    globals()['sdw{}'.format(i)] = "sdw" + str((i))

for j in range(1, nodecount + 1):
    globals()['sdw{}'.format(j) + '_outfile'] = open(options_write_dir + globals()['sdw{}'.format(j)] + '_outfile', 'w')
    #globals()['sdw{}'.format(j) + '_outfile_deli'] = open(options_write_dir + globals()['sdw{}'.format(j)] + '_outfile_01_deli', 'w')

for file_name in os.listdir(options_read_dir):
    if "sys." in file_name:
        yearmonth = file_name[4:10]
        make_table(yearmonth)

for file_name in os.listdir(options_read_dir):
    if "sys." in file_name:
        yearmonth = file_name[4:10]
        make_table(yearmonth)

        source_file = open(options_read_dir + file_name, "r")  
        print(source_file)
        for data in source_file:
            for k in range(1, nodecount + 1):
                if globals()['sdw{}'.format(k)] in data:
                    x = []
                    x = (data.split('|'))
                    field_1st       = x[0].split()
                    field_name      = field_1st[0]

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

                    globals()['sdw{}'.format(j) + '_outfile'] = open(options_write_dir + globals()['sdw{}'.format(j)] + '_outfile', 'w')
                    globals()['sdw{}'.format(k) + '_outfile'].writelines(writedata)
                    globals()['sdw{}'.format(j) + '_outfile'].close


                    
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
                    mariadb.commit()
                    
                    

                    
                    #y = str(x[0]) + ',' + str(x[1]) + ',' + str(x[2]) + ',' + str(x[3]) + ',' + str(x[4]) + ',' + str(x[5])
                    #print(y)

                    



# fields = ('date', 'time', 'cpu_usr', 'cpu_sys', 'cpu_idle', 'cpu_wait' , 'cpu_hiq', 'cpu_siq',
#           'disk_read', 'disk_write', 'net_send', 'net_reveive',
#           'memory_used', 'memory_buff' , 'memory_cache' , 'memory_free')
