import csv
import re
import os

nodecount = 9

options_read_dir = '/Users/junho/Downloads/parsing/data/'
options_read_filename = 'sys.20210401.log'
options_write_dir = '/Users/junho/Downloads/parsing/'

vari_write_filename_sdw = "write_filename_sdw"


vari_write_filename_sdw1 = options_read_filename + "_rewrite_" + "sdw" + "_"

for i in range (1, nodecount + 1):
    globals()['sdw{}'.format(i)] = "sdw" + str((i))
    #print(globals()['sdw_'.format(i)])

# try:
#     source_file = open(options_read_dir  + options_read_filename, "r")
# except:
#     print("Source File not found")


for j in range(1, nodecount + 1):
    #print(j)
    globals()['sdw{}'.format(j) + '_outfile'] = open(options_write_dir + globals()['sdw{}'.format(j)] + '_outfile', 'w')
    #globals()['sdw{}'.format(j) + '_outfile'].writelines("sdw")
    #print(globals()['sdw{}'.format(j) + '_outfile'])




for file_name in os.listdir(options_read_dir):
    if "sys." in file_name:
        source_file = open(options_read_dir + file_name, "r")
        print(source_file)
        for data in source_file:
            for k in range(1, nodecount + 1):
                if globals()['sdw{}'.format(k)] in data:
                    globals()['sdw{}'.format(j) + '_outfile'] = open(options_write_dir + globals()['sdw{}'.format(j)] + '_outfile', 'w')
                    globals()['sdw{}'.format(k) + '_outfile'].write(data)
                    globals()['sdw{}'.format(j) + '_outfile'].close




# fields = ('date', 'time', 'cpu_usr', 'cpu_sys', 'cpu_idle', 'cpu_wait' , 'cpu_hiq', 'cpu_siq',
#           'disk_read', 'disk_write', 'net_send', 'net_reveive',
#           'memory_used', 'memory_buff' , 'memory_cache' , 'memory_free')
