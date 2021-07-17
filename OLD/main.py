import csv
import re
import os

nodecount = 8

options_read_dir = '/Users/junho/Downloads/parsing/data/'
options_read_filename = 'sys.20210401.log'
options_write_dir = '/Users/junho/Downloads/parsing/'

vari_write_filename_sdw = "write_filename_sdw"


vari_write_filename_sdw1 = options_read_filename + "_rewrite_" + "sdw" + "_"

for i in range (1, nodecount + 1):
    globals()['sdw_{}'.format(i)] = "sdw" + str('{}'.format(i))
    #print(globals()['sdw_{}'.format(i)])

try:
    source_file = open(options_read_dir  + options_read_filename, "r")
    source_file_line = source_file.read().splitlines()
except:
    print("Source File not found")


for i in range(1, nodecount + 1):
    globals()['sdw_{}'.format(i) + '_outfile'] = open(options_write_dir + globals()['sdw_{}'.format(i)] + '_outfile', 'w')


for line in source_file_line:
    #print(line)
    # if globals()['sdw_{}'.format(i)] in line:
    #     globals()['sdw_{}'.format(i) + '_outfile'].write(line)
    if 'sdw1' in line:
        print('sdw1')
        globals()['sdw_{}'.format(i) + '_outfile'].write(line)


# for data in source_file:
#     print(data)
#     for i in range(1, nodecount + 1):
#         if globals()['sdw_{}'.format(i)] in data:
#             globals()['sdw_{}'.format(i) + '_outfile'].write(data)

