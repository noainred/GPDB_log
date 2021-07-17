import os
import sys

dev_name = "JunHoui-MacBookPro.local12345"

if "MAC" in dev_name:
    options_read_dir = '/Users/junho/Downloads/parsing/data/'
    options_write_dir = '/Users/junho/Downloads/parsing/'
    print("include MAC")

elif "123" in dev_name:
    options_read_dir = '/Users/junho/Downloads/parsing/data/'
    options_write_dir = '/Users/junho/Downloads/parsing/'
    print("include 123")
else:
    print("ERROR!! please Defind operation mode")
    sys.exit()