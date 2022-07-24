import datetime

#import sys, os
#sys.path.append(os.path.abspath('../subs'))
from csv_to_parquet import *

def _convert(year,month):
    with open("./info.json") as json_file:
        files_info = json.load(json_file)
    #print(files_info)

    files_info["data_directory"]="../data/"
    date=datetime.datetime(year,month, 1)
    print(date,date.strftime("%Y"),date.strftime("%m"))
    return convert_format(date.strftime("%Y"),date.strftime("%m"),files_info)

for i in range(36):
    first_year=2018
    first_month=8
    year=first_year+(first_month+i-1)//12
    month=(first_month+i-1)%12+1
    _convert(year,month)
#
# _convert()
