import pandas as pd
from urllib import request
import  os
import pyarrow as pa
import pyarrow.parquet as pq

import json
import glob


def convert_format(syear: str,smonth: str,files_info) -> None:
    '''
    Download CSV file from dataset and convert it to parquet file.
    Arguments:
        syear: srt: year
        smonth: srt: month
        files_info: json: contain url directory and data directory path to save parquet data
    Return:
        none
    '''
    
    #read the url and data_directory 
    remote_url_directory=files_info['remote_url_directory']
    data_directory=files_info['data_directory']

    #Transform the data directory path  to absolut path
    dir_data=os.path.abspath(data_directory)+"/"

    # Create the file_name
    taxi_kinds=files_info["taxi_kinds"]
    for taxi in taxi_kinds:
        #create parqeut file name with path and name of taxi
        parquet_file=dir_data+taxi+"_par"+"/"+taxi+"_tripdata_"+syear+"-"+smonth+".parquet"
        
        #check if the directory exist. if no make it.
        try:
            os.mkdir(dir_data+taxi+"_par"+"/")
        except OSError as error:
            a=1

        #check if the parqeut file exist. if yes, do not download it again
        if os.path.isfile(parquet_file):
            print('file exist:',parquet_file)
            print('continue the loop')
            continue

        #create the csv full url path 
        csv_file=remote_url_directory+taxi+"_tripdata_"+syear+"-"+smonth+".csv"

        #download and transform
        print(f'Download and Convert {csv_file} to {parquet_file}')
        file_to_data_frame_to_parquet(csv_file,parquet_file)
        print(f'Finished {csv_file} to {parquet_file}')

import pyarrow as pa
import pyarrow.csv
import urllib.request


def file_to_data_frame_to_parquet(csv_file: str,parquet_file: str) -> None:
    '''
    Download CSV file from dataset and convert it to parquet file.
    Arguments:
        csv_file: srt: csv url
        parquet_file: srt: absolut parquet file path to write
    Return:
        none
    '''
    pokemon = urllib.request.urlopen(csv_file)
    table = pyarrow.csv.read_csv(pokemon)
    pa.parquet.write_table(table, parquet_file)

