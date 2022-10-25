import csv
import ftplib
import os
import shutil
import urllib.request
import zipfile
from contextlib import closing
from datetime import datetime

import pandas as pd
from pandas.errors import EmptyDataError  # type: ignore
# from tqdm import tqdm # do not works in DAG airflow , comment when using in DAG

# local import
import config as cfg

DATE_TIME = datetime.now().strftime("%d-%m-%Y, %H:%M:%S")
# PROJECT_PATH = cfg.PROJECT_PATH

# path for DAG in airflow container
PROJECT_PATH = '/opt/airflow/dags/'

'''
# get_ftp_data_list() connecting to FTP, getting list of csv_files files adn creating 'to_download_list.csv_files', return nothing;
# diff_list() checking for diff in downloaded list and list to_download, 
using 'to_download_list.csv_files' creating with get_ftp_data_list(), return nothing;
# get_download_list() creating download list, using csv_files files creating with diff_list(), 
return dict with list of files to download;
# download_data() are include get_download_list(), and download files from FTP, return nothing;
# # delete_file() deletion all no xml files from folder;
# unzip_file(zip_file: str) unzip downloaded files, include delete_file();

The order to run:
# get_ftp_data_list() > diff_list() > download_data()
'''


def get_ftp_data_list():
    try:
        # connecting tp FTP server 'zakupki.gov.ru'
        # server name: 'ftp.zakupki.gov.ru', user: 'free', password: 'free'
        with ftplib.FTP('ftp.zakupki.gov.ru', 'free', 'free') as ftp:
            print(f'>>INFO  {DATE_TIME}: FTP connection successful')
            ftp.encoding = 'utf-8'
            ftp.dir()
            # folder with FAS complaints
            files = ftp.nlst(f'fcs_fas/complaint/')

            # save list of files for download to 'to_download_list.csv_files' file
            with open(f'{PROJECT_PATH}ftp_files/csv/to_download_list.csv',
                      'w',
                      encoding='utf-8',
                      newline='') as csv_file:
                writer = csv.writer(csv_file)
                writer.writerow(['id', 'zip_file_name'])

                for file in files:
                    # returns only complaint with '.zip' file extension
                    file_part = file.split('complaint_')
                    # temporary measure to fast start: return date period from YYYY==2020 to YYYY==2029
                    # files we are needed are always splits for two parts with '.split('complaint_')'
                    if len(file_part) == 2:
                        if '202' in str(f'complaint_{file_part[1]}'):
                            # filling the '.csv_files' with list of files to download, 1st row starts with id=793
                            writer.writerow([int(files.index(file)), str(f'complaint_{file_part[1]}')])
                    else:
                        pass

    except NameError as e:
        print(f'>>INFO  {DATE_TIME}: FTP connection failed: {e}')
    ftp.close()
    print(f'>>INFO  {DATE_TIME}: FTP connection closed')


def diff_list():
    global df
    # check 'downloaded_files.csv_files' for empty
    try:
        df = pd.read_csv(f'{PROJECT_PATH}ftp_files/csv/downloaded_files.csv')
        print('Downloaded_files is not empty')
        # compare 'to_download_list.csv_files' with 'downloaded_list.csv_files' and get diff
        with open(f'{PROJECT_PATH}ftp_files/csv/downloaded_files.csv', 'r') as f1, open(
                f'{PROJECT_PATH}ftp_files/csv/to_download_list.csv', 'r') as f2:
            print('Calculate the diff')
            downloaded_files = f1.readlines()
            to_download_list = f2.readlines()
        # write diff to 'diff_list.csv_files'
        with open(f'{PROJECT_PATH}ftp_files/csv/diff_list.csv', 'w') as out_file:
            count = 0
            for line in to_download_list:
                if line not in downloaded_files:
                    out_file.write(line)
                    count += 1
                    if count > 0:
                        print(f'Added next files:\n{line}')
    except EmptyDataError as e:
        print(f'EmptyDataError exception in "downloaded_files.csv" : {e}')
        print('"downloaded_files.csv" is empty')
    finally:
        # copy 'to_download_list.csv_files' 'as downloaded_files.csv_files'
        to_download_list = f'{PROJECT_PATH}ftp_files/csv/to_download_list.csv'
        downloaded_files = f'{PROJECT_PATH}ftp_files/csv/downloaded_files.csv'
        shutil.copy(to_download_list, downloaded_files)


# func get list of files to download from FTP
def get_download_list():
    # check 'diff_list.csv_files' for empty
    try:
        # if diff is not empty then return diff and save result in a dictionary
        diff_list = pd.read_csv(f'{PROJECT_PATH}ftp_files/csv/diff_list.csv')
        diff_list_to_dict = dict(zip(list(diff_list['id']), list(diff_list['zip_file_name'])))
        print(f'Diff_list is not empty. Returned Diff_list')
        return diff_list_to_dict
    except EmptyDataError as e:
        # if diff_list is empty rise exception and move to finally section
        print(f'Diff_list is empty: EmptyDataError exception {e}\nReturned Downloaded_files')
    finally:
        # if diff is empty return downloaded files list and save result in a dictionary
        downloaded_list = pd.read_csv(f'{PROJECT_PATH}ftp_files/csv/downloaded_files.csv')
        # downloaded_list_to_dict = dict(zip(list(downloaded_list['id']), list(downloaded_list['zip_file_name'])))
        downloaded_list_to_dict = dict(zip(list(downloaded_list['id']), list(downloaded_list['zip_file_name'])))
        return downloaded_list_to_dict


# func download data from FTP with download list
def download_data():
    data_list = get_download_list()
    # download all .zip from data_list
    # for key, counter in tqdm(data_list.items()): # shows download progress bar into console, do not works in DAG airflow , comment when using in DAG
    for key in data_list.keys(): # works in apache-airflow
        # get names of files for download
        xml_zip_file = data_list.get(key)
        # downloading file from FTP
        with closing(urllib.request.urlopen(
                f'ftp://free:free@ftp.zakupki.gov.ru/fcs_fas/complaint/{xml_zip_file}')) as ftp_file:
            with open(f'{PROJECT_PATH}ftp_files/zip/{xml_zip_file}', 'wb') as local_file:
                shutil.copyfileobj(ftp_file, local_file)
                # print(f'Download --> {ftp_file}')
        # unziping files
        unzip_file(xml_zip_file)
        print('All data were download')


# fun calling in 'download_data()'
def unzip_file(zip_file: str):
    with zipfile.ZipFile(f'{PROJECT_PATH}ftp_files/zip/{zip_file}', 'r') as zip_ref:
        zip_ref.extractall(f'{PROJECT_PATH}ftp_files/xml/')
    # deleting no.xml files from folder
    delete_file()


# func delete all no .xml files in a folder 'parent_dir', fun calling in 'unzip_file()'
def delete_file():
    parent_dir = f'{PROJECT_PATH}ftp_files/xml/'
    no_delete_kw = '.xml'
    try:
        for (dir_path, dir_names, file_names) in os.walk(parent_dir):
            for file in file_names:
                # if file name has not '.xml' extension it will be deleted
                if no_delete_kw not in file:
                    os.remove(f'{dir_path}/{file}')
    except Exception as e:
        print(e)


if __name__ == '__main__':
    print('')
    # get_ftp_data_list()
    # diff_list()
    # download_data()
