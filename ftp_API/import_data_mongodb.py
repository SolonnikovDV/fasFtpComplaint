import glob

import PySimpleGUI as sg # do not works in DAG airflow
import pymongo

from xml_parse import parse_xml, get_file_list  # type: ignore

# local import
# import config as cfg

# PROJECT_PATH = cfg.PROJECT_PATH
PROJECT_PATH = '/opt/airflow/dags/'


def connect_to_mongo():
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    # database name
    db_name = client["startup"]
    # collection name
    tab_name = db_name["monitoring"]
    return tab_name


def data_import():
    tab_name = connect_to_mongo()
    # get the list of xml files in 'ftp_files/xml' folder
    _list = get_file_list()
    # init local metrics for console log out
    total = len(glob.glob(f'{PROJECT_PATH}ftp_files/xml/*'))
    # inserting parsed .xml to db

    # part with progress bar, works only in python
    # <editor-fold desc="Download with progress bar">
    for i, xml_file in enumerate(_list):
        # show download progress bar
        sg.one_line_progress_meter('Download progress',
                                   i + 1,
                                   len(_list),
                                   no_button=True,
                                   size=(50, 50),
                                   keep_on_top=True,
                                   key=xml_file)
        dict_data = parse_xml(f'{PROJECT_PATH}ftp_files/xml/{xml_file}')
        tab_name.insert_one(dict_data)
    # </editor-fold>

    # part without progress bar for apache-airflow
    # <editor-fold desc="Download for apache-airflow">
    # for xml_file in _list:
    #     dict_data = parse_xml(f'{PROJECT_PATH}ftp_files/xml/{xml_file}')
    #     tab_name.insert_one(dict_data)
    # </editor-fold>
    print(f'Inserted {total} .xml documents.')


def collection_filter():
    tab_name = connect_to_mongo()
    # remove null value
    tab_name.find({'customer': {'$ne': 'null'}})
    print('Documents with null value of "customer" field has been removed')
    # sort collection by date
    tab_name.find().sort('acceptDate', pymongo.ASCENDING)


def drop_data():
    tab_name = connect_to_mongo()
    tab_name.delete_many({})


data_import()

# optional using func, in the demo project all filters were added into UI MongoDb Compass
# collection_filter()


if __name__ == '__main__':
    print('Importing data')
