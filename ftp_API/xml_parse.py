from datetime import datetime
from bs4 import BeautifulSoup  # type: ignore
import os
# local import
import config as cfg

PROJECT_PATH = cfg.PROJECT_PATH


def parse_xml(path_to_xml: str):
    with open(path_to_xml, 'r', encoding='utf-8') as f:
        data_dict = {}
        file = f.read()
        soup = BeautifulSoup(file, 'xml')
        try:
            data_dict = {
            'complaintNumber': ''.join(soup.find('complaintNumber').text),
            'acceptDate': ''.join(soup.find('publishDate').text),
            'decisionPlace': soup.find('decisionPlace').text,
            'considerationKO': soup.find('considerationKO').find('fullName').text,
            'createUser': soup.find('createUser').text,
            'customer': soup.find('customer').find('fullName').text,
            'customer_INN': soup.find('customer').find('INN').text,
            'customer_KPP': soup.find('customer').find('KPP').text,
            'applicantNew': soup.find('applicantNew').findNext().findNext().text,
            'purchaseNumber': soup.find('purchaseNumber').text,
            'purchaseCode': soup.find('purchaseCode').text,
            'purchaseName': soup.find('purchaseName').text,
            'purchasePlacingDate': soup.find('purchasePlacingDate').text,
            'purchaseUrl': soup.find('printForm').find('url').text,
            }

        except AttributeError:
            print(data_dict)
        return data_dict


def get_file_list():
    list_of_files = []
    for root, dirs, files in os.walk(f'{PROJECT_PATH}ftp_files/xml'):
        for filename in files:
            list_of_files.append(filename)
    return list_of_files

# test case
# parse_xml(f'{PROJECT_PATH}ftp_files/xml/complaint_20190019274004507_1974946.xml')
# get_file_list()


if __name__ == '__main__':
    print('')