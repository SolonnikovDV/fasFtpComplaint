import psycopg2 as psycopg2
import datetime
import csv
from psycopg2.extensions import AsIs

# local import
import config as cfg
from xml_parse import parse_xml, get_file_list


DATA_TIME = datetime.datetime.now()
TAB_NAME = 'procmonitoring'
PROJECT_PATH = cfg.PROJECT_PATH


def create_connection_to_psql(host, port, dbname, user, password):
    conn = psycopg2.connect(f"""
            host={host}
            port={port}
            dbname={dbname}
            user={user}
            password={password}
            target_session_attrs=read-write
            """)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute('SELECT version()')
        print(f'[INFO] : {DATA_TIME} >> Server version : \n{cur.fetchone()}')
        return conn

def create_table():
    conn = create_connection_to_psql(
        cfg.HOST,
        cfg.PORT,
        cfg.DBNAME,
        cfg.USER,
        cfg.PASSWORD
    )
    try:
        conn.autocommit = True
    # create table
        with conn.cursor() as cur:
            sql_query = f"""
            CREATE TABLE IF NOT EXISTS {TAB_NAME}(
            complaintNumber varchar,
            acceptDate varchar,
            decisionPlace varchar,
            considerationKO varchar,
            createUser varchar, 
            customer varchar,
            customer_INN varchar,
            customer_KPP varchar,
            applicantNew varchar,
            purchaseNumber varchar,
            purchaseCode varchar,
            purchaseName varchar,
            purchasePlacingDate varchar,
            purchaseUrl varchar
            );"""
            cur.execute(sql_query)
            print(f'''[INFO] : {DATA_TIME} >> Table '{TAB_NAME}' created successful''')

    except Exception as e:
        print(f'[INFO] : {DATA_TIME} >> Error creation table : ', e)
    finally:
        if conn:
            conn.close()
            print(f'[INFO] : {DATA_TIME} >> Psql connection closed')


def insert_data_to_table():
    conn = create_connection_to_psql(
        cfg.HOST,
        cfg.PORT,
        cfg.DBNAME,
        cfg.USER,
        cfg.PASSWORD
    )
    try:
        conn.autocommit = True

        with conn.cursor() as cur:
            list_of_files = get_file_list()
            print(list_of_files)
            for xml_file in list_of_files:
                # get dict from parser
                dict_data = parse_xml(f'{PROJECT_PATH}ftp_files/xml/{xml_file}')
                print(f'xml file {xml_file}')
                # import dict to psql
                columns = dict_data.keys()
                values = [dict_data[column] for column in columns]

                sql_query = f'''insert into {TAB_NAME} (%s) 
                            values %s;'''
                cur.execute(sql_query, (AsIs(','.join(columns)), tuple(values)))
            print(f'''[INFO] : {DATA_TIME} >> Data inserted in the table <{TAB_NAME}>''')

    except Exception as e:
        print(f'[INFO] : {DATA_TIME} >> Error inserting : ', e, )
    finally:
        if conn:
            conn.close()
            print(f'[INFO] : {DATA_TIME} >> Psql connection closed')


# create_connection_to_psql(cfg.HOST, cfg.PORT, cfg.DBNAME, cfg.USER, cfg.PASSWORD)
# create_table()
insert_data_to_table()

if __name__ == '__main__':
    print('')