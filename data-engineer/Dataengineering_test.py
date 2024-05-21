#!/usr/bin/env python
# coding: utf-8

# Import required libraries
import os
import pandas as pd
import xml.etree.ElementTree as ETree 
import sqlite3
import logging
from typing import List, Dict


#Logging
logging.basicConfig(filename="D:/PythonPrograms/data-challenges-main/newfile.log",filemode='w')
 
# Creating an object
logger = logging.getLogger()
 
# Setting the threshold of logger to DEBUG
logger.setLevel(logging.DEBUG)

# function to get te list of all the .xml files
def read_files_from_dir(dir: str) -> List[str]:
    logging.info('Execution of read_files_from_dir function to get the list of all files')
    list_file_names = []
    if os.path.exists(dir):  
        dir_list = os.listdir(dir)
        if dir_list == []:
            logging.error("Error: No Files Present.")
        else:
            for file_name in dir_list:
                logging.info(file_name+' is available')
                if os.path.splitext(file_name)[-1].lower() =='.xml':
                    file_path = os.path.join(dir, file_name)
                    list_file_names.append(file_path)

    else:
        logging.error('Directory'+ dir+' is not available')
    logging.info('Execution of read_files_from_dir function finished')
    return list_file_names


# function to parse xml files
def parse_xml(files: List[str]) -> pd.DataFrame:
    logging.info('Execution of parse_xml function is initiated')
    data = []
    
    for file_name in files:
        try:
            logging.info('Parsing file: ')
            prstree = ETree.parse(file_name)
            for event in prstree.iter('event'):
                order_id = event.find('order_id').text
                date_time = event.find('date_time').text
                status = event.find('status').text
                cost = event.find('cost').text
                technician = event.find('repair_details/technician').text
                parts = [(part.attrib['name'], part.attrib['quantity']) for part in event.findall('repair_details/repair_parts/part')]
                data.append([order_id, date_time, status, cost, technician, parts])
            logging.info('Parsing file completed: ')
        except ETree.ParseError:
            logging.error('Error in ')

    #conversion of partname and quantity field as a seperate field.
    df_event_updates = pd.DataFrame(data, columns=['order_id', 'date_time', 'status', 'cost', 'technician', 'parts']) 
    df_event_updates = df_event_updates.explode('parts')
    df_event_updates['partname'] = df_event_updates['parts'].str[0]
    df_event_updates['quantity'] = df_event_updates['parts'].str[1]
    df_event_updates = df_event_updates[['order_id', 'date_time', 'status', 'cost', 'technician', 'partname','quantity']]
    logging.info('Execution of parse_xml function is completed')
    return df_event_updates


# function to get latest updates based on date interval
def window_by_datetime(df_RO: pd.DataFrame, window: str) -> Dict[str, pd.DataFrame]:
    try:
        logging.info('Execution of window_by_datetime initiated')
        df_RO['date_time_temp'] = pd.to_datetime(df_RO['date_time'], format='%Y-%m-%dT%H:%M:%S')
        df_max = df_RO.groupby(pd.Grouper(key='date_time_temp', axis=0,freq=window)).max()
        df_max = df_max.reset_index()
        df_max= df_max[['date_time','order_id', 'status', 'cost', 'technician', 'partname','quantity']]
        df_RO = df_RO.merge(df_max,how='inner', on='date_time')
        df_RO = df_RO.rename(columns ={'order_id_x':'order_id','status_x':'status','cost_x':'cost','technician_x':'technician','partname_x':'partname','quantity_x':'quantity'})
        df_RO= df_RO[['date_time','order_id', 'status', 'cost', 'technician', 'partname','quantity']]
        windows={}
        for index,rows in df_RO.iterrows():
                windows[rows['date_time']]=pd.DataFrame(df_RO.where(df_RO['date_time']== rows['date_time']))
    except Exception as e:
        logging.error('Error while execution of window_by_datetime function ',e)
    logging.info('Execution of window_by_datetime completed')
    return windows


# Repair Order class for table structure
class RO:
    def __init__(self, order_id, date_time, status, cost, technician, partname,quantity):
        self.order_id = order_id
        self.date_time = date_time
        self.status = status
        self.cost = cost
        self.technician = technician
        self.partname = partname
        self.quantity = quantity


# function to transform dataset into RO format
def process_to_RO(data: Dict[str, pd.DataFrame]) -> List[RO]:
    try:
        logging.info('Execution of process_to_RO function initiated')
        list_repair_order = []
        for date_time_window, df_event_updates in data.items():
            for index, row in df_event_updates.iterrows() :
                if ( len(str(row['date_time']))>3 ):
                    repair_order = RO(row['order_id'], row['date_time'], row['status'], row['cost'], row['technician'], row['partname'],row['quantity'])
                    list_repair_order.append(repair_order)
    except Exception as e:
        logging.error('Error while execution of process_to_RO function', e)
    logging.info('Finished processing data into RO format')

    return list_repair_order


# function connect to dataase
def connect_db(dbname):
    try:
        logging.info('Execution of connect_db function initiated to connect to database')
        conn = sqlite3.connect(dbname)
        logging.info("database conenction established")
        c = conn.cursor()
        c.execute('''DROP TABLE repair_order ''')
        c.execute('''CREATE TABLE IF NOT EXISTS repair_order (order_id INTEGER,date_time TEXT,status TEXT,cost REAL,technician TEXT,partname TEXT,quantity REAL)''')
    except Exception as e:
        logging.error('Error while execution of connect_db function', e)
    logging.info('Execution of connect_db function completed to connect to database')    
    return conn


# function to write data 
def write_to_sqlite(ro_list: List[RO], db_name: str,conn):
    try:
        logging.info(' Execution of write_to_sqlite function initiated')
        conn = sqlite3.connect(db_name)
        for ro in ro_list:
    #         print(ro)
            conn.execute("INSERT INTO repair_order (order_id, date_time, status, cost, technician, partname, quantity) VALUES (?, ?, ?, ?, ?, ?,?)",
                         (ro.order_id, ro.date_time, ro.status, ro.cost, ro.technician,ro.partname,ro.quantity))

        conn.commit()
        conn.close()
    except Exception as e:
        logging.info('Error while execution of write_to_sqlite function')
    logging.info('Execution of write_to_sqlite function completed')


# function to read data from table
def read_to_sqlite(db_name: str):
    try:
        logging.info('Execution of read_to_sqlite function initiated')
        conn = sqlite3.connect(db_name)
        sql_select_Query = "select * from repair_order"
        cursor = conn.cursor()
        cursor.execute(sql_select_Query)
        # get all records
        records = cursor.fetchall()
        print("Total number of rows in table: ", cursor.rowcount)

        print("\nPrinting each row")
        for row in records:
            print( row)

        conn.commit()
        conn.close()
    except Exception as e:
        logging.info('Error while execution of read_to_sqlite function',e)
    logging.info('Execution of read_to_sqlite function completed')


# function to execute all the steps
def pipeline(dir: str, window: str, db_name: str):
    #Logging
    logging.basicConfig(filename="D:/PythonPrograms/data-challenges-main/newfile.log",filemode='w')
    # Creating an object
    logger = logging.getLogger()
    # Setting the threshold of logger to DEBUG
    logger.setLevel(logging.DEBUG)
    logging.info('Execution of pipeline function initiated')
    try:
        file_names = read_files_from_dir(dir)
        df_repair_order = parse_xml(file_names)
        df_repair_order = df_repair_order.drop_duplicates()
        windows_latest_event = window_by_datetime(df_repair_order, window)
        repair_order_list = process_to_RO(windows_latest_event)
        conn = connect_db(db_name)
        write_to_sqlite(repair_order_list, db_name,conn)
    except Exception as e:
        logging.error('Error occurred while pipeline function execution',e)
    logging.info('Execution of pipeline function completed')


pipeline('data-challenges-main/data/', 'D', 'repair_order.db')

# display the record inserted in the table
read_to_sqlite('repair_order.db')


