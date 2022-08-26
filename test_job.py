import csv
from logging import NullHandler
import sqlite3
from datetime import date, datetime
import time
from progress.bar import IncrementalBar
import logging

def benchmark(func):
    import time

    def wrapper(*args, **kwargs):
        start = time.time()
        return_value = func(*args, **kwargs)
        end = time.time()
        logging.info('execution time: {} seconds.'.format(end-start))
        return return_value
    return wrapper

class Database:
    conn = None
    cur = None

    def __init__(self):   
        self.conn = sqlite3.connect('calls-for-service.db')
        self.cur = self.conn.cursor()
        

    def clear_data(self):
        logging.info('clear data from DB')
        self.cur.execute("DELETE FROM Calls;")
        self.conn.commit()
        self.cur.execute("DELETE FROM Disposition;")
        self.conn.commit()
        self.cur.execute("DELETE FROM City;")
        self.conn.commit()
        self.cur.execute("DELETE FROM Address_type;")
        self.conn.commit()
               
    def create_table_calls(self):
        logging.info('create table Calls')
        # ['Crime Id', 'Original Crime Type Name', 'Report Date', 'Call Date', 'Offense Date', 'Call Time', 'Call Date Time', 'Disposition', 'Address', 'City', 'State', 'Agency Id', 'Address Type', 'Common Location']
        create_table_query = """CREATE TABLE IF NOT EXISTS Calls(
                id              INTEGER PRIMARY KEY,
                Crime_Id        INTEGER,
                Crime_Type      TEXT,
                Report_Date     DATE,
                Call_Date       DATE,
                Offense_Date    DATE,
                Call_Time       DATETIME,
                Call_Date_Time  DATETIME,
                Disposition     INTEGER,
                Address         TEXT,
                City            INTEGER,
                State           TEXT,
                Agency_Id       INTEGER,
                Address_Type    INTEGER,
                Common_Location TEXT
                )"""
        self.cur.execute(create_table_query)
        self.conn.commit()

    def create_table_disposition(self):
        logging.info('create table Disposition')
        create_table_query = """CREATE TABLE  IF NOT EXISTS Disposition(
                id              INTEGER PRIMARY KEY,
                Disposition            TEXT
                )"""
        self.cur.execute(create_table_query)
        self.conn.commit()

    def create_table_city(self):
        logging.info('create table City')
        create_table_query = """CREATE TABLE  IF NOT EXISTS City(
                id              INTEGER PRIMARY KEY,
                City            TEXT
                )"""
        self.cur.execute(create_table_query)
        self.conn.commit()

    def create_table_address_types(self):
        logging.info('create table Address_type')
        create_table_query = """CREATE TABLE IF NOT EXISTS Address_type(
                id              INTEGER PRIMARY KEY,
                Address_type    TEXT
                )"""
        self.cur.execute(create_table_query)
        self.conn.commit()

    def insert_to_table_disposition(self,Disposition):
            insert_table_query = """INSERT INTO Disposition (Disposition) VALUES (?);"""
            self.cur.execute(insert_table_query, (Disposition,))
            self.conn.commit()

    def insert_to_table_cities(self,City):
        insert_table_query = """INSERT INTO City (City) VALUES (?);"""
        self.cur.execute(insert_table_query, (City,))
        self.conn.commit()

    def insert_to_table_address_types(self,Address_type):
        insert_table_query = """INSERT INTO Address_type (Address_type) VALUES (?);"""
        self.cur.execute(insert_table_query, (Address_type,))
        self.conn.commit()

    def insert_to_table_calls(self,row):
        insert_table_query = """INSERT INTO Calls Values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"""
        self.cur.execute(insert_table_query, row)
        self.conn.commit()
# ----------------------------------------------------------------------------------------------------------------------------------
    @benchmark
    def select_from_table_calls(self, date_from, date_to, page=None): #поиск в таблице Calls по дате между date_from и date_to
        log_str = 'SEARCH CALL FROM ' + date_from + ' TO ' + date_to + '. PAGE '
        if page == None:
            log_str = log_str + 'NOT SET'
        elif page >= 0:
            log_str = log_str + str(page)
        else:
            page = 0
            log_str = log_str + str(page)
        logging.info(log_str)
        rez = {
            'total_records' : 0,
            'records' : []
        }
        try:
            select_from_calls = "SELECT * FROM Calls WHERE Report_Date >= date(?) AND Report_Date <= date(?) order by Report_Date;"
            self.cur.execute(select_from_calls,(date_from,date_to,))
            all_results = self.cur.fetchall()
        except:
            logging.info('ERROR SELECT FROM DB')
            rez['total_records'] = 0
            return None

        rez['total_records'] = len(all_results)  
        #заменим индексированные поля данными из связанных таблиц   
        cities = self.select_all_cities()
        address_types = self.select_all_address_types()
        dispositions = self.select_all_dispositions()

        all_results = list(all_results)
 
        for el in all_results:
            el = list(el)
            el[8] = dispositions[int(el[8])-1][1]
            el[10] = cities[int(el[10])-1][1]
            el[13] = address_types[int(el[13])-1][1]
            rez['records'].append(el)

        if page == None: #если page не задан, то отдаем все
            return(rez)
        else:
            page_col = int(rez['total_records']/20) #общее количество страниц
            if page > page_col:
                page = page_col
        first = page*20
        last = (page+1)*20
        rez['records'] = rez['records'][first:last:1]
        logging.info('FIND RECORDS: {}'.format(rez['total_records']))
        return(rez)
#-----------------------------------------------------------------------------------------------------------------------------------
    def select_all_cities(self):
        self.cur.execute("SELECT * FROM City;")
        all_results = self.cur.fetchall()
        return(all_results)

    def select_all_address_types(self):
        self.cur.execute("SELECT * FROM Address_type;")
        all_results = self.cur.fetchall()
        return(all_results)

    def select_all_dispositions(self):
        self.cur.execute("SELECT * FROM Disposition;")
        all_results = self.cur.fetchall()
        return(all_results)    

@benchmark
def search_for_mini_table(): #заполнение вспомогательных таблиц для нормализации БД
    logging.info('MAKE MINI TABLES')
    dispositions = []
    cities = []
    address_types = []

    with open('police-department-calls-for-service.csv', mode='r') as csv_file:
        csv_reader = csv.reader(csv_file)
        row_counter = 0
        for row in csv_reader:              #поиск всех уникальных значений
            row_counter = row_counter + 1  
            # if not row[1] in crime_types: #17000 уникальных значений
            #     crime_types.append(row[1])
            if not row[7] in dispositions:
                dispositions.append(row[7])
            if not row[9] in cities:
                cities.append(row[9]) 
            if not row[12] in address_types:
                address_types.append(row[12])      

    dispositions.remove(dispositions[0])
    cities.remove(cities[0])
    address_types.remove(address_types[0])

    bd = Database()
    bd.create_table_calls()
    bd.create_table_address_types()
    bd.create_table_city()
    bd.create_table_disposition()
    bd.clear_data()

   
    for disposition in dispositions:
        bd.insert_to_table_disposition(disposition)
    for city in cities:
        bd.insert_to_table_cities(city)
    for address_type in address_types:
        bd.insert_to_table_address_types(address_type)
    return row_counter #вернем общее количество строк в файле

@benchmark
def load_to_db(num=None): #можно прочитать не весь файл, а заданное количество записей. по умолчанию читает все
    rows = search_for_mini_table() #очистим БД и создадим вспомогательные таблицы для нормализации
    if num == None:
        num = rows
    logging.info('LOAD DATA FROM CSV. ROWS TO READ {}'.format(num))
    
    bd = Database()
    cities = bd.select_all_cities()
    address_types = bd.select_all_address_types()
    dispositions = bd.select_all_dispositions()

    bar = IncrementalBar('Processing', max=100)
    percent = int(num / 100)
    counter = 0
    with open('police-department-calls-for-service.csv', mode='r') as csv_file:
        csv_reader = csv.reader(csv_file)
        line_count = 0
        columns = []
        for row in csv_reader:
            counter = counter + 1
            if counter == percent:
                counter = 0
                bar.next()
            if line_count == 0:
                columns = row
            elif line_count == num:
                break
            else:
                for city in cities:
                    if city[1] == row[9]:
                        row[9] = city[0]
                for address_type in address_types:
                    if address_type[1] == row[12]:
                        row[12] = address_type[0]
                for disposition in dispositions:
                    if disposition[1] == row[7]:
                        row[7] = disposition[0]
                row.insert(0,line_count)
                bd.insert_to_table_calls(row)
            line_count = line_count + 1
        
    bar.finish()


def main():
    logging.basicConfig(
        filename = "test_job.log",
        level=logging.DEBUG,
        format = "%(asctime)s - %(message)s",
        datefmt='%H:%M:%S',
        )
    
    # load_to_db(10000) #загрузка 10000 записей из CSV, если БД пустая

    bd = Database() #подключение к БД
    
    records = bd.select_from_table_calls('2016-04-01','2016-04-03',2)
    if records['total_records'] > 0:
        for e in records["records"]:
            # print(e)
            logging.info('{}'.format(e))
    else:
        logging.info('CALLS NOT FOUND')

if __name__ == "__main__":
    main()
