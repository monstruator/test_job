import csv
from logging import NullHandler
import sqlite3
from datetime import date, datetime
import time
from progress.bar import IncrementalBar


def benchmark(func):
    import time

    def wrapper(*args, **kwargs):
        start = time.time()
        return_value = func(*args, **kwargs)
        end = time.time()
        print('[*] Время выполнения: {} секунд.'.format(end-start))
        return return_value
    return wrapper

def search_all_crime_type(): #17084 варианта
    crime_type = []
    with open('police-department-calls-for-service.csv', mode='r') as csv_file:
        csv_reader = csv.reader(csv_file)
        for row in csv_reader:
            if not row[1] in crime_type:
                crime_type.append(row[1])
    crime_type.remove(crime_type[0])
    return crime_type


def search_all_disposition():
    disposition = []
    with open('police-department-calls-for-service.csv', mode='r') as csv_file:
        csv_reader = csv.reader(csv_file)
        for row in csv_reader:
            if not row[7] in disposition:
                disposition.append(row[7])
                print(row[7])
    disposition.remove(disposition[0])
    return disposition

def fill_table_disposition():
    bd = Database()
    bd.create_table_disposition()
    dispositions = search_all_disposition()
    print(f'количество dispositions {len(dispositions)}')
    print(dispositions)
    for disposition in dispositions:
        bd.insert_to_table_disposition(disposition)

def search_all_cities():
    cities = []
    with open('police-department-calls-for-service.csv', mode='r') as csv_file:
        csv_reader = csv.reader(csv_file)
        for row in csv_reader:
            if not row[9] in cities:
                cities.append(row[9])

    cities.remove(cities[0])
    return cities

def fill_table_city():
    bd = Database()
    bd.create_table_city()
    cities = search_all_cities()
    print(f'количество cities {len(cities)}')
    print(cities)
    for city in cities:
        bd.insert_to_table_city(city)

def search_all_address_types():
    address_types = []
    with open('police-department-calls-for-service.csv', mode='r') as csv_file:
        csv_reader = csv.reader(csv_file)
        for row in csv_reader:
            if not row[12] in address_types:
                address_types.append(row[12])

    address_types.remove(address_types[0])
    return address_types

def fill_table_address_types():
    bd = Database()
    bd.create_table_address_types()
    address_types = search_all_address_types()
    print(f'количество address_types {len(address_types)}')
    print(address_types)
    for address_type in address_types:
        bd.insert_to_table_address_types(address_type)   

class Database:
    conn = None
    cur = None

    def __init__(self):   
        self.conn = sqlite3.connect('calls-for-service.db')
        self.cur = self.conn.cursor()
        self.create_table_calls()

    def create_table_calls(self):
        # id              INTEGER PRIMARY KEY,
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
        create_table_query = """CREATE TABLE IF NOT EXISTS Disposition(
                id              INTEGER PRIMARY KEY,
                Disposition            TEXT
                )"""
        self.cur.execute(create_table_query)
        self.conn.commit()

    def create_table_city(self):
        create_table_query = """CREATE TABLE IF NOT EXISTS City(
                id              INTEGER PRIMARY KEY,
                City            TEXT
                )"""
        self.cur.execute(create_table_query)
        self.conn.commit()

    def create_table_address_types(self):
        create_table_query = """CREATE TABLE IF NOT EXISTS Address_type(
                id              INTEGER PRIMARY KEY,
                Address_type     TEXT
                )"""
        self.cur.execute(create_table_query)
        self.conn.commit()

    def insert_to_table_disposition(self,Disposition):
            insert_table_query = """INSERT INTO Disposition (Disposition) VALUES (?);"""
            self.cur.execute(insert_table_query, (Disposition,))
            self.conn.commit()

    def insert_to_table_city(self,City):
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
    def select_from_table_calls(self, date_from, date_to, page=None):
        rez = []
        select_from_calls = "SELECT * FROM Calls WHERE Report_Date >= date(?) AND Report_Date <= date(?) order by Report_Date;"
        self.cur.execute(select_from_calls,(date_from,date_to,))
        all_results = self.cur.fetchall()

        cities = self.select_all_cities()
        address_types = self.select_all_address_types()
        dispositions = self.select_all_dispositions()

        all_results = list(all_results)
 
        for el in all_results:
            el = list(el)
            el[8] = dispositions[int(el[8])-1][1]
            el[10] = cities[int(el[10])-1][1]
            el[13] = address_types[int(el[13])-1][1]
            rez.append(el)

        if page == None:
            return(rez)
        else:
            page_col = int(len(rez)/20)
            if page > page_col:
                page = page_col
        first = page*20
        last = (page+1)*20
        rez = rez[first:last:1]
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
def load_to_db(num=None):
    bd = Database()
    cities = bd.select_all_cities()
    address_types = bd.select_all_address_types()
    dispositions = bd.select_all_dispositions()
    with open('police-department-calls-for-service.csv', mode='r') as csv_file:
        csv_reader = csv.reader(csv_file)
        line_count = 0
        columns = []
        for row in csv_reader:
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
        
        print(f'количество колонок {len(columns)}')
        print(f'количество строк {line_count}')




# bd = Database()
# records = bd.select_from_table_calls('2016-04-01','2016-04-03',0)
# for e in records:
#     print(e)

# load_to_db(1000)
# fill_table_address_types()

# fill_table_disposition()


