import csv
from logging import NullHandler
import sqlite3
from datetime import date, datetime

class Database:
    conn = None
    cur = None

    def __init__(self):   
        self.conn = sqlite3.connect('calls-for-service.db')
        self.cur = self.conn.cursor()

    def create_table_calls(self):
        create_table_query = """CREATE TABLE IF NOT EXISTS Calls(
                Call Date Time     DATETIME 
                )
        """
        self.cur.execute(create_table_query)
        self.conn.commit()

    def insert_to_table_calls(self):
        insert_table_query = """INSERT INTO Calls Values(?);"""
        self.cur.execute(insert_table_query, (datetime.now(),))
        self.conn.commit()

    def select_from_table_calls(self):
        self.cur.execute("SELECT * FROM Calls WHERE datereg >= date('2022-08-24') AND datereg <= date('2022-08-25') order by datereg;")
        all_results = self.cur.fetchall()
        return(all_results)

with open('police-department-calls-for-service.csv', mode='r') as csv_file:
    # csv_reader = csv.reader(csv_file, delimiter=',')
    
    csv_reader = csv.reader(csv_file)
    line_count = 0
    columns = []
    for row in csv_reader:
        if line_count == 0:
            columns = row
        # if line_count == 1:
        #     print(row)
        if line_count == 2:
            # print(row)
            break
        line_count = line_count + 1
    
    print(columns)
    
    print(f'количество колонок {len(columns)}')
    print(f'количество строк {line_count}')

bd = Database()
bd.create_table_calls()
bd.insert_to_table_calls()
rez = bd.select_from_table_calls()
print(rez)

# ['Crime Id', 'Original Crime Type Name', 'Report Date', 'Call Date', 'Offense Date', 'Call Time', 'Call Date Time', 'Disposition', 'Address', 'City', 'State', 'Agency Id', 'Address Type', 'Common Location']