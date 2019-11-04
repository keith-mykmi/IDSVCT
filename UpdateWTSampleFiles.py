#!/usr/bin/env python3

import pandas as pd
import os
import copy
import pyodbc

pd.options.mode.chained_assignment = None  # default='warn'

def update(sqlFile, outputFile, updateName):
    print('Updating data: ',updateName)
    query = open(sqlFile, 'r') 
    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};'
                      'Server=US1153APP200.dir.slb.com;'
                      'Database=Welltrak_Data;'
                      'Trusted_Connection=yes;')

    print('SQL connection started - this may take some time')       
    cursor = conn.cursor()
    cursor.execute(query.read())

    col_headers = [ i[0] for i in cursor.description ]
    rows = [ list(i) for i in cursor.fetchall()] 
    df = pd.DataFrame(rows, columns=col_headers)

    print('Writing File')      
    df.to_csv(outputFile, index=False)
    print('Update Complete: ',updateName)

update(r'SQL\WT OperationalTime BD 2018 2019.sql',r'Input\WTExport20182019.csv','Operational Time')
update(r'SQL\WT Mobilisation BD 2018 2019.sql',r'Input\WTMobilisation20182019.csv','Mobilisation Data')
update(r'SQL\WT ROPO 20182019.sql',r'Input\WTRopo.csv','ROPO')
print('All Updates Complete')