import pandas as pd
import pyodbc

def setup_cursors():
    database_aw = {'servername': 'localhost\SQLEXPRESS',
                   'database': 'AdventureWorks2019'}
    database_nw = {'servername': 'localhost\SQLEXPRESS',
                   'database': 'Northwind'}
    database_aenc = (r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
                     r"DBQ=C:\Users\bartv\OneDrive\Documenten\GitHub\United-Cheese\aenc.accdb")
    datawarehouse = {'servername': 'localhost\SQLEXPRESS',
                     'database': 'DATAWAREHOUSE'}
    
    connection_aw = pyodbc.connect('DRIVER={SQL Server};SERVER=' + database_aw['servername'] + ';DATABASE=' + database_aw['database'] + ';Trusted_Connection=yes')
    connection_nw = pyodbc.connect('DRIVER={SQL Server};SERVER=' + database_nw['servername'] + ';DATABASE=' + database_nw['database'] + ';Trusted_Connection=yes')
    connection_aenc = pyodbc.connect(database_aenc)
    connection_dw = pyodbc.connect('DRIVER={SQL Server};SERVER=' + datawarehouse['servername'] + ';DATABASE=' + datawarehouse['database'] + ';Trusted_Connection=yes')

    cursor_aw = connection_aw.cursor()
    cursor_nw = connection_nw.cursor()
    cursor_aenc = connection_aenc.cursor()
    export_cursor = connection_dw.cursor()

    return cursor_aw, cursor_nw, cursor_aenc, export_cursor

def get_data(cursor, name):
    cursor.execute(f"SELECT * FROM {name}")
    rows = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]
    rows_as_tuples = [tuple(row) for row in rows]
    data = pd.DataFrame(rows_as_tuples, columns=column_names)
    return data