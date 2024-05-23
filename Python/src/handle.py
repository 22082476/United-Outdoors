import pyodbc
import datetime
import decimal
import os
import math
from formatting import get_column_lists, format_strings_list, normalize_value
from dotenv import load_dotenv
load_dotenv('.env')
import pandas as pd

def setup_cursor(connection):
    connection = pyodbc.connect(connection)

    cursor = connection.cursor()

    return cursor

def handleOne (cursor, source, method):   
    for index, row in source.iterrows():
        try :
            query = method(index, row)
            cursor.execute(query)
        except pyodbc.Error:
            print(f"Error executing: {query}")    

    cursor.commit()  
    cursor.close() 

def handleTwo (cursor, source1, source2, method):
    for index1, row1 in source1.iterrows():
        for index, row in source2.iterrows():
            try :
                query = method(index1, index, row1, row)
                cursor.execute(query)
            except pyodbc.Error:
                print(f"Error executing: {query}")    

    cursor.commit()
    cursor.close()

def handle_insert_single_pk(cursor, destination_table, p_key, data):
    #destination_table = "order_temp"
    #cursor = setup_cursor(os.getenv('datawharehouse'))
    columns_string, fill_string, column_types = get_column_data(cursor, destination_table)
    column_names = columns_string.split(', ')
    
    for i, r in data.iterrows():
        try:
            cursor.execute(f"SELECT * FROM {destination_table} WHERE {p_key}=?", (r[p_key]))
            all_rows = cursor.fetchall()
            if all_rows:
                existing_row = all_rows[-1]
            else:
                existing_row = None

            if existing_row == None:
                values = [validate_data_type(r[column_names[x]], column_types[x]) for x in range(len(column_names))]
                cursor.execute(f"INSERT INTO {destination_table} ({columns_string}) VALUES ({fill_string})", tuple(values))
            
            elif check_changes(existing_row, [r[column_names[x]] for x in range(len(column_names))]):
                values = [validate_data_type(r[column_names[x]], column_types[x]) for x in range(len(column_names))]
                cursor.execute(f"INSERT INTO {destination_table} ({columns_string}) VALUES ({fill_string})", tuple(values))
        except Exception as e:
            print(e)

    cursor.commit()
    
def validate_data_type(value, data_type):
    try:
        if value is None or (isinstance(value, float) and math.isnan(value)) or (isinstance(value, pd._libs.tslibs.nattype.NaTType)):
            return None
        elif data_type == int:
            return int(value)
        elif data_type == decimal.Decimal:
            return float(value)
        elif data_type == datetime.datetime:
            return value.strftime('%Y-%m-%d %H:%M:%S')
        else:
            return str(value)
    except Exception as e:
        raise Exception(f"Incompatible data types: {value}, {data_type} - {e}")
    
def check_changes(current, new):
    current = list(current)
    current.pop(0)
    current.pop(-1)

    current = [validate_data_type(current[x], type(current[x])) for x in range(len(current))]
    new = [validate_data_type(new[x], type(new[x])) for x in range(len(new))]
    check = compare_rows(current, new)
    if check:
        print('helaas')
        print(current)
        print(new)
    return check

def compare_rows(row1, row2):
    if len(row1) != len(row2):
        return False
    for val1, val2 in zip(row1, row2):
        if normalize_value(val1) != normalize_value(val2):
            return False
    return True

def get_column_data(cursor, table):
    cursor.execute(f"SELECT * FROM {table}")
    data = cursor.description
    column_names = get_column_lists(0, data)
    column_types = get_column_lists(1, data)
    fill = ['?' for x in range(len(column_names))]

    fill_string = format_strings_list(fill)
    col_string = format_strings_list(column_names)
    
    return col_string, fill_string, column_types

def read(cursor, table, where):
    try:
        query = f"SELECT MAX(S_KEY) FROM {0} WHERE {1}", table , where
        cursor.execute(query)
        result = cursor.fetchall()
        result = result[0][0]
    except pyodbc.Error:
        print(query)  
        result = None

    cursor.commit()
    cursor.close()

    return result

def get_data(cursor, name):
    cursor.execute(f"SELECT * FROM " + name)
    rows = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]
    rows_as_tuples = [tuple(row) for row in rows]
    data = pd.DataFrame(rows_as_tuples, columns=column_names)
    return data