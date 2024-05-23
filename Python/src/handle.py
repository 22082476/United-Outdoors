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

def handle_insert_single_pk(cursor, destination_table, p_key, data, p_key2 = None, p_key3 = None, p_key4 = None):
    # Samengestelde primary keys moet nog ingebouwd worden
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

def get_column_data(cursor, table):
    cursor.execute(f"SELECT * FROM {table}")
    data = cursor.description
    column_names = get_column_lists(0, data)
    column_types = get_column_lists(1, data)
    fill = ['?' for x in range(len(column_names))]

    fill_string = format_strings_list(fill)
    col_string = format_strings_list(column_names)
    
    print(column_types)
    return col_string, fill_string, column_types

def check_changes(current, new):
    # Heel lelijk maar werkt voor nu
    check = False
    current = list(current)
    current.pop(0)
    current.pop(-1)

    current = [validate_data_type(current[x], type(current[x])) for x in range(len(current))]
    new = [validate_data_type(new[x], type(new[x])) for x in range(len(new))]
    for x in range(len(current)):
        check = compare_rows(current[x], new[x])
        if check:
            break
    return check
    
def validate_data_type(value, data_type):
    # Heel lelijk maar werkt voor nu
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

def compare_rows(value1, value2):
    # Heel lelijk maar werkt voor nu
    if normalize_value(value1) == normalize_value(value2):
        return False
    elif value1 == None and value2 == None:
        return False
    elif normalize_value(value1) == True or normalize_value(value1) == False or normalize_value(value2) == False or normalize_value(value2) == True:
        if normalize_value(value1) == bool(value2):
            if value2 in [0, 1]:
                return False
        if normalize_value(value2) == bool(value1):
            if value1 in [0, 1]:
                return False
    elif bool(value1) == bool(value2):
        if value1 in [0, 1] and value2 in [0, 1]:
            return False
    return True

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