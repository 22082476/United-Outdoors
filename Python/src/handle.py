import pyodbc
import datetime
import decimal
import math
import pandas as pd
import re
from classes import DateTable, DateHandler

def setup_cursor(connection):
    connection = pyodbc.connect(connection)
    cursor = connection.cursor()

    return cursor

def get_data(cursor, name):
    cursor.execute(f"SELECT * FROM " + name)
    rows = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]
    rows_as_tuples = [tuple(row) for row in rows]
    data = pd.DataFrame(rows_as_tuples, columns=column_names)
    return data

def insert_data(cursor, destination_table: str, p_keys: list, data):
    # cursor: Je geeft de export cursor mee die naar de datawarehouse schrijft
    # destination_table: De tabel in je datawarehouse waar je naar gaat schrijven
    # p_keys: Je geeft de primary key(s) mee in een lijst
    # data: een pandas dataframe met al je data, de kolomnamen moeten 1:1 overeenkomen met de namen in je datawarehouse.
    columns_string, fill_string, column_types = get_column_data(cursor, destination_table)
    column_names = columns_string.split(', ')

    for i, r in data.iterrows():
        try:
            p_key_values = [r[p_key] for p_key in p_keys]
            existing_row = fetch_existing_row(cursor, destination_table, p_keys, p_key_values)
            new_values = [validate_data_type(r[col], col_type) for col, col_type in zip(column_names, column_types)]

            if existing_row is None or check_changes(existing_row, new_values, column_types):
                print(f"INSERT INTO {destination_table} ({columns_string}) VALUES ({fill_string})", tuple(new_values))
                cursor.execute(f"INSERT INTO {destination_table} ({columns_string}) VALUES ({fill_string})", tuple(new_values))
        #except Exception as e:
            #print(e)
        except pyodbc.IntegrityError as e:
            print(f"IntegrityError occurred: {e}")
        except pyodbc.OperationalError as e:
            print(f"OperationalError occurred: {e}")
        except pyodbc.DatabaseError as e:
            print(f"DatabaseError occurred: {e}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            raise

    cursor.commit()

def fetch_existing_row(cursor, table, p_keys, p_key_values):
    where_clause = " AND ".join([f"{x} = ?" for x in p_keys])
    cursor.execute(f"SELECT * FROM {table} WHERE {where_clause}", tuple(p_key_values))
    rows = cursor.fetchall()
    return rows[-1] if rows else None

def get_column_data(cursor, table):
    cursor.execute(f"SELECT TOP 1 * FROM {table}")
    data = cursor.description
    column_names = [x[0] for x in data][1:-1]
    column_types = [x[1] for x in data][1:-1]

    fill_string = ', '.join(['?' for _ in column_names])
    col_string = ', '.join(column_names)
    
    return col_string, fill_string, column_types

def check_changes(current, new, types):
    # De eerste en laatste waarde worden weggehaald want dit zijn de s_key en timestamp
    current = current[1:-1]
    current = [validate_data_type(current[x], types[x]) for x in range(len(current))]
    new = [validate_data_type(new[y], types[y]) for y in range(len(new))]
    if len(current) != len(new):
        raise Exception(F"De lengte van de opgegeven data en de data uit de datawarehouse komen niet overeen. Geef je per ongeluk de s_key of timestamp mee in je dataframe?")
    
    return any(not compare_rows(c, n) for c, n in zip(current, new))
    
def validate_data_type(value, data_type):
    try:
        if value is None or (isinstance(value, float) and math.isnan(value)) or (isinstance(value, pd._libs.tslibs.nattype.NaTType)):
            return None
        elif data_type == int:
            return int(value)
        elif data_type == decimal.Decimal:
            return float(value)
        elif data_type == datetime.datetime:
            if isinstance(value, str):
                try:
                    datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
                    return value
                except:
                    pass
            return value.strftime('%Y-%m-%d %H:%M:%S')
        elif data_type == bool:
            return bool(value)
        return str(value)
    except Exception as e:
        raise Exception(f"Incompatible data types: {value}, {data_type} - {e}")
    
def compare_rows(value1, value2):
    if value1 == value2:
        return True
    if value1 is None and value2 is None:
        return True
    return False

def house_number(address):
    try:
        pattern = r'\b(\d+)\b'
        matches = re.findall(pattern, address)
        
        if matches:
            return matches[0]
        else:
            return None
    except Exception as e:
        print(f"Error: {e}")
        return None
    
def date (date):
    date_list = date.split(' ')
    date = date_list[0].split('-')
    time = date_list[1].split(':')

    return DateTable(date[0], quarter(DateHandler().get_month_number(date[1])), DateHandler().get_month_number(date[1]), date[2], time[0], time[1], date)



def quarter (month):
    if month <= 3:
        return 1
    elif month <= 6:
        return 2
    elif month <= 9:
        return 3
    else:
        return 4

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