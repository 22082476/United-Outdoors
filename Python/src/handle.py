import pyodbc

def handleOne (DB, source, method):
    export_conn = pyodbc.connect('DRIVER={SQL server};SERVER=' + DB['servername'] + ';DATABASE=' + DB['database'] + ';Trusted_Connection=yes')
    export_cursor = export_conn.cursor()
    export_cursor
    
    for index, row in source.iterrows():

        try :
            vlaai = method(index, row)
            export_cursor.execute(vlaai)
        except pyodbc.Error:
            print(vlaai)


    export_conn.commit()  
    export_cursor.close() 

def handleTwo (DB, source1, source2, method):
    export_conn = pyodbc.connect('DRIVER={SQL server};SERVER=' + DB['servername'] + ';DATABASE=' + DB['database'] + ';Trusted_Connection=yes')
    export_cursor = export_conn.cursor()
    export_cursor

    for index1, row1 in source1.iterrows():

        for index, row in source2.iterrows():

            try :
                vlaai = method(index1, index, row1, row)
                export_cursor.execute(vlaai)
            except pyodbc.Error:
                print(vlaai)    


    export_conn.commit()  
    export_cursor.close()

def read(DB, table, where):
    export_conn = pyodbc.connect('DRIVER={SQL server};SERVER=' + DB['servername'] + ';DATABASE=' + DB['database'] + ';Trusted_Connection=yes')
    export_cursor = export_conn.cursor()

    try:
        query = f"SELECT MAX(S_KEY) FROM {table} WHERE {where}"
        export_cursor.execute(query)
        result = export_cursor.fetchall()
    except pyodbc.Error:
        print(query)
        result = None

    export_cursor.close()
    export_conn.commit()
    export_conn.close()

    return result[0][0]