from mysql.connector import connect

def connect_to_database():
    # Connect to the MySQL database
    cnx = connect(
        host="localhost",
        user="root",
        password="pass,123",
        database="analysis"
    )

    # Create a cursor object to execute SQL queries
    cursor = cnx.cursor()

    return cursor, cnx

def execute_select_query(query):
    cursor, cnx = connect_to_database()
    cursor.execute(query)
    result = cursor.fetchall()
    columns = [description[0] for description in cursor.description]

    # Prepare a list of dictionaries
    output = []
    for row in result:
        row_dict = {}
        for i in range(len(columns)):
            row_dict[columns[i]] = row[i]
        output.append(row_dict)
    
    close_connection(cursor, cnx)
    return output

def execute_update_query(query):
    # Execute update query
    cursor, cnx = connect_to_database()
    try:
        _enable_safe_mode(cursor, cnx)
        cursor.execute(query)
        cnx.commit()
        _disable_safe_mode(cursor, cnx)
    except Exception as e:
        print("Error : ",e)
    close_connection(cursor, cnx)
    
def execute_insert_query(query):
    # Execute update query
    cursor, cnx = connect_to_database()

    try:
        cursor.execute(query)
        cnx.commit()
    except Exception as e:
        print("Error : ",e)
    close_connection(cursor, cnx)
    
def execute_delete_query(query):
    # Execute update query
    cursor, cnx = connect_to_database()
    _enable_safe_mode(cursor, cnx)
    cursor.execute(query)
    cnx.commit()
    _disable_safe_mode(cursor, cnx)
    close_connection(cursor, cnx)

def close_connection(cursor, cnx):
    # Close the cursor and connection
    cursor.close()
    cnx.close()

def _enable_safe_mode(cursor, cnx):
    # Enable safe mode to prevent SQL injection
    cursor.execute("SET SQL_SAFE_UPDATES = 1")
    cnx.commit()
    
def _disable_safe_mode(cursor, cnx):
    # Enable safe mode to prevent SQL injection
    cursor.execute("SET SQL_SAFE_UPDATES = 0")
    cnx.commit()