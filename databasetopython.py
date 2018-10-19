#Author: Manish Puri
# This is an extension of the previous file. This is how to import a record from an SQL database to Python

import psycopg2
from psycopg2 import Error

try:
    connection = psycopg2.connect(user="postgres",
                                  password="password123",
                                  host="127.0.0.1",
                                  port="5432",
                                  database="postgres")

    filename = "L_Bernard"
    
    postgres_select = """SELECT filepath FROM csvtable WHERE filename = '%s' """ % (filename)
 
    cursor = connection.cursor()
    cursor.execute(postgres_select)
   # use fetchall method to fetch all the rows from database table
    mobile_records = cursor.fetchall()
    print ("Displaying rows from mobile table using cursor.fetchall")
    for row in mobile_records:
        print (row)
        
except (Exception, psycopg2.DatabaseError) as error :
    print ("Error while connecting to PostgreSQL", error)
    
finally:
    #closing database connection.
    if(connection):
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")
