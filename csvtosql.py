#Author: Manish Puri
# This is an example of updating an SQL database with records in a csv file.

import psycopg2
import csv
import os, sys
from pathlib import Path

path = 'C:\\Users\\theguest\\Downloads\\10-9-18v5\\10-9-18v5\\'
foodir = r"C:\\Users\theguest\\Downloads\\10-9-18v5\\10-9-18v5"

barlist = []
barlist2= []
barlist3=[]


for root, dirs, files in walk(foodir):
    counter=1
    for f in files:
        if splitext(f)[1].lower() == ".txt":
            barlist3.append(counter)
            barlist.append(join(root, f))
            barlist2.append(f.rsplit('.',1)[0])
            counter+=1

print(barlist)
print(barlist2)


try:
    with open(path+'test.csv', 'w') as fp:
        writer = csv.writer(fp, delimiter=',',lineterminator='\n')
        writer.writerows(zip(barlist3,barlist,barlist2))
    fp.close()
except:
    print("write unsuccessful")



try:
    
    connection = psycopg2.connect(user="postgres",
                                  password="xxxxxxx",
                                  host="xxx.xx.xx.x",
                                  port="xxxx",
                                  database="postgres")
  
    cursor = connection.cursor()

    #sql_insert_query = """ INSERT INTO csvtable (id, filename, filepath) VALUES (%s,%s,%s) """

    cursor.execute("DELETE FROM csvtable")
    
    with open ('C:/Users/theguest/Downloads/10-9-18v5/10-9-18v5/test.csv', 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            cursor.execute("INSERT INTO csvtable VALUES (%s, %s, %s)", row)
    
    connection.commit()
    print (cursor.rowcount, "Record inserted successfully into csv table")

except (Exception, psycopg2.DatabaseError) as error :
    print("Failed inserting record into csv table {}".format(error))
finally:
    #closing database connection.
    if(connection):
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")

