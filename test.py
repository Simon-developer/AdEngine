import mysql.connector
import re
import json
from config import *


mydb = mysql.connector.connect(
    host=host,
    user=user,
    passwd=password,
    database=database
)

mycursor = mydb.cursor()
max_results = 3

mycursor.execute('SELECT title FROM channels')
res = mycursor.fetchall()
for i in res:
    print('-',i [0])

mydb.commit()