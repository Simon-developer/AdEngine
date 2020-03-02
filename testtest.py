import mysql.connector
from config import *


mydb = mysql.connector.connect(
    host=host,
    user=user,
    passwd=password,
    database=database
)

mycursor = mydb.cursor()

mycursor.execute('DELETE FROM vk_users')
mycursor.execute('DELETE FROM vk_users_career')
mycursor.execute('DELETE FROM vk_users_universities')
mycursor.execute('DELETE FROM vk_users_schools')
mycursor.execute('DELETE FROM vk_users_relatives')

mydb.commit()