import funcsCommon
from config import *
import mysql.connector

try:
    mydb = mysql.connector.connect(
        host=host,
        user=user,
        passwd=password,
        database=database
    )
except:
    print("\nERROR---------------------------\n"
          "Ошибка входа в базу данных, проверьте состояние сервера или входные данные!"
          "\nERROR---------------------------")
    raise SystemExit


mycursor = mydb.cursor()
#mycursor.execute('ALTER TABLE channels ADD lastEditTime DATETIME')
#mycursor.execute('DELETE FROM channels')
#mycursor.execute('DELETE FROM channelsToGo')
#mycursor.execute('DELETE FROM videos_ids')
funcsCommon.delete_channel_all('UCSF6ewY9LS8GnbphwvnZpUg')

mycursor.execute('SELECT category_name FROM youtube_video_categories')
r = mycursor.fetchall()
print(r)



mydb.commit()