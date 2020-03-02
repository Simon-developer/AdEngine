import mysql.connector
from config import *
import datetime

mydb = mysql.connector.connect(
    host=host,
    user=user,
    passwd=password,
    database=database
)


mycursor = mydb.cursor()


vk_address = 'https://vk.com/nationalpepper'
ages = []

mycursor.execute("SELECT user_birthday, user_sex, user_followers_count FROM vk_users WHERE user_id in "
                 "(SELECT user_id FROM vk_page_subs WHERE vk_page = %s)", (vk_address,))
subscribers = mycursor.fetchall()

for subscriber in subscribers:
    if subscriber[0].year == 1000:
        continue
    else:
        ages.append(datetime.datetime.now().year - subscriber[0].year)

average_age = format(sum(ages)/len(ages), ".2f")
print("Средний возраст аудитории - ", average_age)
mydb.commit()