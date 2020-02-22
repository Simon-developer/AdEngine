from config import *
import mysql.connector
import datetime
from funcsCommon import hasCyrillic, delete_channel_all


mydb = mysql.connector.connect(host=host,
                               user=user,
                               passwd=password,
                               database=database)
mycursor = mydb.cursor()


def first_channel_analysis(channel_id: str):
    print("Начинаем анализ канала...")
    try:
        mycursor.execute("SELECT * FROM channels WHERE channelId = %s", (channel_id, ))
        res = mycursor.fetchone()
    except mysql.connector.Error as err:
        print("Ошибка!")
        print(err)
        return False

    print("Имя канала -", res[2])
    print("Изначальный рейтинг -", res[4])
    age = int((datetime.date.today() - res[10].date()).total_seconds())
    age_years = int(age/60/60/24/365)
    age_months = int(age/60/60/24/30 - age_years*12)
    age_days = int(age/60/60/24 - age_months*30 - age_years*365)
    print('Канал активен(приблизительно): ', age_years, " года(лет), ", age_months, " месяцев(а), ", age_days, " дня(ей).", sep="")
    localization_status = res[24]
    print("Статус локализации - ", localization_status, sep="")

    if localization_status < 10:
        mycursor.execute("SELECT title, description FROM videos WHERE channel_id = %s", (channel_id,))
        fetch_title_description = mycursor.fetchall()

        for i in fetch_title_description:
            if hasCyrillic(i[0]) or hasCyrillic(i[1]):
                localization_status += 1

        values = (localization_status, channel_id)
        mycursor.execute('UPDATE channels SET localisationStatus = %s WHERE channelId = %s', values)
        mydb.commit()
    if localization_status < 5:
        delete_channel_all(channel_id)
    # Анализ частоты выкладки видео
    mycursor.execute('SELECT title, published_at FROM videos WHERE channel_id = %s', (channel_id,))
    fetch_all_videos_date = mycursor.fetchall()
    add_dates = []
    for i in fetch_all_videos_date:
        print(i[0])
        print(i[1])



    if(res[17] == 0):
        print("Количество подписчиков предположительно скрыто (либо равно 0)")
    else:
        print('Количество подписчиков -', res[17])