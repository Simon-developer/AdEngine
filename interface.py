import mysql.connector
from config import *
from funcsCommon import hasCyrillic, returnId
from funcs_analysis import first_channel_analysis
try:
    mydb = mysql.connector.connect(
        host=host,
        user=user,
        passwd=password,
        database=database
    )
    mycursor = mydb.cursor()
except:
    print("\nERROR---------------------------\n"
          "Ошибка входа в базу данных, проверьте состояние сервера или входные данные!"
          "\nERROR---------------------------")
    raise SystemExit

def interface():
    if_continue = True
    while if_continue:
        what_to_search = input("--------------------------------------\nКанал/Видео/ГруппаВК? (c/v/g): ")
        if what_to_search == "c":
            channel_name = input("Пожалуйста, введите название или адрес канала после 'tube.com/': ")
            sql = "SELECT channelId FROM channels WHERE title = %s OR title = %s OR channelId = %s"
            value = (channel_name, returnId(channel_name),returnId(channel_name))
            search_for_channel = mycursor.execute(sql, value)
            search_result = mycursor.fetchall()
            if len(search_result) == 0:
                print("Каналов не найдено!\nПродолжить (n/y)?")
                check = input()
                if check == "n":
                    return 'quit'
                else:
                    continue
            for channel in search_result:
                first_channel_analysis(channel[0])

        elif what_to_search == "v":
            print("Пожалуйста, введите название видео: ")
            video_name = input()
        elif what_to_search == "g":
            print("Пожалуйста, введите название группы: ")
            video_name = input()
        else:
            print("Извините, вы ввели неверную букву!\nПродолжить (n/y)?")
            check = input()

            if check == "n":
                return 'quit'
            else:
                continue

