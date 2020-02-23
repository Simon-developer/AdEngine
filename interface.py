import mysql.connector
from config import *
from funcsCommon import hasCyrillic, returnId
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
        print("--------------------------------------\nКанал/Видео/ГруппаВК? (c/v/g): ")
        what_to_search = input()
        if what_to_search == "c":
            print("Пожалуйста, введите название или адрес канала после 'tube.com/': ")
            channel_name = input()
            if hasCyrillic(channel_name):
                sql = "SELECT * FROM channels WHERE title = %s"
                value = (channel_name,)
            else:
                sql = "SELECT * FROM channels WHERE id = %s"
                value = (returnId(channel_name), )

            search_for_channel = mycursor.execute(sql, value)
            search_result = mycursor.fetchall()
            if len(search_result) == 0:
                print("Каналов не найдено!\nПродолжить (n/y)?")
                check = input()
                if check == "n":
                    quit()
                else:
                    continue
            for channel in search_result:
                print('Канал ', channel_name, " найден.")
                print('Кол-во подписчиков - ', channel[17], ".")
                print('Дата создания ', channel[10], ".")
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
                quit()

