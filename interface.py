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
        what_to_search = input("--------------------------------------\n"
                               "Чтобы Посмотреть все таблицы, введите table\n"
                               "Чтобы узнать количество записей в таблице, введите название show 'название таблицы'\n"
                               "Канал/Видео/ГруппаВК? (c/v/g): ")
        command = what_to_search.split()
        if command[0] == "show":
            try:
                mycursor.execute('DESC {table}'.format(table=command[1]))
                tables = mycursor.fetchall()
                print("Столбцов -", len(tables))
                print("%s, "*len(tables))
                for table in tables:
                    print(table[0], end=", ", sep="")
            except mysql.connector.Error as err:
                print("ERROR - MYSQL!\n", err)
                raise SystemExit

        elif what_to_search == "table":
            mycursor.execute('SHOW TABLES')
            tables = mycursor.fetchall()
            for table in tables:
                print('----',table[0])
        elif what_to_search == "c":
            channel_name = input("Пожалуйста, введите название или адрес канала после 'tube.com/': ")
            sql = "SELECT channelId FROM channels WHERE title = %s OR title = %s OR channelId = %s"
            value = (channel_name, returnId(channel_name), returnId(channel_name))
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
