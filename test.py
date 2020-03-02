import mysql.connector
import re
import json
import datetime
from config import *
import vk
from funcsCommon import returnId

mydb = mysql.connector.connect(
    host=host,
    user=user,
    passwd=password,
    database=database
)

mycursor = mydb.cursor()


def collect_users(vk_adress: str):
    session = vk.Session(access_token=vkToken)
    vk_api = vk.API(session)
    group_id = returnId(vk_adress)

    try:
        res = vk_api.groups.getMembers(group_id=group_id, count=1, v=vkApiVersion)
    except:
        try:
            res = vk_api.users.get(user_ids=group_id, v=vkApiVersion)
            res = vk_api.users.getFollowers(user_id=res[0]['id'], count=1, v=vkApiVersion)
        except:
            return False, 0
    # Собранное количество подписчиков
    vk_subscribers = res['count']
    # Сколько человек будет собираться за один запрос
    count = 1000
    # Какой сдвиг установлен
    # (зависит от нынешнего шага цикла)
    offset = 0
    # Репрезентативная выборка (20%)
    max_offset = vk_subscribers * 0.2
    # Количество итераций сбора даных
    times = 1
    # Ограничение количества пользователей на канал
    # (ед. - тысячи человек)
    limit_per_channel = 10
    # Количество собранных пользователей
    count_collected_users = 0

    if max_offset > limit_per_channel:
        times = max_offset // count
        if times > limit_per_channel:
            times = limit_per_channel
        if times < 1:
            times = 1

    times_counter = 0
    while times_counter < times:
        offset = count * times_counter
        times_counter += 1
        print("---------НАЧАТ НОВЫЙ ЗАХОД - НОМЕР", times_counter)
        try:
            res = vk_api.groups.getMembers(group_id=group_id, count=count, offset=offset,
                                           v=vkApiVersion)
        except:
            try:
                res = vk_api.users.get(user_ids=group_id, v=vkApiVersion)
                res = vk_api.users.getFollowers(user_id=res[0]['id'], count=count, offset=offset,
                                                v=vkApiVersion)
            except:
                return False, 0
        print("НАЙДЕНО НА ЭТОМ ЭТАПЕ -", len(res['items']))
        users_stack = ", ".join(str(e) for e in res['items'])
        all_users = vk_api.users.get(user_ids=users_stack, fields='photo_id, verified, sex, '
                                                                  'bdate, city, country, home_town, '
                                                                  'photo_max, domain, '
                                                                  'has_mobile, contacts, site, education, '
                                                                  'universities, schools, status, '
                                                                  'last_seen, followers_count, '
                                                                  'occupation, nickname, relatives, relation, '
                                                                  'personal, connections, exports, activities, '
                                                                  'interests, music, movies, tv, books, games, '
                                                                  'about, quotes,  can_see_all_posts, can_post, '
                                                                  'can_see_audio, can_write_private_message, '
                                                                  'can_send_friend_request, timezone, screen_name, '
                                                                  'maiden_name, career, military', v=vkApiVersion)

        # Следующие две строчки использовать для сбора постов и музыки из аккаунтов после
        # завершения цикла на 1000 участников, для этого создать еще таблицы с постами и музыкой
        ids_to_collect_posts = []
        ids_to_collect_music = []
        print("ПОЛУЧЕНО ДАННЫХ НА ЭТОМ ЭТАПЕ -", len(all_users))
        count_collected_inner = 0
        for users in all_users:

            # ОБРАБАТЫВАЕТ И СОХРАНЯЕТ ВСЕ ДАННЫЕ ПОЛЬЗОВАТЕЛЕЙ
            # ПОЛУЧЕННЫЕ ИЗ ГРУПП или СТРАНИЦ каналов
            # Подсказка по значениям -
            # https://vk.com/dev/objects/user_2

            if 'id' in users:
                user_id = users['id']
                mycursor.execute('SELECT user_first_name, user_last_name FROM vk_users WHERE user_id = %s', (user_id,))
                search_user = mycursor.fetchall()
                if len(search_user) > 0:
                    continue
            else:
                continue
            # ИМЯ
            if 'first_name' in users:
                user_first_name = str((users['first_name'][:28] + '..')) if len(users['first_name']) > 30 else users['first_name']
            else:
                user_first_name = ""
            # ФАМИЛИЯ
            if 'last_name' in users:
                user_last_name = str((users['last_name'][:28] + '..')) if len(users['last_name']) > 30 else users['last_name']
            else:
                user_last_name = ""
            # Закрыт ли аккаунт, можно ли увидеть если закрыт
            if 'is_closed' in users and 'can_access_closed' in users:
                if users['is_closed'] and not users['can_access_closed']:
                    continue

            # ПОЛ
            if 'sex' in users:
                user_sex = users['sex']
            else:
                user_sex = 0
            # Никнейм
            if 'nickname' in users:
                user_nickname = str((users['nickname'][:28] + '..')) if len(users['nickname']) > 30 else users['nickname']
            else:
                user_nickname = ""
            # Девичья фамилия
            if 'maiden_name' in users:
                user_maiden_name = str((users['maiden_name'][:28] + '..')) if len(users['maiden_name']) > 30 else users['maiden_name']
            else:
                user_maiden_name = ""
            # Короткое имя страницы
            if 'screen_name' in users:
                user_screen_name = str((users['screen_name'][:28] + '..')) if len(users['screen_name']) > 30 else users['screen_name']
            else:
                user_screen_name = ""
            # День рождения
            # Базовые значения ограничены снизу минимальным значением mYsql
            day = 1
            month = 1
            year = 1000
            if 'bdate' in users:
                ############
                # ПРОВЕРКА НА ФОРМАТ (д.м или д.м.гггг)
                user_birthday = users['bdate']
                bday_array = user_birthday.split(".")
                if len(bday_array) == 2:
                    day = int(bday_array[0])
                    month = int(bday_array[1])
                    year = 1000
                    user_birthday = datetime.date(year=year, month=month, day=day)
                elif len(bday_array) == 3:
                    day = int(bday_array[0])
                    month = int(bday_array[1])
                    year = int(bday_array[2])
                    user_birthday = datetime.date(year=year, month=month, day=day)
                else:
                    user_birthday = datetime.date(year=year, month=month, day=day)
            else:
                user_birthday = datetime.date(year=year, month=month, day=day)
            # ГОРОД
            if 'city' in users:
                if 'id' in users['city']:
                    user_city_id = users['city']['id']
                else:
                    user_city_id = 0
                if 'title' in users['city']:
                    user_city_name = (users['city']['title'] + "..") if len(users['city']['title']) > 50 else \
                    users['city']['title']
                else:
                    user_city_name = ""
            else:
                user_city_id = 0
                user_city_name = ""
            # СТРАНА
            if 'country' in users:
                if 'id' in users['country']:
                    user_country_id = users['country']['id']
                else:
                    user_country_id = 0
                if 'title' in users['country']:
                    user_country_name = (users['country']['title'] + "..") if len(users['country']['title']) > 50 else \
                    users['country']['title']
                else:
                    user_country_name = ""
            else:
                user_country_id = 0
                user_country_name = ""
            # Cсылка на фото
            if 'photo_max' in users:
                user_photo_link = users['photo_max']
            else:
                user_photo_link = ""
            # Известен ли номер телефона
            if 'has_mobile' in users:
                user_has_mobile = users['has_mobile']
            else:
                user_has_mobile = 0
                # Словно не известен номер

            # Можно ли видеть все посты из приложения
            # Если можно, необходимо добавить в список для сбора данных о постах ids_to_collect_posts
            if 'can_see_all_posts' in users:
                if users['can_see_all_posts']:
                    user_can_see_all_posts = 1
                    ids_to_collect_posts.append(user_id)
                else:
                    user_can_see_all_posts = 0
            else:
                user_can_see_all_posts = 0
                # Словно нельзя увидеть

            # Можно ли видеть всю музыку пользователя
            # Если можно, необходимо добавить в список для сбора данных о музыке ids_to_collect_music
            if 'can_see_audio' in users:
                if users['can_see_audio']:
                    user_can_see_audio = 0
                    ids_to_collect_music.append(user_id)
                else:
                    user_can_see_audio = 0
            else:
                user_can_see_audio = 0
                # Словно нельзя увидеть
            # Можно ли написать ЛС без дружбы
            if 'can_write_private_message' in users:
                user_can_write_private_message = users['can_write_private_message']
            else:
                user_can_write_private_message = 0

            # Можно ли написать ЛС без дружбы
            if 'can_send_friend_request' in users:
                user_can_send_friend_request = users['can_send_friend_request']
            else:
                user_can_send_friend_request = 0

            # Можно ли постить на странице без дружбы
            if 'can_post' in users:
                user_can_post = users['can_post']
            else:
                user_can_post = 0

            # Есть ли сайт у пользователя (могут быть ссылки на инстаграм, работу)
            if 'site' in users:
                user_site = str((users['site'][:98] + '..')) if len(users['site']) > 100 else users['site']
            else:
                user_site = ""

            # Статус пользователя
            if 'status' in users:
                user_status = str((users['status'][:248] + '..')) if len(users['status']) > 250 else users['status']
            else:
                user_status = ""

            # Последний раз заходил
            # Время - unixtime, неизвестно - 0
            # НЕ ДОБАВЛЯЕМ В БД, ИБО НЕ СМОЖЕМ ОБНОВЛЯТЬ ПОСТОЯННО,
            # УДАЛЯЕМ ПОЛЬЗОВАТЕЛЯ, ЕСЛИ НЕ ЗАХОДИЛ 2 МЕСЯЦА
            # Платформа - 1-7, неизвестно - 10
            if 'last_seen' in users:
                if 'time' in users['last_seen']:
                    user_last_seen_time = users['last_seen']['time']
                    # ПРОВЕРКА - ДАВНО ЛИ ЗАХОДИЛ
                    # ПРОВЕРКА - ДАВНО ЛИ ЗАХОДИЛ
                    # ПРОВЕРКА - ДАВНО ЛИ ЗАХОДИЛ
                    # ПРОВЕРКА - ДАВНО ЛИ ЗАХОДИЛ
                    # ПРОВЕРКА - ДАВНО ЛИ ЗАХОДИЛ
                    # ПРОВЕРКА - ДАВНО ЛИ ЗАХОДИЛ
                else:
                    user_last_seen_time = 0
                if 'platform' in users['last_seen']:
                    user_last_seen_platform = users['last_seen']['platform']
                else:
                    user_last_seen_platform = 0
            else:
                user_last_seen_time = 0
                user_last_seen_platform = 0

            # Верифицирован ли
            if 'verified' in users:
                user_verified = users['verified']
            else:
                user_verified = 0

            # Количество подписчиков
            if 'followers_count' in users:
                user_followers_count = users['followers_count']
            else:
                user_followers_count = 0

            # Занятость (тип, id (группы), название) - тип (work, school, university)
            if 'occupation' in users:
                if 'type' in users['occupation']:
                    user_occupation_type = users['occupation']['type']
                else:
                    user_occupation_type = ""
                if 'id' in users['occupation']:
                    user_occupation_id = users['occupation']['id']
                else:
                    user_occupation_id = 0
                if 'name' in users['occupation']:
                    user_occupation_name = str((users['occupation']['name'][:48] + '..')) if len(
                        users['occupation']['name']) > 50 else users['occupation']['name']
                else:
                    user_occupation_name = ""
            else:
                user_occupation_type = "none"
                user_occupation_id = 0
                user_occupation_name = ""

            # Родной Город
            if 'home_town' in users:
                user_home_town = (users['home_town'][:48] + "..") if len(users['home_town']) > 50 else users[
                    'home_town']
            else:
                user_home_town = user_city_name

            # семейное положение.
            # 10 если скрыто или неизвестно
            if 'relation' in users:
                user_relation = users['relation']
            else:
                user_relation = 0

            # Персональные предпочтения, 10 - отсутствует
            if 'personal' in users:
                if 'political' in users['personal']:
                    user_personal_political = users['personal']['political']  # 1-9
                else:
                    user_personal_political = 0
                if 'langs' in users['personal']:
                    user_personal_langs = ""
                    langs = users['personal']['langs']
                    for lang in langs:
                        user_personal_langs + str(lang)
                else:
                    user_personal_langs = 'Русский'
                if 'religion_id' in users['personal']:
                    user_personal_religion_id = users['personal']['religion_id']
                else:
                    user_personal_religion_id = 0
                if 'inspired_by' in users['personal']:
                    user_personal_inspired_by = str((users['personal']['inspired_by'][:248] + '..')) if len(
                        users['personal']['inspired_by']) > 250 else users['personal']['inspired_by']
                else:
                    user_personal_inspired_by = ""
                if 'people_main' in users['personal']:
                    user_personal_people_main = users['personal']['people_main']
                else:
                    user_personal_people_main = 0
                if 'life_main' in users['personal']:
                    user_personal_life_main = users['personal']['life_main']
                else:
                    user_personal_life_main = 0
                if 'smoking' in users['personal']:
                    user_personal_smoking = users['personal']['smoking']
                else:
                    user_personal_smoking = 0
                if 'alcohol' in users['personal']:
                    user_personal_alcohol = users['personal']['alcohol']
                else:
                    user_personal_alcohol = 0
            else:
                user_personal_political = 0
                user_personal_langs = 'Русский'
                user_personal_religion_id = 0
                user_personal_inspired_by = ""
                user_personal_people_main = 0
                user_personal_life_main = 0
                user_personal_smoking = 0
                user_personal_alcohol = 0

            if 'interests' in users:
                user_interests = str((users['interests'][:248] + '..')) if len(users['interests']) > 250 else users[
                    'interests']
            else:
                user_interests = ""

            if 'music' in users:
                user_music = str((users['music'][:248] + '..')) if len(users['music']) > 250 else users['music']
            else:
                user_music = ""

            if 'activities' in users:
                user_activities = str((users['activities'][:248] + '..')) if len(users['activities']) > 250 else users[
                    'activities']
            else:
                user_activities = ""

            if 'movies' in users:
                user_movies = str((users['movies'][:248] + '..')) if len(users['movies']) > 250 else users['movies']
            else:
                user_movies = ""

            if 'tv' in users:
                user_tv = str((users['tv'][:248] + '..')) if len(users['tv']) > 250 else users['tv']
            else:
                user_tv = ""

            if 'books' in users:
                user_books = str((users['books'][:248] + '..')) if len(users['books']) > 250 else users['books']
            else:
                user_books = ""

            if 'games' in users:
                user_games = str((users['games'][:248] + '..')) if len(users['games']) > 250 else users['games']
            else:
                user_games = ""

            if 'about' in users:
                user_about = str((users['about'][:248] + '..')) if len(users['about']) > 250 else users['about']
            else:
                user_about = ""

            if 'quotes' in users:
                user_quotes = str((users['quotes'][:248] + '..')) if len(users['quotes']) > 250 else users['quotes']
            else:
                user_quotes = ""

            if 'military' in users:
                if 'unit' in users['military']:
                    user_military_unit = users['military']['unit']
                else:
                    user_military_unit = ""
                if 'unit_id' in users['military']:
                    user_military_unit_id = users['military']['unit_id']
                else:
                    user_military_unit_id = 0
                if 'country_id' in users['military']:
                    user_military_country_id = users['military']['country_id']
                else:
                    user_military_country_id = 0
                if 'from' in users['military']:
                    user_military_from = users['military']['from']
                else:
                    user_military_from = 0
                if 'until' in users['military']:
                    user_military_until = users['military']['until']
                else:
                    user_military_until = 0
            else:
                user_military_unit = ""
                user_military_unit_id = 0
                user_military_country_id = 0
                user_military_from = 0
                user_military_until = 0
            values = (
            user_id, user_first_name, user_last_name, user_sex, user_nickname, user_maiden_name, user_screen_name,
            user_birthday, user_city_id, user_city_name, user_country_id, user_country_name, user_photo_link,
            user_has_mobile, user_can_see_all_posts, user_can_see_audio, user_can_write_private_message,
            user_can_send_friend_request, user_can_post, user_site, user_status, user_last_seen_platform,
            user_verified, user_followers_count, user_occupation_type, user_occupation_id, user_occupation_name,
            user_home_town, user_relation, user_personal_political, user_personal_langs,
            user_personal_religion_id,
            user_personal_inspired_by, user_personal_people_main, user_personal_life_main, user_personal_smoking,
            user_personal_alcohol, user_interests, user_music, user_activities, user_movies, user_tv, user_books,
            user_games, user_about, user_quotes, user_military_unit, user_military_unit_id,
            user_military_country_id,
            user_military_from, user_military_until)
            sql = 'INSERT INTO vk_users ' \
                  '(user_id, user_first_name, user_last_name, user_sex, user_nickname, ' \
                  'user_maiden_name, user_screen_name, user_birthday, user_city_id, user_city_name, user_country_id, ' \
                  'user_country_name, user_photo_link, user_has_mobile, user_can_see_all_posts, user_can_see_audio, ' \
                  'user_can_write_private_message, user_can_send_friend_request, user_can_post, user_site, ' \
                  'user_status, user_last_seen_platform, user_verified, user_followers_count, user_occupation_type, ' \
                  'user_occupation_id, user_occupation_name, user_home_town, user_relation, user_personal_political, ' \
                  'user_personal_langs, user_personal_religion_id, user_personal_inspired_by, user_personal_people_main, ' \
                  'user_personal_life_main, user_personal_smoking, user_personal_alcohol, user_interests, user_music, ' \
                  'user_activities, user_movies, user_tv, user_books, user_games, user_about, user_quotes, ' \
                  'user_military_unit, user_military_unit_id, user_military_country_id, ' \
                  'user_military_from, user_military_until) ' \
                  'VALUES ' \
                  '(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, ' \
                  '%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, ' \
                  '%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
            sql_group_subs = "INSERT INTO vk_page_subs (user_id, vk_page) VALUES (%s, %s)"
            values_group_subs = (user_id, vk_adress)
            try:
                mycursor.execute(sql, values)
                mydb.commit()
                mycursor.execute(sql_group_subs, values_group_subs)
                mydb.commit()
                count_collected_users += 1
                count_collected_inner += 1
            except mysql.connector.Error as err:
                print("ERROR - Ошибка базы данных!\n", err)
                raise SystemExit

            # Карьера - ОТДЕЛЬНАЯ ТАБЛИЦА
            if 'career' in users:
                for work in users['career']:
                    if 'group_id' in work:
                        user_career_group_id = work['group_id']
                    else:
                        user_career_group_id = 0
                    if 'company' in work:
                        user_career_company = str((work['company'][:48] + '..')) if len(work['company']) > 50 else work[
                            'company']
                    else:
                        user_career_company = ""
                    if 'country_id' in work:
                        user_career_country_id = work['country_id']
                    else:
                        user_career_country_id = 0
                    if 'city_id' in work:
                        user_career_city_id = work['city_id']
                    else:
                        user_career_city_id = 0
                    if 'city_name' in work:
                        user_career_city_name = work['city_name']
                    else:
                        user_career_city_name = ""
                    if 'from' in work:
                        user_career_from = work['from']
                    else:
                        user_career_from = 0
                    if 'until' in work:
                        user_career_until = work['until']
                    else:
                        user_career_until = 0
                    if 'position' in work:
                        user_career_position = (work['position'][:48] + '..') if len(work['position']) > 50 else work[
                            'position']
                    else:
                        user_career_position = ""
                    sql = "INSERT INTO vk_users_career " \
                          "(user_id, user_career_group_id, user_career_company, user_career_country_id, " \
                          "user_career_city_id, user_career_city_name, user_career_from, " \
                          "user_career_until, user_career_position) " \
                          "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                    values = (user_id, user_career_group_id, user_career_company,
                              user_career_country_id, user_career_city_id, user_career_city_name,
                              user_career_from, user_career_until, user_career_position)
                    try:
                        mycursor.execute(sql, values)
                        mydb.commit()
                    except mysql.connector.Error as err:
                        print("ERROR - Ошибка базы данных!\n", err)
                        raise SystemExit

            # ПОД УНИВЕРСИТЕТЫ ОТДЕЛЬНАЯ ТАБЛИЦА
            if 'universities' in users:
                for university in users['universities']:
                    if 'id' in university:
                        user_university_id = university['id']
                    else:
                        user_university_id = 0
                    if 'country' in university:
                        user_university_country_id = university['country']
                    else:
                        user_university_country_id = 0
                    if 'city' in university:
                        user_university_city_id = university['city']
                    else:
                        user_university_city_id = 0
                    if 'name' in university:
                        user_university_name = str((university['name'][:48] + '..')) if len(
                            university['name']) > 50 else university['name']
                    else:
                        user_university_name = ""

                    if 'faculty' in university:
                        user_university_faculty = university['faculty']
                    else:
                        user_university_faculty = 0

                    if 'faculty_name' in university:
                        user_university_faculty_name = str((university['faculty_name'][:118] + '..')) if len(
                            university['faculty_name']) > 120 else university['faculty_name']
                    else:
                        user_university_faculty_name = ""

                    if 'chair' in university:
                        user_university_chair = university['chair']
                    else:
                        user_university_chair = 0

                    if 'chair_name' in university:
                        user_university_chair_name = str((university['chair_name'][:118] + '..')) if len(
                            university['chair_name']) > 120 else university['chair_name']
                    else:
                        user_university_chair_name = ""

                    if 'graduation' in university:
                        user_university_graduation = university['graduation']
                    else:
                        user_university_graduation = 0
                    if 'education_form' in university:
                        user_university_education_form = university['education_form']
                    else:
                        user_university_education_form = ""

                    if 'education_status' in university:
                        user_university_education_status = university['education_status']
                    else:
                        user_university_education_status = ""
                    sql = "INSERT INTO vk_users_universities " \
                          "(user_id, user_university_id, user_university_name, user_university_country_id, " \
                          "user_university_city_id, user_university_faculty, user_university_faculty_name, " \
                          "user_university_chair, user_university_chair_name, user_university_graduation, " \
                          "user_university_education_form, user_university_education_status) " \
                          "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                    values = (
                        user_id, user_university_id, user_university_name, user_university_country_id,
                        user_university_city_id,
                        user_university_faculty, user_university_faculty_name, user_university_chair,
                        user_university_chair_name,
                        user_university_graduation, user_university_education_form, user_university_education_status)
                    try:
                        mycursor.execute(sql, values)
                        mydb.commit()
                    except mysql.connector.Error as err:
                        print("ERROR - Ошибка базы данных!\n", err)
                        raise SystemExit

            # ПОД ШКОЛЫ ОТДЕЛЬНАЯ ТАБЛИЦА
            if 'schools' in users:
                for school in users['schools']:
                    if 'id' in school:
                        user_school_id = school['id']
                    else:
                        user_school_id = 0
                    if 'country' in school:
                        user_school_country_id = school['country']
                    else:
                        user_school_country_id = 0
                    if 'city' in school:
                        user_school_city_id = school['city']
                    else:
                        user_school_city_id = 0

                    if 'name' in school:
                        user_school_name = (school['name'][:118] + "..") if len(school['name']) > 120 else school[
                            'name']
                    else:
                        user_school_name = ""
                    if 'year_from' in school:
                        user_school_year_from = school['year_from']
                    else:
                        user_school_year_from = 0
                    if 'year_to' in school:
                        user_school_year_to = school['year_to']
                    else:
                        user_school_year_to = 0
                    # КОДИРОВКА ТИПОВ ПО ПОДСКАЗКЕ ВНАЧАЛЕ
                    if 'type' in school:
                        user_school_type = school['type']
                    else:
                        user_school_type = 0
                    sql = "INSERT INTO vk_users_schools (user_id, user_school_id, user_school_name, " \
                          "user_school_country_id, user_school_city_id, user_school_year_from, " \
                          "user_school_year_to, user_school_type) " \
                          "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
                    values = (user_id, user_school_id, user_school_name, user_school_country_id,
                              user_school_city_id, user_school_year_from, user_school_year_to, user_school_type)
                    try:
                        mycursor.execute(sql, values)
                        mydb.commit()
                    except mysql.connector.Error as err:
                        print("ERROR - Ошибка базы данных!\n", err)
                        raise SystemExit

            # ПОД РОдственников ОТДЕЛЬНАЯ ТАБЛИЦА
            if 'relatives' in users:
                for relative in users['relatives']:
                    if 'id' in relative:
                        relative_id = relative['id']
                    else:
                        relative_id = 0
                    if 'type' in relative:
                        relative_type = relative['type']
                    else:
                        relative_type = ""
                    if 'name' in relative:
                        relative_name = relative['name']
                    else:
                        relative_name = ""
                    sql = "INSERT INTO vk_users_relatives " \
                          "(user_id, relative_id, relative_type, relative_name) " \
                          "VALUES (%s, %s, %s, %s)"
                    values = (user_id, relative_id, relative_type, relative_name)
                    try:
                        mycursor.execute(sql, values)
                        mydb.commit()
                    except mysql.connector.Error as err:
                        print("ERROR - Ошибка базы данных!\n", err)
                        raise SystemExit
        print("РЕАЛЬНО СОБРАНО НА ЭТАПЕ -", count_collected_inner)

    return True, count_collected_users


vk_group = 'https://vk.com/nationalpepper'
success, count_collected_users = collect_users(vk_group)
if success:
    print('Сбор Данных прошел успешно!')
    print('Кол-во собранных пользователей:', count_collected_users)
