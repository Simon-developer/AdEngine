import mysql.connector
import datetime
import vk
import funcsCommon
import interface
from config import *
from funcsVideos import *
from funcs_analysis import *
from youtube_api import YouTubeDataAPI


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

if __name__ == "__main__":
    try:
        session = vk.Session(access_token=vkToken)
        vkApi = vk.API(session)
        print("\nSUCCESS---------------------------\n| Подключение к Vk API успешно!    |"
              "\nSUCCESS---------------------------")
    except:
        print("\nERROR---------------------------\n"
              "Ошибка входа в VK API, входные данные указаны неверно!"
              "\nERROR---------------------------")
        raise SystemExit
else:
    print("\nERROR---------------------------\n"
          "Ошибка входа в VK API, скрипт исполняется извне!"
          "\nERROR---------------------------")
    raise SystemExit


if_continue = True
while if_continue:
    i = str(input('\nЧтобы удалить канал, после его адреса напишите delete ("channel/rgkjHRFjcnF delete").'
                  '\nЧтобы посмотреть информацию в базе, введите db или database.'
                  '\nЧтобы добавить канал введите вдрес после http://youtube.com/.'
                  '\nЧтобы выйти введите q или quit: '))
    if i == 'q' or i == 'quit':
        print("Всего Доброго!")
        if_continue = False
        continue

    # Запуск функции интерфейса
    if i == "db" or i == "database":
        res = interface.interface()
        if res == 'quit':
            continue
    # Проверка, нужно ли удалять канал
    i = i.split()
    to_delete = False
    if len(i) > 1:
        to_delete = True
    # Добавление канала
    channelURL = "https://www.youtube.com/" + i[0]

    localisationCheck = 0.0
    apiUrl = funcsCommon.returnYTApiUrl(channelURL, "channel")
    data = funcsCommon.ytChannelStatsGet(apiUrl)
    if data == "ERROR":
        print("Ошибка! Вы ввели неверные символы!")
        print("----------------------------------\n")
        continue
    if data['pageInfo']['totalResults'] == 0:
        data = "wrong"
    if data == "wrong":
        print('--ERROR----ERROR----ERROR--')
        print('Канал с таким ID не найден')
        print('ID - ', channelURL)
        print('--ERROR----ERROR----ERROR--')
        continue

    if "customUrl" in data["items"][0]["snippet"]:
        customUrl = data["items"][0]["snippet"]["customUrl"]
    else:
        customUrl = ""
    print("\n----------------------------------")
    print("--------------Этап 1--------------")
    if customUrl:
        print("https://www.youtube.com/", customUrl, sep="")
    else:
        print(channelURL)
    channelId = data['items'][0]['id']

    #Удаление канала, если было выбрано пользователем
    if to_delete:
        funcsCommon.delete_channel_all(channelId)
        continue

    title = data["items"][0]["snippet"]["title"]
    print("Пытаюсь добавить - ", title)

    publishedAt = funcsCommon.dateToFormat(str(data["items"][0]["snippet"]["publishedAt"]))
    if "country" in data["items"][0]["snippet"]:
        country = data["items"][0]["snippet"]["country"]
    else:
        country = ""
    if country == "RU":
        localisationCheck += 1.0
    if "thumbnails" in data["items"][0]["snippet"] and "high" in data["items"][0]["snippet"]["thumbnails"]:
        logoUrl = data["items"][0]["snippet"]["thumbnails"]["high"]["url"]
    elif "thumbnails" in data["items"][0]["snippet"] and "default" in data["items"][0]["snippet"]["thumbnails"]:
        logoUrl = data["items"][0]["snippet"]["thumbnails"]["default"]["url"]
    else:
        logoUrl = ""
    if "relatedPlaylists" in data["items"][0]["contentDetails"]:
        if "likes" in data["items"][0]["contentDetails"]["relatedPlaylists"]:
            playlistLikes = data["items"][0]["contentDetails"]["relatedPlaylists"]["likes"]
        else:
            playlistLikes = ""
        if "uploads" in data["items"][0]["contentDetails"]["relatedPlaylists"]:
            playlistUploads = data["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
        else:
            playlistUploads = ""
    else:
        playlistLikes = ""
        playlistUploads = ""
    if "viewCount" in data["items"][0]["statistics"]:
        viewCount = data["items"][0]["statistics"]['viewCount']
    else:
        viewCount = 0
    if "commentCount" in data["items"][0]["statistics"]:
        commentCount = data["items"][0]["statistics"]['commentCount']
    else:
        commentCount = 0
    if "subscriberCount" in data["items"][0]["statistics"]:
        subscriberCount = data["items"][0]["statistics"]['subscriberCount']
    else:
        subscriberCount = 0
    hiddenSubscriberCount = data["items"][0]["statistics"]['hiddenSubscriberCount']
    if "videoCount" in data['items'][0]['statistics']:
        videoCount = data["items"][0]["statistics"]['videoCount']
    else:
        videoCount = 0
    privacyStatus = data["items"][0]["status"]['privacyStatus']
    if 'image' in data["items"][0]['brandingSettings'] and'bannerImageUrl' in data["items"][0]['brandingSettings']['image']:
        bannerImage = data["items"][0]['brandingSettings']['image']['bannerImageUrl']
    else:
        bannerImage = ""
    addDateTime = funcsCommon.dateToFormat(str(datetime.datetime.now()))
    lastEditTime = addDateTime
    localisationStatus = localisationCheck
    subscriberCount = int(subscriberCount)
    # Проверка канала на существование в базе
    query = 'SELECT title FROM channels WHERE channelId = %s'
    value = (channelId, )
    mycursor.execute(query, value)
    result = mycursor.fetchall()
    if len(result) != 0:
        print("Канал уже существует в базе данных - ", title)
        print("----------------------------------")
        continue
    else:
        if subscriberCount >= 5000000:
            rating = 10
        elif 3000000 <= subscriberCount < 5000000:
            rating = 9
        elif 2000000 <= subscriberCount < 3000000:
            rating = 8
        elif 1000000 <= subscriberCount < 2000000:
            rating = 7
        elif 750000 <= subscriberCount < 1000000:
            rating = 6
        elif 500000 <= subscriberCount < 750000:
            rating = 5
        elif 350000 <= subscriberCount < 500000:
            rating = 4
        elif 200000 <= subscriberCount < 350000:
            rating = 3
        elif 100000 <= subscriberCount < 200000:
            rating = 2
        else:
            rating = 1
        print("---Присвоенный рейтинг - ", rating)

        if "description" in data['items'][0]['snippet']:
            # Присваиваем описание переменной
            description = data["items"][0]["snippet"]["description"]
            # Проверка на кириллицу
            if funcsCommon.hasCyrillic(description):
                localisationCheck += 1.0
            # Поиск всех ссылок на группы ВК, профили ВК, Instagram, Email, Facebook
            foundDomains = funcsCommon.searchAllLinks(description, channelURL)
            # Если есть группы или страницы ВК, канал русский (подходит по локализации)
            if len(foundDomains['vkGroups']) != 0 or len(foundDomains['vkElse']) != 0:
                localisationCheck += 1.0
        else:
            #   Даже если описание пусто, вызываем поиск ссылок, он спарсит то, что есть
            # на страничке About
            description = ""
            foundDomains = funcsCommon.searchAllLinks(description, channelURL)

        relatedVkGroup = ""
        relatedInstagramPage = ""
        relatedPromotionPage = ""
        relatedOther = ""

        if foundDomains:
            # ВЫБИРАЕМ ЛУЧШУЮ ГРУППУ ДЛЯ АНАЛИЗА ВК
            foundDomains = funcsCommon.sortFoundDomains(foundDomains)

            relatedVkGroup = str(foundDomains['vkGroups'])
            relatedInstagramPage = str(foundDomains['inst'])
            relatedPromotionPage = str(foundDomains['emails'])
            relatedOther = str(foundDomains['vkElse'] + foundDomains['other'])

        if localisationCheck < 1:
            print("ВНИМАНИЕ: канал не подходит по региону/локализации (Страна: ", country,
                  ", кириллица в описании: ", funcsCommon.hasCyrillic(description), ")", sep="")

        if "showRelatedChannels" in data["items"][0]['brandingSettings']['channel']:
            if data["items"][0]['brandingSettings']['channel']['showRelatedChannels']:
                showRelatedChannels = 1
                if 'featuredChannelsUrls' in data['items'][0]['brandingSettings']['channel']:
                    featuredChannels = data['items'][0]['brandingSettings']['channel']['featuredChannelsUrls']
                    counter = 0
                    for i in featuredChannels:
                        query = "SELECT * FROM channels WHERE channelId = %s"
                        value = (i,)
                        mycursor.execute(query, value)
                        result = mycursor.fetchall()
                        if len(result) == 0:
                            query = "SELECT * FROM channelsToGo WHERE channelId = %s"
                            value = (i,)
                            mycursor.execute(query, value)
                            result = mycursor.fetchall()
                            if len(result) == 0:
                                query = "INSERT INTO channelsToGo (channelId, leadingChannelId, status) VALUES (%s, %s, %s)"
                                values = (i, channelId, "TO ADD")
                                mycursor.execute(query, values)
                                mydb.commit()
                                counter += 1
                    print("---Количество каналов, добавленных в 'To Go' список: ", counter)
                else:
                    featuredChannels = ""
                    print("---Рекомендуемые каналы отсутствуют")
            else:
                print("---Рекомендуемые каналы скрыты")
                showRelatedChannels = 0
                featuredChannels = ""
        else:
            print("---Рекомендуемые каналы скрыты")
            showRelatedChannels = 0
            featuredChannels = ""
        query = 'INSERT INTO channels (channelId, title, description, rating, relatedVkGroup, ' \
                'relatedInstagramPage, relatedPromotionPage, relatedOther, customUrl, publishedAt, country, ' \
                'logoUrl, playlistLikes, playlistUploads, viewCount, commentCount, subscriberCount, ' \
                'hiddenSubscriberCount, videoCount, privacyStatus, showRelatedChannels, ' \
                'bannerImage, addDateTime, localisationStatus, lastEditTime) ' \
                'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, ' \
                '%s, %s, %s, %s, %s, %s, %s, %s, %s)'
        values = (channelId, title, description, rating, relatedVkGroup, relatedInstagramPage, relatedPromotionPage,
                    relatedOther, customUrl, publishedAt, country, logoUrl, playlistLikes, playlistUploads, viewCount,
                    commentCount, subscriberCount, hiddenSubscriberCount, videoCount, privacyStatus, showRelatedChannels,
                    bannerImage, addDateTime, localisationStatus, lastEditTime)
        try:
            mycursor.execute(query, values)
            mydb.commit()
            print("Канал был добавлен - ", title)
            go_to_stage_two = True
        except mysql.connector.Error as err:
            print("ERROR - не удалось добавить канал!!!\n", err)
            raise SystemExit


    print("----------------------------------")
    '''
    STAGE 2
    STAGE 2 - сбор последних видео, опубликованных на канале
    STAGE 2
    '''
    # Проверка, можно ли продолжать, проходит только если канал успешно добавлен в БД
    try:
        go_to_stage_two
    except NameError:
        go_to_stage_two = False

    if go_to_stage_two:
        print('\n----------------------------------')
        print("--------------Этап 2--------------")
        r = collectAllVideosForThisChannel(channelId)
        if r:
            print("----------------------------------\n")
            go_to_stage_three = True
        else:
            print('>>>--->>>--->>> Переход к следующему каналу')
            continue

    else:
        print("Ошибка, невозможно продолжать действие программы!")
        raise SystemExit

    try:
        go_to_stage_three
    except NameError:
        go_to_stage_three = False

    if go_to_stage_three:
        print('\n----------------------------------')
        print('--------------Этап 3--------------')
        print('Промежуточный анализ канала...')
        r = first_channel_analysis(channelId)
        if r:
            print("Первичный анализ проведен успешно!")
            print("----------------------------------\n")
            go_to_stage_four = True
        else:
            print('>>>--->>>--->>> Переход к следующему каналу')
            continue
    else:
        print("Ошибка, невозможно продолжать действие программы!")
        raise SystemExit

