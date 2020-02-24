import mysql.connector
import re
import json
import funcsCommon
import numpy as np
from config import *
from youtube_api import YouTubeDataAPI
from datetime import datetime, timedelta
from tensorflow.keras.models import load_model
from nltk.tokenize import TweetTokenizer
from nltk.stem.snowball import RussianStemmer

mydb = mysql.connector.connect(
    host=host,
    user=user,
    passwd=password,
    database=database
)

mycursor = mydb.cursor()
yt = YouTubeDataAPI(googleApiKey)

'''
Константы и объекты для предсказаний по модели
Суть - предсказание тональной направленности комментариев
'''
vocab_size = 5000
vocab = []
token_to_idx = []
tokenizer = TweetTokenizer()
stemmer = RussianStemmer()
regex = re.compile('[^а-яА-Я ]')
stem_cache = {}
model = load_model("model/model.h5")

''' Загрузка словаря стем, на котором была обучена модель '''
with open('model/vocab.json') as json_file:
    data = json.load(json_file)
    for stem in data:
        vocab.append(stem)
#Загрузка словаря в переменную
token_to_idx = {vocab[i] : i for i in range(vocab_size)}
#Получение из слова стемы
def get_stem(token):
    stem = stem_cache.get(token, None)
    if stem:
        return stem
    token = regex.sub('', token).lower()
    stem = stemmer.stem(token)
    stem_cache[token] = stem
    return stem

#Функция преобразования комментария в вектор размерностью 5000 стем, для предсказаний по модели
def tweet_to_vector(tweet, show_unknowns=False):
    vector = np.zeros(vocab_size, dtype=np.int_)
    for token in tokenizer.tokenize(tweet):
        stem = get_stem(token)
        idx = token_to_idx.get(stem, None)
        if idx is not None:
            vector[idx] = 1
        elif show_unknowns:
            print("Неизвестное слово: ", token)
    return vector

'''
Сбор всех комментариев по id видео, вызывается из функции сбора информации о видео
Вход - id видео
Выход - количество добавленных комментариев
'''
def fetch_all_comments_from_video_by_id(video_id: str):
    comments = yt.get_video_comments(video_id=video_id,
                                     get_replies=False,
                                     max_results=100,
                                     next_page_token=False,
                                     part=['snippet'])
    counter = 0
    sentiment = 0

    for i in comments:
        comment_author = i['commenter_channel_id']
        text = i['text']

        #Определение тональности комментария
        #Преобразуем комментарий в векторное представление
        processed_comment = tweet_to_vector(text)
        #Изменяем размерность вектора чтобы поместился в модель
        processed_comment = processed_comment.reshape(1, 5000)
        #Предсказание
        prediction = model.predict(processed_comment)
        sentiment = float(prediction[0][0])

        comment_like_count = int(i['comment_like_count'])
        comment_publish_date = i['comment_publish_date']
        comment_publish_date = funcsCommon.dateToFormat(str(comment_publish_date))

        values = (video_id, comment_author, text, sentiment, comment_like_count, comment_publish_date)
        mycursor.execute('INSERT INTO comments (video_id, comment_author, text, sentiment, comment_like_count, comment_publish_date) '
                             'VALUES (%s, %s, %s, %s, %s, %s)', values)
        mydb.commit()
        counter += 1

    return counter

def get_all_info_from_videos(channel_id: str):
    query = "SELECT video_id FROM videos_ids WHERE channel_id = %s"
    value = (channel_id,)
    try:
        mycursor.execute(query, value)
    except mysql.connector.Error as err:
        print(err)
    result = mycursor.fetchall()
    counter_collected_video_info = 0
    if len(result) > 0:
        for i in result:
            r = check_if_video_exists_in_table(i[0], 'videos')
            if r:
                continue
            else:
                r = collect_video_info(i[0])
                if r:
                    counter_collected_video_info += 1
        if counter_collected_video_info > 0:
            print(f"Собрано подробной информации о \"{counter_collected_video_info}\" видео!")
            return True
        else:
            return False
    else:
        print('Это соообщение ты в принципе по всей логике не должен был увидеть,\nведь '
              'эта часть кода должна была вывестись только если программа решила,\nчто она добавила видео, '
              'а потом их же не нашла.\nСтранно все ёто...')
        return False


def check_if_video_exists_in_table(video_id: str, table: str):
    if table == 'videos_ids':
        query = "SELECT id FROM videos_ids WHERE video_id = %s"
    elif table == 'videos':
        query = "SELECT title FROM videos WHERE video_id = %s"
    else:
        # ERROR, точно
        query = "Nope."
    values = (video_id, )
    mycursor.execute(query, values)
    result = mycursor.fetchall()
    if len(result) > 0:
        return True
    else:
        return False


def updateChannelsLastEdit(channel_id: str):
    query = "UPDATE channels SET lastEditTime = %s WHERE channelId = %s"
    time = funcsCommon.dateToFormat(str(datetime.now()))
    values = (time, channel_id)
    try:
        mycursor.execute(query, values)
    except mysql.connector.Error as err:
        print(err)


def insertInVideosIds(channelId: str, videoId: str, addDate: datetime):
    query = "INSERT INTO videos_ids (channel_id, video_id, add_date) VALUES (%s, %s, %s)"
    values = (channelId, videoId, addDate)
    try:
        mycursor.execute(query, values)
        mydb.commit()
        return True
    except:
        return False


def collect_video_info(video_id: str):
    try:
        video = yt.get_video_metadata(video_id, parser=None, part=['contentDetails',
                                                                    'recordingDetails', 'status', 'snippet',
                                                                    'statistics',
                                                                    'topicDetails'])
    except:
        print("Не удалось получить данные о видео")
        return False

    channel_id = video['snippet']['channelId']
    video_id = video['id']
    published_at = funcsCommon.dateToFormat(video['snippet']['publishedAt'])
    title = video['snippet']['title']
    if 'description' in video['snippet']:
        description = video['snippet']['description']
    else:
        description = ""
    if 'url' in video['snippet']['thumbnails']['high']:
        image_url = video['snippet']['thumbnails']['high']['url']
    else:
        image_url = ""
    if 'liveBroadcastContent' in video['snippet']:
        live_broadcast = video['snippet']['liveBroadcastContent']
    else:
        live_broadcast = ""
    if 'categoryId' in video['snippet']:
        category_id = video['snippet']['categoryId']
    else:
        category_id = ""
    if 'defaultLanguage' in video['snippet']:
        default_language = video['snippet']['defaultLanguage']
    else:
        default_language = ""
    # Длительность преобразовывается в суммарные секунды, если видео длиннее дня, дни не учитываются
    if 'duration' in video['contentDetails']:
        duration = video['contentDetails']['duration']
        duration = funcsCommon.duration_decoder(duration)
    else:
        duration = ""
    if 'dimension' in video['contentDetails']:
        dimension = video['contentDetails']['dimension']
    else:
        dimension = ""
    if 'definition' in video['contentDetails']:
        definition = video['contentDetails']['definition']
    else:
        definition = ""
    if 'caption' in video['contentDetails']:
        caption = video['contentDetails']['caption']
    else:
        caption = False
    if caption == 'true':
        caption = True
    else:
        caption = False
    if 'licensedContent' in video['contentDetails']:
        licensed_content = video['contentDetails']['licensedContent']
    else:
        licensed_content = False
    if 'projection' in video['contentDetails']:
        projection = video['contentDetails']['projection']
    else:
        projection = ""
    if 'privacyStatus' in video['status']:
        privacyStatus = video['status']['privacyStatus']
    else:
        privacyStatus = ""
    if 'embeddable' in video['status']:
        embeddable = video['status']['embeddable']
    else:
        embeddable = False
    if 'viewCount' in video['statistics']:
        view_count = video['statistics']['viewCount']
    else:
        view_count = 0
    if 'likeCount' in video['statistics']:
        like_count = video['statistics']['likeCount']
    else:
        like_count = 0
    if 'dislikeCount' in video['statistics']:
        dislike_count = video['statistics']['dislikeCount']
    else:
        dislike_count = 0
    if 'favoriteCount' in video['statistics']:
        favorite_count = video['statistics']['favoriteCount']
    else:
        favorite_count = 0
    if 'commentCount' in video['statistics']:
        comment_count = video['statistics']['commentCount']
    else:
        comment_count = 0
    # Категория видео подбирается из ссылок на Википедии, возвращая английское название
    topic_categories = []
    if 'topicDetails' in video and 'topicCategories' in video['topicDetails']:
        for i in video['topicDetails']['topicCategories']:
            i = funcsCommon.returnId(i)
            category_name = i.replace("_", " ")
            topic_categories.append(category_name)
    query = "INSERT INTO videos (channel_id, video_id, published_at, title, description, image_url, " \
            "live_broadcast, category_id, default_language, duration, dimension, definition, caption, " \
            "licensed_content, projection, privacy_status, embeddable, view_count, like_count, dislike_count, " \
            "favorite_count, comment_count, topic_categories)" \
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    values = (channel_id, video_id, published_at, title, description, image_url, live_broadcast, category_id,
              default_language, duration, dimension, definition, caption, licensed_content, projection, privacyStatus,
              embeddable, view_count, like_count, dislike_count, favorite_count, comment_count, str(topic_categories))

    fetch_all_comments_from_video_by_id(video_id)

    try:
        mycursor.execute(query, values)
        mydb.commit()
        print("-->Видео (", title, ") добавлено!", sep="")
        return True
    except mysql.connector.Error as err:
        print(err)
        print("Ошибка! Не удалось добавить видео")
        return False


def collectAllVideosForThisChannel(channelId: str):
    query = 'SELECT title FROM channels WHERE channelId = %s'
    value = (channelId,)
    mycursor.execute(query, value)

    channelSearchRes = mycursor.fetchall()
    if len(channelSearchRes) == 1:
        print("Осуществляем поиск видео по каналу...")
        res = yt.search(channel_id=channelId, order_by='date', max_results=10)
        # Добавляем канал, только если на нем более 5 видео, если нет, удаляем из БД
        if len(res) > 5:
            r = []
            counter = 0
            counter_progress_bar = 0
            # Перебираем каждый найденный результат
            for video in res:
                counter += 1
                #counter_progress_bar += 1
                #funcsCommon.update_progress(counter_progress_bar / len(res)*2)
                # Проверяем, не существует ли уже такое видео
                # (на случай вызова функции из другого участка кода, когда уже добавлены видео на втором этапе)
                check_if_exists = check_if_video_exists_in_table(video['video_id'], 'videos_ids')
                if check_if_exists:
                    print(f'Ошибка, видео "{video["video_title"]}" уже было добавлено ранее')
                else:
                    # Добавляем видео в БД и присылаем в массив r True, если добавлено и False если не добавлено
                    r.append(insertInVideosIds(video['channel_id'], video['video_id'], video['video_publish_date']))
                # Как только закончились результаты, выбираем видео, что были раньше,
                # необходимо для максимума результатов - 100 видео
                if counter == 10:
                    delta = video['video_publish_date'] - timedelta(days=0.5)
                    res = yt.search(channel_id=channelId, order_by='date', published_before=delta, max_results=10)
                    for video2 in res:
                        #counter_progress_bar += 1
                        #funcsCommon.update_progress(counter_progress_bar / len(res)*2)
                        check_if_exists = check_if_video_exists_in_table(video2['video_id'], 'videos_ids')
                        if check_if_exists:
                            print(f'Ошибка, видео "{video2["video_title"]}" уже было добавлено ранее')
                        else:
                            r.append(insertInVideosIds(video2['channel_id'], video2['video_id'],
                                                       video2['video_publish_date']))

            print("Видео успешно добавлены (Кол-во: ", len(r), ")!", sep="")
            # Обновляем дату последних изменений
            updateChannelsLastEdit(channelId)
            # Собираем доступную информацию о всех видео и созраняем в базу
            print('---Пытаемся получить подробную информацию по каждому видео...')
            # Проверяем, есть ли вообще добавленные видео, чтобы понять,
            # можно ли переходить на следующий этап
            for i in r:
                # Если хоть один добавлен, то продолжаем
                if i:
                    final = True
            try:
                final
            except NameError:
                # Соответственно если не добавлен, возвращаем отказ от продолжения
                return False
            # Если же что-то было добавлено, до собираем подробную информацию по каждому добавленному видео, которые
            # есть в таблице videos_ids и нет в таблице videos
            if final:
                final2 = get_all_info_from_videos(channelId)
                if final2:
                    return True
                else:
                    return False
        else:
            print(f"На канале {channelSearchRes[0][0]} отсутсвуют видео (или их меньше 5),"
                  f"\nпроизводим удаление канала из базы данных...")
            funcsCommon.delete_channel_all(channelId)
        print("----------------------------------")
    else:
        print("\nERROR---------------------------\n"
              "Канала с таким Id не существует в базе данных,\n"
              "для поиска его видео сначала необходимо добавить канал!"
              "\nERROR---------------------------")
        raise SystemExit

# collectAllVideosForThisChannel("UCsAw3WynQJMm7tMy093y37A")
