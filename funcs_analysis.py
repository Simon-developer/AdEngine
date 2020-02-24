from config import *
import mysql.connector
import datetime
from funcsCommon import hasCyrillic, delete_channel_all

mydb = mysql.connector.connect(host=host,
                               user=user,
                               passwd=password,
                               database=database)
mycursor = mydb.cursor()


def video_sentiment_analysis(channel_id: str):
    # Возвращает 3 переменных:
    # Среднюю оценку тональности отзывов - усредненная оценка среди всех видео
    # Максимальную положительную оценку тональности в формате {'название видео', 'id видео', оценка}
    # Минимальную положительную оценку тональности в формате {'название видео', 'id видео', оценка}

    mycursor.execute('SELECT title, video_id FROM videos WHERE channel_id = %s', (channel_id,))
    fetched_videos = mycursor.fetchall()

    # Объявляем список для комплексного показателя оценок {'Название', 'id видео', оценка}
    videos_sentiment = []
    # Для каждого видео собираем все комментарии (а именно их оценку тональности)
    for video in fetched_videos:
        mycursor.execute('SELECT sentiment FROM comments WHERE video_id = %s', (video[1],))
        fetched_comments = mycursor.fetchall()

        """
        Добавляем переменную для комплексной оценки видео
        Базовые значения 0 и 1 нужны на случай отсутствия комментариев,
        тогда оценка будет 0,5 и ни на что не повлияет
        """
        complex_sentiment = [0.0, 1.0]
        for comment in fetched_comments:
            # Добавить оценку отдельного комментария к complex_sentiment
            comment_sentiment = comment[0]
            complex_sentiment.append(comment_sentiment)

        # Средняя оценка для видео
        average_video_sentiment = (sum(complex_sentiment) / len(complex_sentiment))
        # Обновляем эту оценку для видео в БД
        mycursor.execute('UPDATE videos SET sentiment = %s WHERE video_id = %s', (average_video_sentiment, video[1]))
        mydb.commit()

        videos_sentiment.append([video[0], video[1], average_video_sentiment])

    # Сортируем многомерный список с видео по параметру "sentiment"
    videos_sentiment.sort(key=lambda i: i[2])

    maximum_sentiment_score = videos_sentiment[-1]
    minimum_sentiment_score = videos_sentiment[0]
    average_video_sentiment = (maximum_sentiment_score[2] + minimum_sentiment_score[2]) / 2

    return average_video_sentiment, maximum_sentiment_score, minimum_sentiment_score


def engagement_rate_analysis(channel_id: str, hidden_subscribers: bool):
    """
    Возвращает аналогично функции video_sentiment_analysis, 3 переменных:
    средний индекс вовлеченности,
    максимальный {'имя видео', 'id видео', ER видео},
    минимальный {'имя видео', 'id видео', ER видео}
    """
    mycursor.execute('SELECT subscriberCount FROM channels WHERE channelId = %s', (channel_id,))
    subscribers_count = mycursor.fetchone()
    subscribers_count = subscribers_count[0]

    mycursor.execute('SELECT '
                     'title, '
                     'video_id, '
                     'like_count, '
                     'dislike_count, '
                     'view_count, '
                     'comment_count, '
                     'sentiment '
                     'FROM videos WHERE channel_id = %s', (channel_id,))
    fetched_videos = mycursor.fetchall()
    engagement_rates = []
    channel_engagement_rate = [0,]

    # На случай, если нет видео (Хз, как)
    if len(fetched_videos) == 0:
        return 0, 0, 0

    for video in fetched_videos:
        title = video[0]
        video_id = video[1]
        like_count = video[2]
        dislike_count = video[3]
        view_count = video[4]
        comment_count = video[5]
        sentiment = video[6]

        # Если хоть одно значение равно нулю, то это значение скрыто и калькуляция не может быть проведена
        if like_count == 0 or dislike_count == 0 or view_count == 0:
            return 0,0,0

        # Кодировка оценки, чтобы позитивные и негативные имели влияние,
        # а нейтральные - нет
        if sentiment > 0.6:
            sentiment = 2
        elif sentiment < 0.4:
            sentiment = 0
        else:
            sentiment = 1
        # Расчет индекса вовлеченности видео
        engagement_rate = (like_count - (2 * dislike_count) + (4 * comment_count * sentiment)) / view_count
        engagement_rates.append([video[0], video[1], engagement_rate])

        # Расчет индекса вовлеченности для всего канала
        if not hidden_subscribers:
            channel_engagement_rate.append((view_count * engagement_rate) / subscribers_count)

    # Расчет среднего индекса вовлеченности (формула для канала)
    channel_engagement_rate = sum(channel_engagement_rate) / len(channel_engagement_rate)

    # Сортировка по индексу вовлеченности
    if len(engagement_rates) <= 1:
        return 0, 0, 0
    engagement_rates.sort(key=lambda i: i[2])
    max_engagement_rate = engagement_rates[-1]
    min_engagement_rate = engagement_rates[0]
    return channel_engagement_rate, max_engagement_rate, min_engagement_rate


def video_frequency(channel_id: str):
    # Возвращает среднюю частоту выкладки видео на канал, на основе последних 20 видео.
    # Возвращаемый период в секундах

    mycursor.execute('SELECT published_at, duration '
                     'FROM videos '
                     'WHERE channel_id = %s '
                     'ORDER BY published_at '
                     'DESC LIMIT 20',
                     (channel_id,))
    fetch_all_videos_date = mycursor.fetchall()

    # Считаем среднюю длительность видео
    videos_length = []
    for video in fetch_all_videos_date:
        videos_length.append(int(video[1]))
    average_length = sum(videos_length)/len(videos_length)

    # Берем даты выхода самого нового и последнего (согласно лимиту, макс. - 20)
    newest_video = fetch_all_videos_date[0][0]
    oldest_video = fetch_all_videos_date[-1][0]

    # Считаем временной промежуток между новым и последним (согласно лимиту) видео,
    # делим на количество найденных видео, получаем частоту, возвращаем в секундах
    time_between = newest_video.date() - oldest_video.date()
    total_videos = len(fetch_all_videos_date)
    period_between_video = (time_between / total_videos).total_seconds()

    return period_between_video, average_length


def first_channel_analysis(channel_id: str):
    print("Начинаем анализ канала...")
    try:
        mycursor.execute("SELECT * FROM channels WHERE channelId = %s", (channel_id,))
        res = mycursor.fetchone()
    except mysql.connector.Error as err:
        print("Ошибка!")
        print(err)
        return False

    print("Имя канала -", res[2])
    print("Изначальный рейтинг -", res[4])
    if (res[17] == 0):
        print("Количество подписчиков предположительно скрыто (либо равно 0)")
    else:
        print('Количество подписчиков -', res[17])

    # Возраст канала
    age = int((datetime.date.today() - res[10].date()).total_seconds())
    age_years = int(age / 60 / 60 / 24 / 365)
    age_months = int(age / 60 / 60 / 24 / 30 - age_years * 12)
    age_days = int(age / 60 / 60 / 24 - age_months * 30 - age_years * 365)
    print('Канал активен(приблизительно): ', age_years, " года(лет), ", age_months, " месяцев(а), ", age_days,
          " дня(ей).", sep="")
    localization_status = res[24]

    # Анализ локализации
    if localization_status < 10:
        mycursor.execute("SELECT title, description FROM videos WHERE channel_id = %s", (channel_id,))
        fetch_title_description = mycursor.fetchall()
        # Исправление статуса локализации при наличии русских видео
        for i in fetch_title_description:
            if hasCyrillic(i[0]) or hasCyrillic(i[1]):
                localization_status += 1

        values = (localization_status, channel_id)
        mycursor.execute('UPDATE channels SET localisationStatus = %s WHERE channelId = %s', values)
        mydb.commit()

    # Удаление канала при наборе до 5 баллов
    if localization_status < 5:
        delete_channel_all(channel_id)

    print("Статус локализации - ", localization_status, sep="")

    # Анализ частоты выкладки видео
    upload_frequency,\
    average_video_length = video_frequency(channel_id)
    print("Частота добавления видео на канал: ", upload_frequency / 60 / 60 / 24, " - дня/ей")
    print("Средняя продолжительность видео: ", average_video_length/60, " - минут")

    # Анализ средней тональной оценки видео согласно комментариям
    average_sentiment_score, \
    maximum_sentiment_score, \
    minimum_sentiment_score = video_sentiment_analysis(channel_id)
    print('--->(Тональность) Средняя тональность комментариев - ', average_sentiment_score)
    print('--->(Тональность) Лучшая оценка видео: ', maximum_sentiment_score[2])
    print('------>Для видео: "', maximum_sentiment_score[0], '"')
    print('------>Ссылка: https://www.youtube.com/watch?v=', maximum_sentiment_score[1], sep="")
    print('--->(Тональность) Худшая оценка видео: ', minimum_sentiment_score[2])
    print('------>Для видео: "', minimum_sentiment_score[0], '"')
    print('------>Ссылка: https://www.youtube.com/watch?v=', minimum_sentiment_score[1], sep="")

    # Анализ вовлеченности
    hidden_subscribers = res[18]
    channel_engagement_rate, \
    max_engagement_rate, \
    min_engagement_rate = engagement_rate_analysis(channel_id, hidden_subscribers)
    if max_engagement_rate == 0 or min_engagement_rate == 0:
        choose = input('Необходимые данные для расчетов скрыты.\n'
                       'Комментарии/лайки/дизлайки \n'
                       'Канал переведен в статус "скрыт"')
        mycursor.execute('UPDATE channels SET rating="hidden" WHERE channelId=%s', (channel_id,))
        mydb.commit()
        return
    print('--->(ER - Индекс вовлеченности) ER всего канала - ', channel_engagement_rate)
    print('--->(ER - Индекс вовлеченности) Лучший ER видео: ', max_engagement_rate[2])
    print('------>Для видео: "', max_engagement_rate[0], '"')
    print('------>Ссылка: https://www.youtube.com/watch?v=', max_engagement_rate[1], sep="")
    print('--->(ER - Индекс вовлеченности) Худший ER видео: ', min_engagement_rate[2])
    print('------>Для видео: "', min_engagement_rate[0], '"')
    print('------>Ссылка: https://www.youtube.com/watch?v=', min_engagement_rate[1], sep="")

    # Анализ аудитории по страницам ВКонтача нах