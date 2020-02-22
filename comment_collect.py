import mysql.connector
from config import *
from funcsCommon import dateToFormat
from youtube_api import YouTubeDataAPI
from datetime import datetime, timedelta

mydb = mysql.connector.connect(
        host=host,
        user=user,
        passwd=password,
        database=database
        )

mycursor = mydb.cursor()

yt = YouTubeDataAPI(googleApiKey)


def fetch_all_comments_from_video_by_id(video_id: str):
    comments = yt.get_video_comments(video_id=video_id, get_replies=False, max_results=None)
    counter = 0
    for i in comments:
        text = i['text']
        comment_like_count = int(i['comment_like_count'])
        comment_publish_date = i['comment_publish_date']
        comment_publish_date = dateToFormat(str(comment_publish_date))
        values = (video_id, text, comment_like_count, comment_publish_date)
        mycursor.execute('INSERT INTO comments (video_id, text, comment_like_count, comment_publish_date) '
                             'VALUES (%s, %s, %s, %s)', values)
        mydb.commit()
        counter += 1
    print("Добавлено: ", counter, " комментариев", sep="")


fetch_all_comments_from_video_by_id('Uj4O2_dwRiA')

