import tkinter as tk
from tkinter import ttk
from tkinter import messagebox
import mysql.connector
from config import *
import re
import urllib.request
import json
import datetime
from datetime import timedelta
import requests
import vk
from youtube_api import YouTubeDataAPI
from tensorflow.keras.models import load_model
from nltk.tokenize import TweetTokenizer
from nltk.stem.snowball import RussianStemmer
import numpy as np
import pandas as pd
import webbrowser
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg


class Main(tk.Frame):
    def __init__(self, root):
        super().__init__(root)
        self.advert_image = tk.PhotoImage(file='images/ad.png')
        self.analysis_image = tk.PhotoImage(file='images/analytics.png')
        self.add_image = tk.PhotoImage(file='images/youtube.png')
        self.compare_image = tk.PhotoImage(file='images/compare.png')
        self.delete_image = tk.PhotoImage(file='images/delete.png')
        self.init_main()
        self.db = db
        self.view_channels()
        self.channel = Channel()
        self.base_analysis = Base_analysis()

    def init_main(self):
        toolbar_container = tk.Frame(bg="#ffe040")
        toolbar_container.pack(side=tk.TOP, fill=tk.X)
        toolbar = tk.Frame(toolbar_container, bg="#ffe040", bd=2)
        toolbar.pack(side=tk.TOP)
        btn_add_channell = tk.Button(toolbar, text="Добавить канал в базу", width=150, command=self.add_channel,
                                     bg="#ffe040",
                                     bd=0, compound=tk.TOP, image=self.add_image)
        btn_add_channell.pack(side=tk.LEFT)
        btn_analysis = tk.Button(toolbar, text="Аналитика каналов", width=150, command=None, bg="#ffe040", bd=0,
                                 compound=tk.TOP, image=self.analysis_image)
        btn_analysis.pack(side=tk.LEFT)
        btn_analysis.bind('<Button-1>', lambda event: self.channel_stats())
        btn_compare = tk.Button(toolbar, text="Сравнение каналов", width=150, command=None, bg="#ffe040", bd=0,
                                 compound=tk.TOP, image=self.compare_image)
        btn_compare.pack(side=tk.LEFT)
        btn_compare.bind('<Button-1>', lambda event: self.channels_compare())
        btn_advert = tk.Button(toolbar, text="Получить рекомендации", width=150, command=None, bg="#ffe040", bd=0,
                               compound=tk.TOP, image=self.advert_image)
        btn_advert.pack(side=tk.LEFT)

        self.tree = ttk.Treeview(self, columns=('channel_id', 'title', 'subs',
                                                'rel_vk', 'view_count'), height=20, show="headings")
        self.tree.column('channel_id', width=120, anchor=tk.CENTER)
        self.tree.column('title', width=100, anchor=tk.CENTER)
        self.tree.column('subs', width=100, anchor=tk.CENTER)
        self.tree.column('rel_vk', width=100, anchor=tk.CENTER)
        self.tree.column('view_count', width=120, anchor=tk.CENTER)
        self.tree.heading('channel_id', text='ID')
        self.tree.heading('title', text='Название')
        self.tree.heading('subs', text='Подписчики')
        self.tree.heading('rel_vk', text='Группа ВК')
        self.tree.heading('view_count', text='Число просмотров')
        self.tree.pack()
        channel = Channel()
        btn_delete = tk.Button(toolbar, text="Удалить канал", width=150,
                               bg="#ffe040", bd=0, compound=tk.TOP, image=self.delete_image)
        btn_delete.bind('<Button-1>', lambda event: self.delete_all())
        btn_delete.pack(side=tk.LEFT)

    def view_channels(self):
        self.db.mycursor.execute("SELECT channelId, title, subscriberCount, relatedVkGroup, viewCount "
                                 "FROM channels ORDER BY subscriberCount DESC")
        [self.tree.delete(i) for i in self.tree.get_children()]
        [self.tree.insert('', 'end', values=row) for row in self.db.mycursor.fetchall()]

    def add_channel(self):
        Child()

    def channel_stats(self, channel_id=""):
        channel_id_get = self.tree.set(self.tree.selection(), '#1')
        if channel_id == "":
            channel_id = channel_id_get
        if channel_id is not None and channel_id != "":
            Child_Channel_Stats(channel_id)
        else:
            self.base_analysis.error_popup('Выберите канал для анализа!')

    def channels_compare(self):
        print([self.tree.item(x) for x in self.tree.selection()])
        vk_address_list = []
        for i in self.tree.selection():
            item = self.tree.item(i, "value")
            vk_address_list.append(item[3])
        print(vk_address_list)
        Audience_Short(vk_address_list)


    def delete_all(self, channel_id="t"):
        pass_value = self.tree.set(self.tree.selection(), '#1')
        if pass_value is None or pass_value == "":
            self.base_analysis.error_popup('Выберите канал для удаления!')
            return
        if channel_id != "t":
            pass_value = channel_id
        value = (pass_value,)
        query = "SELECT relatedVkGroup FROM channels WHERE channelId = %s"
        db.mycursor.execute(query, value)
        vk_group = db.mycursor.fetchone()
        query = "DELETE FROM vk_page_subs WHERE vk_page = %s"
        try:
            db.mycursor.execute(query, vk_group)
            db.mydb.commit()
            print("Записи об участниках групп успешно удалены")
        except mysql.connector.Error as err:
            print("Ошибка при удалении данных канала -", err)
            raise SystemExit

        query = "DELETE FROM channels WHERE channelId = %s"
        try:
            db.mycursor.execute(query, value)
            db.mydb.commit()
            print("Channels - Канал успешно удален из списка каналов!")
        except mysql.connector.Error as err:
            print("Ошибка при удалении данных канала -", err)
            raise SystemExit

        try:
            query = "DELETE FROM channelsToGo WHERE leadingChannelId = %s"
            db.mycursor.execute(query, value)
            db.mydb.commit()
            print("Channels_to_go - Каналы, на которые ссылался пустой канал также удалены!")
        except mysql.connector.Error as err:
            print("Ошибка при удалении данных канала -", err)
            raise SystemExit

        try:
            query = "DELETE FROM videos_ids WHERE channel_id = %s"
            db.mycursor.execute(query, value)
            db.mydb.commit()
            print("Videos_ids - Видео из списка видео удалены!")
        except mysql.connector.Error as err:
            print("Ошибка при удалении данных канала -", err)
            raise SystemExit

        try:
            query = "SELECT * FROM videos WHERE channel_id = %s"
            db.mycursor.execute(query, value)
            videos = db.mycursor.fetchall()
            dropped_videos_counter = 0
            for video in videos:
                video_id = (video[2],)
                query = "DELETE FROM comments WHERE video_id = %s"
                db.mycursor.execute(query, video_id)
                db.mydb.commit()
            query = "DELETE FROM videos WHERE channel_id = %s"
            db.mycursor.execute(query, value)
            db.mydb.commit()
            print("Videos - Все видео удалены!")
        except mysql.connector.Error as err:
            print("Ошибка при удалении данных канала -", err)
            raise SystemExit
        self.base_analysis.message_popup('Канал успешно удален!\n\n'
                                         'Возможно, канал не подошел\n'
                                         'по локализации или количеству\nвидео')
        self.view_channels()

    def init_channel_add(self, channel_link):
        channel_to_add = Channel()
        channel_id = channel_to_add.channel_add(channel_link=channel_link)
        self.view_channels()
        self.channel_stats(channel_id)


class Child(tk.Toplevel):
    def __init__(self):
        super().__init__(root)
        self.iconbitmap(default='icon.ico')
        self.init_child()
        self.view = app

    def init_child(self):
        self.title("Поиск канала")
        self.geometry("400x80+250+250")
        self.resizable(False, False)

        self.grab_set()
        self.focus_set()

        label_entry_channel_id = tk.Label(self, text="ID канала:")
        label_entry_channel_id.place(x=20, y=20)

        self.entry_channel_id = ttk.Entry(self)
        self.entry_channel_id.place(x=90, y=20)

        btn_cancel = ttk.Button(self, text="Закрыть", command=self.destroy)
        btn_cancel.place(x=310, y=19)

        btn_go = ttk.Button(self, text="Добавить", command=self.destroy)
        btn_go.place(x=230, y=19)

        def end(event):
            self.view.init_channel_add(self.entry_channel_id.get())

        btn_go.bind("<Button-1>", end)


class Child_Channel_Stats(tk.Toplevel):
    def __init__(self, channel_link):
        super().__init__(root)
        self.view = app
        self.iconbitmap(default='icon.ico')
        self.channel_link = channel_link
        self.init_analysis()
        self.videos_insert()


    def init_analysis(self):
        self.title("Канал успешно добавлен!")
        self.geometry("500x388+150+150")
        self.resizable(False, False)
        self.grab_set()
        self.focus_set()
        notebook = ttk.Notebook(self)
        tab1 = ttk.Frame(notebook)
        notebook.add(tab1, text='Статистика')
        tab2 = ttk.Frame(notebook)
        notebook.add(tab2, text='Видео')
        tab3 = ttk.Frame(notebook)
        notebook.add(tab3, text='Аудитория')
        notebook.pack(expand=1, fill='both')

        '''-----TAB 1-----'''
        stats_data_frame = tk.Frame(tab1, width=300, height=388, bg="white")
        stats_buttons_frame = tk.Frame(tab1, width=200, height=388, bg="#666666")
        stats_data_frame.grid(row=0, column=1)
        stats_buttons_frame.grid(row=0, column=0)
        '''-----TAB 1----LEFT-----'''
        btn_main_analysis = tk.Button(stats_buttons_frame, text="Базовый анализ",
                                      relief=tk.RIDGE, bd=0, padx=20, pady=20, width=23, bg="#ffe040",
                                      font=("", 8, "bold"), fg="black")
        btn_main_analysis.bind("<Button-1>")
        btn_info = tk.Button(stats_buttons_frame, bd=0, text="Инструкция", padx=20, pady=10, width=23)
        btn_main_analysis.place(x=0, y=5)
        btn_info.place(x=0, y=70)
        btn_info.bind("<Button-1>", lambda event: self.view.base_analysis.message_popup(
            "Для анализа нового канала:\n"
            "Вкладка 'Видео' - 'Добавить видео'\n"
            "Вкладка 'Аудитория' - 'Собрать информацию'\n"
            "После, можно пользоваться кнопками анализа"))
        '''-----/TAB 1----LEFT-----'''
        '''-----TAB 1----RIGHT-----'''

        channel_base_stats = self.view.channel.return_base_stats(self.channel_link)
        tree_stats = ttk.Treeview(stats_data_frame, columns=('name', 'value'), height=18, show="headings")
        tree_stats.column('name', width=150, anchor=tk.CENTER)
        tree_stats.column('value', width=150, anchor=tk.CENTER)
        tree_stats.heading('name', text="Характеристика")
        tree_stats.heading('value', text="Значение")

        d_year = str(channel_base_stats[6].year)
        d_month = str(channel_base_stats[6].month)
        d_day = str(channel_base_stats[6].day)
        date = d_day + "." + d_month + "." + d_year

        age = int((datetime.date.today() - channel_base_stats[6].date()).total_seconds())
        age_years = int(age / 60 / 60 / 24 / 365)
        age_months = int(age % (60 * 60 * 24 * 365) / 60 / 60 / 24 / 30)
        if age_years == 1:
            year = "год"
        elif age_years in range(2, 5):
            year = "года"
        else:
            year = "лет"
        month = "мес."
        age_age = str(age_years) + " " + str(year) + ", " + str(age_months) + " " + str(month)

        tree_stats.insert("", 1, values=("Название", channel_base_stats[1]))
        tree_stats.insert("", 2, values=("Рейтинг", channel_base_stats[2]))
        tree_stats.insert("", 3, values=("Кол-во подписчиков", channel_base_stats[9]))
        tree_stats.insert("", 4, values=("Кол-во просмотров", channel_base_stats[7]))
        tree_stats.insert("", 5, values=("Кол-во видео", channel_base_stats[10]))
        tree_stats.insert("", 6, values=("Группа Вк", channel_base_stats[3]))
        tree_stats.insert("", 7, values=("Instagram", channel_base_stats[4]))
        tree_stats.insert("", 8, values=("Связаться", channel_base_stats[5]))
        tree_stats.insert("", 9, values=("Канал активен (пр-но)", age_age))
        tree_stats.insert("", 10, values=("ER канала", channel_base_stats[11]))


        tree_stats.pack()
        '''-----/TAB 1----RIGHT-----'''
        '''-----/TAB 1-----'''

        '''-----TAB 2-----'''
        videos_left_frame = tk.Frame(tab2, width=200, height=388, bg="#666666")
        videos_right_frame = tk.Frame(tab2, width=300, height=388)
        videos_left_frame.grid(row=0, column=0)
        videos_right_frame.grid(row=0, column=1)

        '''-----TAB 2----LEFT-----'''
        btn_add_videos = tk.Button(videos_left_frame, text="Собрать видео",
                                   relief=tk.RIDGE, bd=0, padx=20, pady=20, width=23,
                                   bg="#ffe040", font=("", 8, "bold"), fg="black")

        def what_result(event):
            # self.view.base_analysis.message_popup("Будет произведен сбор данных!\n\nЭто может занять несколько минут!")
            self.loader_videos = ttk.Progressbar(videos_left_frame, length=160)
            self.loader_videos.place(x=20, y=300)
            res = self.collect_videos(self.channel_link)

            if res == "none":

                self.view.base_analysis.message_popup("Ни одного видео не было добавлено!")
            elif res == "delete":
                self.view.delete_all(self.channel_link)
            else:
                self.view.base_analysis.message_popup(f"Добавлено видео: {res}")
                self.videos_insert()
            self.loader_videos['value'] = 0
            self.loader_videos.update()
            self.loader_videos.destroy()
            self.grab_set()
            self.focus_set()

        btn_add_videos.bind("<Button-1>", what_result)
        btn_analize_videos = tk.Button(videos_left_frame, bd=0, text="Анализировать", font=("", 8, "bold"), bg="#bfae54",
                                          padx=20, pady=20, width=23, fg="white")
        btn_analize_videos.bind("<Button-1>", self.analise)
        btn_short_info_videos = tk.Button(videos_left_frame, bd=0, text="Сводка по видео",
                                          padx=20, pady=10, width=23)
        btn_short_info_videos.bind("<Button-1>", self.videos)
        btn_short_info_comments = tk.Button(videos_left_frame, bd=0, text="Сводка по\nкомментариям",
                                            padx=20, pady=10, width=23)
        btn_short_info_comments.bind("<Button-1>", self.comments_info)
        btn_add_videos.place(x=0, y=5)
        btn_analize_videos.place(x=0, y=70)
        btn_short_info_videos.place(x=0, y=139)
        btn_short_info_comments.place(x=0, y=190)
        '''-----/TAB 2----LEFT-----'''
        '''-----TAB 2----RIGHT-----'''
        scroll = tk.Scrollbar(videos_right_frame)
        scroll.pack(side=tk.RIGHT, fill=tk.Y)

        self.videos_right_table = ttk.Treeview(videos_right_frame, yscrollcommand=scroll.set,
                                               columns=('id', 'name', 'value'), height=18, show="headings")
        scroll.config(command=self.videos_right_table.yview)
        self.videos_right_table.column('id', width=0, anchor=tk.CENTER)
        self.videos_right_table.column('name', width=180, anchor=tk.CENTER)
        self.videos_right_table.column('value', width=90, anchor=tk.CENTER)
        self.videos_right_table.heading('id', text="ID")
        self.videos_right_table.heading('name', text="Название")
        self.videos_right_table.heading('value', text="Просмотры")
        self.videos_insert()
        self.videos_right_table.bind("<Double-1>", self.link_tree_video)
        self.videos_right_table.pack()
        '''-----/TAB 2----RIGHT-----'''
        '''-----/TAB 2-----'''
        '''-----TAB 3-----'''
        self.audience_left_frame = tk.Frame(tab3, width=200, height=388, bg="#666666")
        self.audience_right_frame = tk.Frame(tab3, width=300, height=388)
        self.audience_left_frame.grid(row=0, column=0)
        self.audience_right_frame.grid(row=0, column=1)

        '''-----TAB 3----LEFT-----'''

        btn_collect_audience = tk.Button(self.audience_left_frame, bd=0, text="Собрать аудиторию",
                                         bg="#ffe040", font=("", 8, "bold"), fg="black",
                                         padx=20, pady=20, width=23)
        btn_collect_audience.bind("<Button-1>", self.collect_vk)
        btn_analise_audience = tk.Button(self.audience_left_frame, bd=0, text="Анализировать аудиторию",
                                         bg="#bfae54", font=("", 8, "bold"), fg="white",
                                         padx=18, pady=20, width=23)
        btn_analise_audience.bind("<Button-1>", self.analise_vk)
        btn_short_info_audience = tk.Button(self.audience_left_frame, bd=0, text="Сводка по\nаудитории",
                                            padx=20, pady=10, width=23)
        btn_short_info_audience.bind("<Button-1>", self.audience_short_inf)
        btn_cluster_audience = tk.Button(self.audience_left_frame, bd=0, text="Сегментация\nаудитории",
                                         padx=20, pady=10, width=23)
        btn_cluster_audience.bind("<Button-1>", self.cluster)
        btn_collect_audience.place(x=0, y=5)
        btn_analise_audience.place(x=0, y=70)
        btn_short_info_audience.place(x=0, y=135)
        btn_cluster_audience.place(x=0, y=196)
        '''-----TAB 3----LEFT-----'''
        '''-----TAB 3----RIGHT-----'''
        vk_right_top = tk.Frame(self.audience_right_frame)
        self.vk_right_bottom = tk.Frame(self.audience_right_frame)
        vk_right_top.place(x=0, y=0)
        self.vk_right_bottom.place(x=0, y=40)

        self.vk_right_top_btn_groups = tk.Button(vk_right_top, text="Группы", pady=8, padx=39)
        self.vk_right_top_btn_groups.grid(row=0, column=1)
        self.vk_right_top_btn_groups.bind("<Button-1>", self.show_group_table)
        self.vk_rigt_top_btn_users = tk.Button(vk_right_top, text="Пользователи", pady=8, padx=40)
        self.vk_rigt_top_btn_users.grid(row=0, column=2)
        self.vk_rigt_top_btn_users.bind("<Button-1>", self.show_users_table)
        self.show_group_table
        '''-----/TAB 3----RIGHT-----'''
        '''-----/TAB 3-----'''

    ''' Базовый анализ видео '''
    def analise(self, event):
        '''
        Суть - заполнить таблицу videos_stats
        значения:
            частота появления видео, средняя продолжительность видео, средний ER, максимальный ER, min ER
            ср кол-во комментариев в видео, оценка тональности ср, макс, мин
        :param event:
        :return:
        '''
        self.view.db.mycursor.execute("SELECT subscriberCount FROM channels WHERE channelId = %s", (self.channel_link,))
        subscribers_count = self.view.db.mycursor.fetchone()
        subscribers_count = subscribers_count[0]
        query = "SELECT video_id, published_at, duration, sentiment, engagement_rate, comment_count, view_count " \
                "FROM videos WHERE channel_id = %s " \
                "ORDER BY published_at DESC"
        values = (self.channel_link,)
        self.view.db.mycursor.execute(query, values)
        videos = self.view.db.mycursor.fetchall()
        videos_length = []
        sentiments = []
        eng_rates = []
        channel_er = []
        q_comments = []
        for video in videos:
            videos_length.append(int(video[2]))
            sentiments.append(float(video[3]))
            eng_rates.append(float(video[4]))
            if subscribers_count != 0:
                channel_er.append(int(video[6])*float(video[4])/subscribers_count)
            q_comments.append(int(video[5]))
        if len(channel_er) > 0:
            channel_er_res = sum(channel_er) / len(channel_er)
        else:
            channel_er_res = 0.0

        # Собрано комментариев
        collected_comments = int(sum(q_comments))

        # Среднее кол-во комментариев
        average_comments = int(sum(q_comments) / len(q_comments))

        # Средний индекс вовлеченности
        average_er = float(sum(eng_rates) / len(eng_rates))

        # Средняя тональная оценка
        average_sentiment = float(sum(sentiments) / len(sentiments))

        # Средняя продолжительность:
        average_length = int(sum(videos_length) / len(videos_length))

        # Расчет частоты выкладки:
        newest_video = videos[0][1]
        oldest_video = videos[-1][1]
        time_between = newest_video.date() - oldest_video.date()
        total_videos = len(videos)
        average_period = int((time_between / total_videos).total_seconds())
        self.view.db.mycursor.execute("SELECT * FROM analysis_videos WHERE channel_id = %s", (self.channel_link,))
        res = self.view.db.mycursor.fetchall()
        channell = self.channel_link
        self.view.db.mycursor.execute("UPDATE channels SET er=%s WHERE channelId=%s", (channel_er_res, self.channel_link))
        self.view.db.mydb.commit()
        if len(res) > 0:
            query = "UPDATE analysis_videos " \
                    "SET channel_id=%s, collected_comments=%s, average_comments=%s, average_er=%s, " \
                    "average_sentiment=%s, average_length=%s, average_period=%s WHERE channel_id=%s"
            values = (self.channel_link, collected_comments, average_comments, average_er,
                      average_sentiment, average_length, average_period, self.channel_link)
        else:
            query = "INSERT INTO analysis_videos " \
                    "(channel_id, collected_comments, average_comments, average_er, " \
                    "average_sentiment, average_length, average_period) " \
                    "VALUES (%s, %s, %s, %s, %s, %s, %s)"
            values = (self.channel_link, collected_comments, average_comments, average_er,
                      average_sentiment, average_length, average_period)
        try:
            self.view.db.mycursor.execute(query, values)
        except mysql.connector.Error as err:
            self.view.base_analysis.error_popup(f"ОШИБКА - {err}")
        else:
            self.view.db.mydb.commit()
            self.view.base_analysis.message_popup("Видео канала успешно проанализированы")

    def analise_vk(self, event):
        self.view.db.mycursor.execute("SELECT relatedVkGroup FROM channels WHERE channelId = %s", (self.channel_link,))
        group = self.view.db.mycursor.fetchone()
        group = group[0]
        print(group)
        self.view.db.mycursor.execute(f"""SELECT user_id, user_sex, user_birthday, user_followers_count,
                                user_occupation_type, user_relation, user_personal_religion_id,
                                user_personal_political, user_personal_smoking, user_personal_alcohol 
                                FROM vk_users WHERE user_id in 
                                (SELECT user_id FROM vk_page_subs WHERE vk_page = %s)""", (group,))
        subs = self.view.db.mycursor.fetchall()
        men_users = 0
        women_users = 0
        ages = {"hidden":[], "<18":[], "18-25":[], "26-35":[], "36-50":[], "51-65":[], ">65":[]}
        followers = []
        occupation = []
        smoking = []
        alcohol = []
        relation = []
        children_count = []
        bro_count = []
        working_count = []
        religion_ids = []
        for sub in subs:
            user_id = sub[0]
            user_sex = sub[1]
            if user_sex == 1:
                women_users += 1
            elif user_sex == 2:
                men_users += 1
            user_birthday = sub[2]
            user_age = int(datetime.datetime.now().year - user_birthday.year)
            if user_birthday.year == 1000:
                ages['hidden'].append(1)
            if user_age < 18:
                ages['<18'].append(user_age)
            elif 18 <= user_age <= 25:
                ages['18-25'].append(user_age)
            elif 26 <= user_age <= 35:
                ages['26-35'].append(user_age)
            elif 36 <= user_age <= 50:
                ages['36-50'].append(user_age)
            elif 51 <= user_age <= 65:
                ages['51-65'].append(user_age)
            elif 65 <= user_age < 100:
                ages['>65'].append(user_age)

            user_followers = sub[3]
            if user_followers != 0:
                followers.append(user_followers)
            user_occupation_type = sub[4]
            if user_occupation_type != "none":
                if user_occupation_type == "work":
                    occupation.append(0)
                elif user_occupation_type == "school":
                    occupation.append(1)
                elif user_occupation_type == "university":
                    occupation.append(2)
            user_relation = sub[5]
            if user_relation != 0:
                # 0 - свободные, в акт поиске, 1-есть подруга/влюблен, 2-с невестой, женой, в гр. браке, 4-все сложно
                if user_relation == 1 or user_relation == 6:
                    relation.append(0)
                elif user_relation == 2 or user_relation == 7:
                    relation.append(1)
                elif user_relation == 3 or user_relation == 4 or user_relation == 8:
                    relation.append(2)
                else:
                    relation.append(3)
            user_religion_id = sub[6]
            user_political = sub[7]
            user_smoking = sub[8]
            if user_smoking != 0:
                if user_smoking > 3:
                    smoking.append(1)
                else:
                    smoking.append(0)
            user_alcohol = sub[9]
            if user_alcohol != 0:
                if user_alcohol > 3:
                    alcohol.append(1)
                else:
                    alcohol.append(0)

        all_ages = len(ages['hidden']) + len(ages['<18']) + len(ages['18-25']) + len(ages['26-35']) + \
                   len(ages['36-50']) + len(ages['51-65']) + len(ages['>65'])

        # Доли мужчин и женщин
        mens_part = float(men_users/(men_users+women_users))
        womens_part = float(women_users/(men_users+women_users))

        # Доли по возрастным группам
        hidden = len(ages['hidden'])/all_ages
        to18 = len(ages['<18'])/all_ages
        f18to25 = len(ages['18-25'])/all_ages
        f26to35 = len(ages['26-35'])/all_ages
        f36to50 = len(ages['36-50'])/all_ages
        f51to65 = len(ages['51-65'])/all_ages
        f65 = len(ages['>65'])/all_ages

        # среднее и медианное по подписчикам
        followers_av = int(sum(followers)/len(followers))
        followers_med = int(np.median(followers))

        # Доля работающих, студентов, школьников
        work_part = float(occupation.count(0)/len(occupation))
        university_part = float(occupation.count(2)/len(occupation))
        school_part = float(occupation.count(1)/len(occupation))

        # Доли по отношениям (свободен, подруга, женат, все сложно)
        rel_free_part = float(relation.count(0)/len(relation))
        rel_friend_part = float(relation.count(1)/len(relation))
        rel_married_part = float(relation.count(2)/len(relation))
        rel_difficult_part = float(relation.count(3)/len(relation))

        # Доли курящих и некурящих
        smoking_part = float(smoking.count(1)/len(smoking))
        alcohol_part = float(alcohol.count(1)/len(alcohol))

        print(mens_part, womens_part, sep=" ")
        print(all_ages)
        print(hidden, to18, f18to25, f26to35, f36to50, f51to65, f65, sep=" ")
        print(followers_av, followers_med, sep=" ")
        print(work_part, university_part, school_part, sep=" ")
        print(rel_free_part, rel_friend_part, rel_married_part, rel_difficult_part, sep=" ")
        print(smoking_part, alcohol_part, sep=" ")
        self.view.db.mycursor.execute("SELECT channel_id FROM audience_stats WHERE vk_link = %s", (group,))
        update = self.view.db.mycursor.fetchall()
        # Проверим, нужно ли добавлять данные о статистике или обновлять?
        if len(update) > 0:
            query = f"UPDATE audience_stats SET " \
                    "vk_link=%s, channel_id=%s, men_part=%s, women_part=%s, hidden=%s, to18=%s, f18to25=%s, f26to35=%s, " \
                    "f36to50=%s, f51to65=%s, f65=%s, followers_av=%s, followers_med=%s, work_part=%s, " \
                    "university_part=%s, school_part=%s, rel_free_part=%s, rel_friend_part=%s, rel_married_part=%s, " \
                    "rel_difficult_part=%s, smoking_part=%s, alcohol_part=%s " \
                    "WHERE vk_link = %s"
            values = (group, self.channel_link, mens_part, womens_part, hidden, to18, f18to25, f26to35, f36to50, f51to65,
                      f65, followers_av, followers_med, work_part, university_part, school_part, rel_free_part,
                      rel_friend_part, rel_married_part, rel_difficult_part, smoking_part, alcohol_part, group)

        else:
            query = "INSERT INTO audience_stats " \
                    "(vk_link, channel_id, men_part, women_part, hidden, to18, f18to25, f26to35, f36to50, f51to65, f65, " \
                    "followers_av, followers_med, work_part, university_part, school_part, rel_free_part, rel_friend_part, " \
                    "rel_married_part, rel_difficult_part, smoking_part, alcohol_part) " \
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            values = (group, self.channel_link, mens_part, womens_part, hidden, to18, f18to25, f26to35, f36to50, f51to65,
                      f65, followers_av, followers_med, work_part, university_part, school_part, rel_free_part,
                      rel_friend_part, rel_married_part, rel_difficult_part, smoking_part, alcohol_part)
        try:
            self.view.db.mycursor.execute(query, values)
            self.view.db.mydb.commit()
            self.view.base_analysis.message_popup("Данные успешно проанализированы!")
        except mysql.connector.Error as err:
            self.view.base_analysis.error_popup(f'Ошибка - {err}')

    def audience_short_inf(self, event):
        Audience_Short(self.channel_link)

    ''' Показать таблицу групп на вкладке аудитории '''
    def show_group_table(self, event):
        try:
            self.vk_right_bottom_tree_users
            if self.vk_right_bottom_tree_users.winfo_exists():
                self.vk_right_bottom_tree_users.destroy()
        except (NameError, AttributeError):
            pass
        try:
            self.vk_right_bottom_tree_groups
            if self.vk_right_bottom_tree_groups.winfo_exists():
                return
            else:
                self.groups_table()

        except (NameError, AttributeError):
            self.groups_table()

    ''' Показать таблицу пользователей на вкладке аудитории '''
    def show_users_table(self, event):
        try:
            self.vk_right_bottom_tree_groups
            if self.vk_right_bottom_tree_groups.winfo_exists():
                self.vk_right_bottom_tree_groups.destroy()
        except (NameError, AttributeError):
            pass
        try:
            self.vk_right_bottom_tree_users
            if self.vk_right_bottom_tree_users.winfo_exists():
                return
            else:
                self.users_table()
                self.users_insert()

        except (NameError, AttributeError):
            self.users_table()
            self.users_insert()

    ''' Создание таблицы групп на вкладке аудитории '''
    def groups_table(self):
        self.vk_right_bottom_tree_groups = ttk.Treeview(self.vk_right_bottom,
                                                        columns=('address'), height=15, show="headings")
        self.vk_right_bottom_tree_groups.column('address', width=293, anchor=tk.CENTER)
        self.vk_right_bottom_tree_groups.heading('address', text="Адрес")
        self.vk_right_bottom_tree_groups.bind("<Double-1>", self.link_tree_group)
        self.groups_insert()
        self.vk_right_bottom_tree_groups.pack()

    ''' Создание таблицы пользователей на вкладке аудитории '''
    def users_table(self):
        scroll_vk_users_y = tk.Scrollbar(self.vk_right_bottom)
        scroll_vk_users_y.pack(side=tk.RIGHT, fill=tk.Y)
        self.vk_right_bottom_tree_users = ttk.Treeview(self.vk_right_bottom, yscrollcommand=scroll_vk_users_y.set,
                                                       columns=('id', 'name', 'sex', 'age'),
                                                       height=15, show="headings")
        scroll_vk_users_y.config(command=self.vk_right_bottom_tree_users.yview)
        self.vk_right_bottom_tree_users.column('id', width=10, anchor=tk.CENTER)
        self.vk_right_bottom_tree_users.column('name', width=133, anchor=tk.CENTER)
        self.vk_right_bottom_tree_users.column('sex', width=40, anchor=tk.CENTER)
        self.vk_right_bottom_tree_users.column('age', width=90, anchor=tk.CENTER)
        self.vk_right_bottom_tree_users.heading('id', text="ID")
        self.vk_right_bottom_tree_users.heading('name', text="Имя")
        self.vk_right_bottom_tree_users.heading('sex', text="Пол")
        self.vk_right_bottom_tree_users.heading('age', text="Возраст")
        self.vk_right_bottom_tree_users.bind("<Double-1>", self.link_tree_users)
        self.vk_right_bottom_tree_users.pack()

    def collect_vk(self, event):
        self.users_progress = ttk.Progressbar(self.audience_left_frame, length=160)
        self.users_progress.place(x=20, y=300)
        self.view.db.mycursor.execute("SELECT relatedVkGroup FROM channels WHERE channelId = %s", (self.channel_link,))
        related_vk_group = self.view.db.mycursor.fetchone()
        print(related_vk_group[0])
        res, count = self.collect_users(related_vk_group[0])
        if res == True:
            self.view.base_analysis.message_popup(f"Добавлено пользователей: {count}")
            self.show_users_table
        else:
            self.view.base_analysis.error_popup("Не получилось добавить пользователей")


        self.users_progress.destroy()
        self.grab_set()
        self.focus_set()

    def videos_insert(self):
        [self.videos_right_table.delete(i) for i in self.videos_right_table.get_children()]
        fetched_videos = self.view.channel.get_videos_for_table(self.channel_link)
        if len(fetched_videos) == 0:
            self.videos_right_table.insert("", "end", values=("", "Видео отсутствуют"))
        for video in fetched_videos:
            self.videos_right_table.insert("", "end", values=(video[0], video[1], video[2]))

    def groups_insert(self):
        [self.vk_right_bottom_tree_groups.delete(i) for i in self.vk_right_bottom_tree_groups.get_children()]
        self.view.db.mycursor.execute("SELECT relatedVkGroup, relatedInstagramPage, relatedPromotionPage, relatedOther "
                                      "FROM channels WHERE channelId = %s", (self.channel_link,))
        pages = self.view.db.mycursor.fetchone()
        vk_group = pages[0]
        inst_group = pages[1]
        promo = pages[2]
        other = pages[3]
        fetched_array = []
        if vk_group == "" and vk_group != "[]":
            vk_group = "Нет основной группы"
            fetched_array.append(vk_group)
        else:
            fetched_array.append(vk_group)
        if inst_group != "" and inst_group != "[]":
            fetched_array.append(inst_group)
        if promo != "" and promo != "[]":
            fetched_array.append(promo)
        if isinstance(other, list):
            for i in other:
                if i != "" and i != "[]":
                    fetched_array.append(i)
        if len(fetched_array) == 0:
            self.vk_right_bottom_tree_groups.insert("", "end", values=("Ссылки отсутствуют"))
        for link in fetched_array:
            link = link.strip("[]'")
            self.vk_right_bottom_tree_groups.insert("", "end", values=(link))

    def users_insert(self):
        limit=200

        [self.vk_right_bottom_tree_users.delete(i) for i in self.vk_right_bottom_tree_users.get_children()]
        self.view.db.mycursor.execute("SELECT relatedVkGroup FROM channels WHERE channelId = %s", (self.channel_link,))
        vk_group = self.view.db.mycursor.fetchone()
        vk_group = vk_group[0]
        print(vk_group)
        query = f"""SELECT user_id, user_first_name, user_last_name, user_sex, 
                               user_birthday FROM vk_users WHERE user_id in 
                               (SELECT user_id FROM vk_page_subs WHERE vk_page = %s) LIMIT {limit}"""
        self.view.db.mycursor.execute(query, (vk_group,))
        users = self.view.db.mycursor.fetchall()
        if len(users) == 0:
            self.vk_right_bottom_tree_users.insert("", "end", values=("", "Соберите информацию", "", ""))
        for user in users:
            user_id = str(user[0])
            user_name = user[2]+" "+user[1]
            if user[3] == 2:
                user_sex = "Муж"
            elif user[3] == 1:
                user_sex = "Жен"
            else:
                user_sex = "Н/у"
            user_age = str(datetime.datetime.now().year - user[4].year)
            if user[4].year == 1000:
                user_age = "Н/у"
            self.vk_right_bottom_tree_users.insert("", "end", values=(user_id, user_name, user_sex, user_age))

    def link_tree_video(self, event):
        input_id = self.videos_right_table.selection()
        self.input_item = self.videos_right_table.item(input_id, "value")
        print(self.input_item)
        webbrowser.open('https://www.youtube.com/watch?v={}'.format(self.input_item[0]))

    def link_tree_group(self, event):
        address = self.vk_right_bottom_tree_groups.selection()
        address = self.vk_right_bottom_tree_groups.item(address, "value")
        webbrowser.open(address[0])

    def link_tree_users(self, event):
        address = self.vk_right_bottom_tree_users.selection()
        address = self.vk_right_bottom_tree_users.item(address, "value")
        print(address)
        webbrowser.open("https://www.vk.com/id"+address[0])

    def collect_videos(self, channel_id):
        global final
        query = 'SELECT title FROM channels WHERE channelId = %s'
        value = (channel_id,)
        self.view.db.mycursor.execute(query, value)
        channelSearchRes = self.view.db.mycursor.fetchall()
        if len(channelSearchRes) == 1:
            print("Осуществляем поиск видео по каналу...")
            res = self.view.channel.yt.search(channel_id=channel_id, order_by='date', max_results=20)
            # Добавляем канал, только если на нем более 5 видео, если нет, удаляем из БД
            if len(res) > 5:
                r = []
                counter = 0
                counter_progress_bar = 0
                # Перебираем каждый найденный результат
                for video in res:
                    print(video)
                    counter += 1
                    # counter_progress_bar += 1
                    # funcsCommon.update_progress(counter_progress_bar / len(res)*2)
                    # Проверяем, не существует ли уже такое видео
                    # (на случай вызова функции из другого участка кода, когда уже добавлены видео на втором этапе)
                    check_if_exists = self.view.channel.check_if_video_exists_in_table(video['video_id'], 'videos_ids')
                    if check_if_exists:
                        print(f'Ошибка, видео "{video["video_title"]}" уже было добавлено ранее')
                    else:
                        # Добавляем видео в БД и присылаем в массив r True, если добавлено и False если не добавлено
                        r.append(self.view.channel.insert_in_videos_ids(video['channel_id'], video['video_id'],
                                                                        video['video_publish_date']))
                    # Как только закончились результаты, выбираем видео, что были раньше,
                    # необходимо для максимума результатов - 100 видео
                    # if counter == 10:
                    #     delta = datetime.datetime(video['video_publish_date']) - timedelta(days=0.5)
                    #     res = self.view.channel.yt.search(channel_id=channel_id, order_by='date',
                    #                                       published_before=delta, max_results=10)
                    #     for video2 in res:
                    #         # counter_progress_bar += 1
                    #         # funcsCommon.update_progress(counter_progress_bar / len(res)*2)
                    #         check_if_exists = self.view.channel.check_if_video_exists_in_table(video2['video_id'],
                    #                                                                            'videos_ids')
                    #         if check_if_exists:
                    #             print(f'Ошибка, видео "{video2["video_title"]}" уже было добавлено ранее')
                    #         else:
                    #             r.append(
                    #                 self.view.channel.insert_in_videos_ids(video2['channel_id'], video2['video_id'],
                    #                                                        video2['video_publish_date']))

                print("Видео успешно добавлены (Кол-во: ", len(r), ")!", sep="")
                # Обновляем дату последних изменений
                self.view.channel.update_channels_last_edit(channel_id)
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
                    return "none"
                # Если же что-то было добавлено, до собираем подробную информацию по каждому добавленному видео, которые
                # есть в таблице videos_ids и нет в таблице videos
                if final:
                    final2, counter = self.get_all_info_from_videos(channel_id)

                    if final2:
                        return str(counter)
                    else:
                        return "none"
            else:
                print(f"На канале {channelSearchRes[0][0]} отсутсвуют видео (или их меньше 5),"
                      f"\nпроизводим удаление канала из базы данных...")

                return "none"
            print("----------------------------------")
        else:
            print("\nERROR---------------------------\n"
                  "Канала с таким Id не существует в базе данных,\n"
                  "для поиска его видео сначала необходимо добавить канал!"
                  "\nERROR---------------------------")
            return "none"

    def get_all_info_from_videos(self, channel_id: str):
        query = "SELECT video_id FROM videos_ids WHERE channel_id = %s"
        value = (channel_id,)
        try:
            self.view.db.mycursor.execute(query, value)
        except mysql.connector.Error as err:
            print(err)
        result = self.view.db.mycursor.fetchall()
        counter_collected_video_info = 0
        if len(result) > 0:
            step = int(100 / len(result))
            for i in result:
                r = self.view.channel.check_if_video_exists_in_table(i[0], 'videos')
                if r:
                    self.loader_videos['value'] += step
                    self.loader_videos.update()
                    continue
                else:
                    r = self.view.channel.collect_video_info(i[0])
                    self.loader_videos['value'] += step
                    self.loader_videos.update()
                    if r:
                        counter_collected_video_info += 1
            if counter_collected_video_info > 0:
                print(f"Собрано подробной информации о \"{counter_collected_video_info}\" видео!")
                return True, counter_collected_video_info
            else:
                return False, 0
        else:
            print('Это соообщение ты в принципе по всей логике не должен был увидеть,\nведь '
                  'эта часть кода должна была вывестись только если программа решила,\nчто она добавила видео, '
                  'а потом их же не нашла.\nСтранно все ёто...')
            return False, 0

    def collect_users(self, vk_adress: str):
        group_id = self.view.base_analysis.return_id(vk_adress)
        if "public" in group_id:
            group_id = group_id[6:]
        try:
            res = self.view.base_analysis.vkApi.groups.getMembers(group_id=group_id, count=1, v=vkApiVersion)
        except:
            try:
                res = self.view.base_analysis.vkApi.users.get(user_ids=group_id, v=vkApiVersion)
                res = self.view.base_analysis.vkApi.users.getFollowers(user_id=res[0]['id'], count=1, v=vkApiVersion)
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
            self.users_progress['value'] = 0
            self.users_progress.update()
            offset = count * times_counter
            times_counter += 1
            print("---------НАЧАТ НОВЫЙ ЗАХОД - НОМЕР", times_counter)
            try:
                res = self.view.base_analysis.vkApi.groups.getMembers(group_id=group_id, count=count, offset=offset,
                                               v=vkApiVersion)
            except:
                try:
                    res = self.view.base_analysis.vkApi.users.get(user_ids=group_id, v=vkApiVersion)
                    res = self.view.base_analysis.vkApi.users.getFollowers(user_id=res[0]['id'], count=count, offset=offset,
                                                    v=vkApiVersion)
                except:
                    return False, 0
            print("НАЙДЕНО НА ЭТОМ ЭТАПЕ -", len(res['items']))
            users_stack = ", ".join(str(e) for e in res['items'])
            all_users = self.view.base_analysis.vkApi.users.get(user_ids=users_stack, fields='photo_id, verified, sex, '
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
            step = 100/len(all_users)
            count_collected_inner = 0
            for users in all_users:
                self.users_progress['value'] += step
                self.users_progress.update()
                # ОБРАБАТЫВАЕТ И СОХРАНЯЕТ ВСЕ ДАННЫЕ ПОЛЬЗОВАТЕЛЕЙ
                # ПОЛУЧЕННЫЕ ИЗ ГРУПП или СТРАНИЦ каналов
                # Подсказка по значениям -
                # https://vk.com/dev/objects/user_2

                if 'id' in users:
                    user_id = users['id']
                    self.view.db.mycursor.execute('SELECT user_first_name, user_last_name FROM vk_users WHERE user_id = %s',
                                     (user_id,))
                    search_user = self.view.db.mycursor.fetchall()
                    if len(search_user) > 0:
                        continue
                else:
                    continue
                # ИМЯ
                if 'first_name' in users:
                    user_first_name = str((users['first_name'][:28] + '..')) if len(users['first_name']) > 30 else \
                    users['first_name']
                else:
                    user_first_name = ""
                # ФАМИЛИЯ
                if 'last_name' in users:
                    user_last_name = str((users['last_name'][:28] + '..')) if len(users['last_name']) > 30 else users[
                        'last_name']
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
                    user_nickname = str((users['nickname'][:28] + '..')) if len(users['nickname']) > 30 else users[
                        'nickname']
                else:
                    user_nickname = ""
                # Девичья фамилия
                if 'maiden_name' in users:
                    user_maiden_name = str((users['maiden_name'][:28] + '..')) if len(users['maiden_name']) > 30 else \
                    users['maiden_name']
                else:
                    user_maiden_name = ""
                # Короткое имя страницы
                if 'screen_name' in users:
                    user_screen_name = str((users['screen_name'][:28] + '..')) if len(users['screen_name']) > 30 else \
                    users['screen_name']
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
                        try:
                            day = int(bday_array[0])
                        except ValueError:
                            day = 1
                        try:
                            month = int(bday_array[1])
                        except ValueError:
                            month = 1
                        year = 1000
                        try:
                            user_birthday = datetime.date(year=year, month=month, day=day)
                        except ValueError:
                            user_birthday = datetime.date(year=1000, month=1, day=1)
                    elif len(bday_array) == 3:
                        try:
                            day = int(bday_array[0])
                        except ValueError:
                            day = 1
                        try:
                            month = int(bday_array[1])
                        except ValueError:
                            month = 1
                        try:
                            year = int(bday_array[2])
                        except ValueError:
                            year = 1000

                        try:
                            user_birthday = datetime.date(year=year, month=month, day=day)
                        except ValueError:
                            user_birthday = datetime.date(year=1000, month=1, day=1)
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
                        user_country_id = int(users['country']['id'])
                    else:
                        user_country_id = 0
                    if 'title' in users['country']:
                        user_country_name = (users['country']['title'] + "..") if len(
                            users['country']['title']) > 50 else \
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
                # 0 если скрыто или неизвестно
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
                    user_activities = str((users['activities'][:248] + '..')) if len(users['activities']) > 250 else \
                    users['activities']
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
                    user_id, user_first_name, user_last_name, user_sex, user_nickname, user_maiden_name,
                    user_screen_name,
                    user_birthday, user_city_id, user_city_name, user_country_id, user_country_name, user_photo_link,
                    user_has_mobile, user_can_see_all_posts, user_can_see_audio, user_can_write_private_message,
                    user_can_send_friend_request, user_can_post, user_site, user_status, user_last_seen_platform,
                    user_verified, user_followers_count, user_occupation_type, user_occupation_id, user_occupation_name,
                    user_home_town, user_relation, user_personal_political, user_personal_langs,
                    user_personal_religion_id,
                    user_personal_inspired_by, user_personal_people_main, user_personal_life_main,
                    user_personal_smoking,
                    user_personal_alcohol, user_interests, user_music, user_activities, user_movies, user_tv,
                    user_books,
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
                    self.view.db.mycursor.execute(sql, values)
                    self.view.db.mydb.commit()
                    self.view.db.mycursor.execute(sql_group_subs, values_group_subs)
                    self.view.db.mydb.commit()
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
                            user_career_company = str((work['company'][:48] + '..')) if len(work['company']) > 50 else \
                            work[
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
                            user_career_position = (work['position'][:48] + '..') if len(work['position']) > 50 else \
                            work[
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
                            self.view.db.mycursor.execute(sql, values)
                            self.view.db.mydb.commit()
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
                            user_university_graduation, user_university_education_form,
                            user_university_education_status)
                        try:
                            self.view.db.mycursor.execute(sql, values)
                            self.view.db.mydb.commit()
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
                            self.view.db.mycursor.execute(sql, values)
                            self.view.db.mydb.commit()
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
                            self.view.db.mycursor.execute(sql, values)
                            self.view.db.mydb.commit()
                        except mysql.connector.Error as err:
                            print("ERROR - Ошибка базы данных!\n", err)
                            raise SystemExit
            print("РЕАЛЬНО СОБРАНО НА ЭТАПЕ -", count_collected_inner)

        return True, count_collected_users

    def videos(self, event):
        Videos(self.channel_link)

    def cluster(self, event):
        Cluster(self.channel_link)

    def comments_info(self, event):
        Comments(self.channel_link)


class Audience_Short(tk.Toplevel):
    def __init__(self, vk_address_list):
        super().__init__()
        self.iconbitmap(default='icon.ico')
        self.channel_links = vk_address_list
        self.view = app
        self.init_audience_info()

    def init_audience_info(self):
        self.title("Сводка по аудитории")
        y = str(100+len(self.channel_links)*40)
        self.geometry(f"800x{y}+200+200")
        self.resizable(True, False)
        self.grab_set()
        self.focus_set()
        self.scroll = tk.Scrollbar(self, orient="horizontal")
        self.scroll.pack(side=tk.BOTTOM, fill=tk.X)
        self.audience_info_tree = ttk.Treeview(self, xscrollcommand=self.scroll.set,
                                               columns=("vk_link", "channel_id", "men_part", "women_part", "hidden",
                                                        "to18", "f18to25", "f26to35", "f36to50", "f51to65", "f65",
                                                        "followers_av", "followers_med", "work_part", "university_part",
                                                        "school_part", "rel_free_part", "rel_friend_part",
                                                        "rel_married_part", "rel_difficult_part", "smoking_part",
                                                        "alcohol_part"),
                                               height=18, show="headings")
        self.scroll.config(command=self.audience_info_tree.xview)
        self.audience_info_tree.column("vk_link", width=60, anchor=tk.CENTER)
        self.audience_info_tree.column("channel_id", width=60, anchor=tk.CENTER)
        self.audience_info_tree.column("men_part", width=75, anchor=tk.CENTER)
        self.audience_info_tree.column("women_part", width=75, anchor=tk.CENTER)
        self.audience_info_tree.column("hidden", width=70, anchor=tk.CENTER)
        self.audience_info_tree.column("to18", width=40, anchor=tk.CENTER)
        self.audience_info_tree.column("f18to25", width=40, anchor=tk.CENTER)
        self.audience_info_tree.column("f26to35", width=40, anchor=tk.CENTER)
        self.audience_info_tree.column("f36to50", width=40, anchor=tk.CENTER)
        self.audience_info_tree.column("f51to65", width=40, anchor=tk.CENTER)
        self.audience_info_tree.column("f65", width=40, anchor=tk.CENTER)
        self.audience_info_tree.column("followers_av", width=100, anchor=tk.CENTER)
        self.audience_info_tree.column("followers_med", width=100, anchor=tk.CENTER)
        self.audience_info_tree.column("work_part", width=100, anchor=tk.CENTER)
        self.audience_info_tree.column("university_part", width=100, anchor=tk.CENTER)
        self.audience_info_tree.column("school_part", width=100, anchor=tk.CENTER)
        self.audience_info_tree.column("rel_free_part", width=100, anchor=tk.CENTER)
        self.audience_info_tree.column("rel_friend_part", width=100, anchor=tk.CENTER)
        self.audience_info_tree.column("rel_married_part", width=100, anchor=tk.CENTER)
        self.audience_info_tree.column("rel_difficult_part", width=100, anchor=tk.CENTER)
        self.audience_info_tree.column("smoking_part", width=100, anchor=tk.CENTER)
        self.audience_info_tree.column("alcohol_part", width=100, anchor=tk.CENTER)
        self.audience_info_tree.heading("vk_link", text="ВК")
        self.audience_info_tree.heading("channel_id", text="ID")
        self.audience_info_tree.heading("men_part", text="Мужчин, %")
        self.audience_info_tree.heading("women_part", text="Женщин, %")
        self.audience_info_tree.heading("hidden", text="Воз-т скрыт")
        self.audience_info_tree.heading("to18", text="< 18")
        self.audience_info_tree.heading("f18to25", text="18-25")
        self.audience_info_tree.heading("f26to35", text="26-35")
        self.audience_info_tree.heading("f36to50", text="36-50")
        self.audience_info_tree.heading("f51to65", text="51-65")
        self.audience_info_tree.heading("f65", text="> 65")
        self.audience_info_tree.heading("followers_av", text="Подписч. сред.")
        self.audience_info_tree.heading("followers_med", text="Подписч. медиан.")
        self.audience_info_tree.heading("work_part", text="Доля работ-х")
        self.audience_info_tree.heading("university_part", text="Доля студ-в")
        self.audience_info_tree.heading("school_part", text="Доля школь-в")
        self.audience_info_tree.heading("rel_free_part", text="Статус 'Свободен'")
        self.audience_info_tree.heading("rel_friend_part", text="Ст-с 'Влюблен/встреч-ся'")
        self.audience_info_tree.heading("rel_married_part", text="Ст-с 'Женат/замужем'")
        self.audience_info_tree.heading("rel_difficult_part", text="Ст-с 'Все сложно'")
        self.audience_info_tree.heading("smoking_part", text="Доля курящих")
        self.audience_info_tree.heading("alcohol_part", text="Доля пьющих")

        for i in self.channel_links:
            query = "SELECT * FROM audience_stats WHERE vk_link = %s"
            self.view.db.mycursor.execute(query, (i,))
            res = self.view.db.mycursor.fetchone()
            print(res)
            if res is None:
                self.view.base_analysis.message_popup("Для одного из выбранных каналов не собрана\nили не проанализирована аудитория")
                self.destroy()
            else:
                res2 = []
                for one in res:
                    if isinstance(one, float):
                        one = '{:.3f}'.format(one)
                        res2.append(one)
                    else:
                        res2.append(one)
                self.audience_info_tree.insert('', 'end', values=res2)

        self.audience_info_tree.pack(side=tk.LEFT, fill=tk.BOTH)


class Comments(tk.Toplevel):
    def __init__(self, channel_link):
        super().__init__()
        self.iconbitmap(default='icon.ico')
        self.channel_link = channel_link
        self.view = app
        self.init_comments_info()

    def init_comments_info(self):
        self.title("Информация по комментариям на канале")
        self.geometry("500x388+200+200")
        self.resizable(False, False)

        self.grab_set()
        self.focus_set()

        self.comments_btn_frame = tk.Frame(self, width=200, height=388, bg="#666666")
        self.comments_data_frame = tk.Frame(self, width=300, height=388)
        self.comments_btn_frame.grid(row=0, column=0)
        self.comments_data_frame.grid(row=0, column=1)

        self.comments_btn_show_average = tk.Button(self.comments_btn_frame,
                                                   text="Показать средние\nтональные оценки видео",
                                                   padx=20, justify=tk.CENTER)
        self.comments_btn_show_average.place(x=5, y=5)
        self.comments_btn_show_average.bind("<Button-1>", self.show_average)
        self.comments_btn_show_stats = tk.Button(self.comments_btn_frame, padx=3,
                                                 text="Показать основную статистику", justify=tk.CENTER)
        self.comments_btn_show_stats.place(x=5, y=55)
        self.comments_btn_show_stats.bind("<Button-1>", self.show_stats)

    def show_average(self, event):
        for child in self.comments_data_frame.winfo_children():
            child.destroy()
        self.view.db.mycursor.execute("SELECT video_id, title, sentiment FROM videos WHERE channel_id = %s", (self.channel_link,))
        self.videos = self.view.db.mycursor.fetchall()
        # Возвращать функция будет список списков [['video_id', 'video_title', 'sentiment'], []....]
        self.videos_dict = []
        for video in self.videos:
            sentiment = float("{0:.3f}".format(video[2]))
            self.videos_dict.append([video[0], video[1], sentiment])

        self.videos_dict.sort(key=lambda i: i[2], reverse=True)
        self.scroll = tk.Scrollbar(self.comments_data_frame)
        self.scroll.pack(side=tk.RIGHT, fill=tk.Y)
        self.comments_average_tree = ttk.Treeview(self.comments_data_frame, yscrollcommand=self.scroll.set,
                                               columns=('id', 'name', 'average'), height=18, show="headings")
        self.scroll.config(command=self.comments_average_tree.yview)

        self.comments_average_tree.column("id", width=20, anchor=tk.CENTER)
        self.comments_average_tree.column("name", width=160, anchor=tk.CENTER)
        self.comments_average_tree.column("average", width=100, anchor=tk.CENTER)
        self.comments_average_tree.heading("id", text="ID")
        self.comments_average_tree.heading("name", text="Название")
        self.comments_average_tree.heading("average", text="Ср. тональная\nоценка")
        for video in self.videos_dict:
            self.comments_average_tree.insert("", "end", values=(video[0], video[1], video[2]))
        self.comments_average_tree.bind("<Double-1>", self.link_tree_videos)
        self.comments_average_tree.pack(side=tk.LEFT, fill=tk.BOTH)

    def show_stats(self, event):
        for child in self.comments_data_frame.winfo_children():
            child.destroy()
        self.view.db.mycursor.execute("SELECT commentCount FROM channels WHERE channelId = %s", (self.channel_link,))
        commentCount = self.view.db.mycursor.fetchone()
        comment_count = commentCount[0]
        self.view.db.mycursor.execute("SELECT collected_comments, average_comments, average_sentiment "
                                      "FROM analysis_videos WHERE channel_id = %s", (self.channel_link,))
        stats_data = self.view.db.mycursor.fetchone()
        collected_comments = stats_data[0]
        average_comments = stats_data[1]
        average_sentiment = stats_data[2]

        self.comments_stats_tree = ttk.Treeview(self.comments_data_frame,
                                                columns=("name", "value"), height=18, show="headings")
        self.comments_stats_tree.column("name", width=190, anchor=tk.W)
        self.comments_stats_tree.column("value", width=110, anchor=tk.CENTER)
        self.comments_stats_tree.heading("name", text="Показатель")
        self.comments_stats_tree.heading("value", text="Значение")
        self.comments_stats_tree.insert("", "end", values=("Кол-во комментариев (0-скрыто)", comment_count))
        self.comments_stats_tree.insert("", "end", values=("Кол-во собранных комментариев", collected_comments))
        self.comments_stats_tree.insert("", "end", values=("В среднем коммент./видео", average_comments))
        self.comments_stats_tree.insert("", "end", values=("Средняя оценка тональности", average_sentiment))

        self.comments_stats_tree.pack(side=tk.LEFT, fill=tk.BOTH)

    def link_tree_videos(self, event):
        address = self.comments_average_tree.selection()
        address = self.comments_average_tree.item(address, "value")
        print(address)
        webbrowser.open('https://www.youtube.com/watch?v={}'.format(address[0]))


class Videos(tk.Toplevel):
    def __init__(self, channel_link):
        super().__init__()
        self.iconbitmap(default='icon.ico')
        self.channel_link = channel_link
        self.view = app
        self.init_videos_info()

    def init_videos_info(self):
        self.title("Информация по видео на канале")
        self.geometry("500x388+200+200")
        self.resizable(False, False)

        self.grab_set()
        self.focus_set()

        self.videos_btn_frame = tk.Frame(self, width=200, height=388, bg="#666666")
        self.videos_data_frame = tk.Frame(self, width=300, height=388)
        self.videos_btn_frame.grid(row=0, column=0)
        self.videos_data_frame.grid(row=0, column=1)

        self.videos_btn_show_stats = tk.Button(self.videos_btn_frame, padx=36,
                                                  text="Базовая статистика", justify=tk.CENTER)
        self.videos_btn_show_stats.place(x=5, y=5)
        self.videos_btn_show_stats.bind("<Button-1>", self.show_stats)

        self.videos_btn_show_er = tk.Button(self.videos_btn_frame, padx=64,
                                                  text="ER видео", justify=tk.CENTER)
        self.videos_btn_show_er.place(x=5, y=40)
        self.videos_btn_show_er.bind("<Button-1>", self.show_er)

    def show_stats(self, event):
        for child in self.videos_data_frame.winfo_children():
            child.destroy()
        self.view.db.mycursor.execute("SELECT videoCount FROM channels WHERE channelId = %s", (self.channel_link,))
        videoCount = self.view.db.mycursor.fetchone()
        video_count = videoCount[0]
        self.view.db.mycursor.execute("SELECT average_er, average_length, average_period "
                                      "FROM analysis_videos WHERE channel_id = %s", (self.channel_link,))
        stats_data = self.view.db.mycursor.fetchone()
        average_er = stats_data[0]
        average_length = stats_data[1]
        average_period = stats_data[2]
        self.view.db.mycursor.execute("SELECT title FROM videos WHERE channel_id = %s", (self.channel_link,))
        col_vids_count = self.view.db.mycursor.fetchall()
        col_vids_count = len(col_vids_count)
        self.videos_main_info_tree = ttk.Treeview(self.videos_data_frame, columns=("name", "value"),
                                                  height=18, show="headings")
        self.videos_main_info_tree.column("name", width=190, anchor=tk.W)
        self.videos_main_info_tree.column("value", width=110, anchor=tk.CENTER)
        self.videos_main_info_tree.heading("name", text="Показатель")
        self.videos_main_info_tree.heading("value", text="Значение")
        self.videos_main_info_tree.insert("", "end", values=("Всего видео (0-скрыто)", video_count))
        self.videos_main_info_tree.insert("", "end", values=("Собрано видео", col_vids_count))
        self.videos_main_info_tree.insert("", "end", values=("Частота появления (дня/дней)", format(average_period / 60 / 60 / 24, ",.2f")))
        self.videos_main_info_tree.insert("", "end", values=("Ср. продолж. видео (мин)", format(average_length / 60, ",.2f")))
        self.videos_main_info_tree.insert("", "end", values=("Средний ER", average_er))
        self.videos_main_info_tree.pack(side=tk.LEFT)

    def show_er(self, event):
        for child in self.videos_data_frame.winfo_children():
            child.destroy()
        self.view.db.mycursor.execute("SELECT video_id, title, engagement_rate FROM videos WHERE channel_id = %s",
                                      (self.channel_link,))
        self.videos = self.view.db.mycursor.fetchall()
        # Возвращать функция будет список списков [['video_id', 'video_title', 'sentiment'], []....]
        self.videos_dict = []
        for video in self.videos:
            sentiment = float("{0:.3f}".format(video[2]))
            self.videos_dict.append([video[0], video[1], sentiment])

        self.videos_dict.sort(key=lambda i: i[2], reverse=True)
        self.scroll = tk.Scrollbar(self.videos_data_frame)
        self.scroll.pack(side=tk.RIGHT, fill=tk.Y)
        self.videos_er_tree = ttk.Treeview(self.videos_data_frame, yscrollcommand=self.scroll.set,
                                           columns=("id", "name", "er"),
                                           height=18, show="headings")
        self.videos_er_tree.column("id", width=20, anchor=tk.W)
        self.videos_er_tree.column("name", width=210, anchor=tk.CENTER)
        self.videos_er_tree.column("er", width=50, anchor=tk.CENTER)
        self.videos_er_tree.heading("id", text="ID")
        self.videos_er_tree.heading("name", text="Название")
        self.videos_er_tree.heading("er", text="ER")
        for video in self.videos_dict:
            self.videos_er_tree.insert("", "end", values=(video[0], video[1], video[2]))
        self.scroll.config(command=self.videos_er_tree.yview)
        self.videos_er_tree.pack(side=tk.LEFT)


class Cluster(tk.Toplevel):
    def __init__(self, channel_link):
        super().__init__()
        self.iconbitmap(default='icon.ico')
        self.channel_link = channel_link
        self.view = app
        self.init_clustering()

    def init_clustering(self):
        self.title("Сегментирование аудитории")
        self.geometry("900x388+200+200")
        self.resizable(False, False)
        self.grab_set()
        self.focus_set()

        self.clustering_left = tk.Frame(self, width=200, height=388, bg="#666666")
        self.clustering_right = tk.Frame(self, width=700, height=388)
        self.clustering_left.grid(row=0, column=0)
        self.clustering_right.grid(row=0, column=1)

        '''-----Cluster Left-----'''
        self.clustering_left_lbl = tk.Label(self.clustering_left, fg="white", bg="#666666",
                                            text="Введите кол-во кластеров\nи нажмите ввод", justify=tk.LEFT)
        self.clustering_left_lbl.place(x=5, y=5)
        self.num_clusters = tk.StringVar()
        self.clustering_left_entry = tk.Entry(self.clustering_left, textvariable=self.num_clusters, width=18, bd=2)
        self.clustering_left_entry.place(x=5, y=50)
        self.clustering_left_entry_btn = tk.Button(self.clustering_left, bd=0, width=8, height=1, pady=0, bg="#ffe040", text="Ввод")
        self.clustering_left_entry_btn.place(x=125, y=50)
        self.clustering_left_entry_btn.bind("<Button-1>", self.init_clustering_proccess)

        '''-----Cluster Right-----'''

    def init_clustering_proccess(self, event):
        self.q = int(self.num_clusters.get())
        if self.q < 1 or self.q > 20:
            self.view.base_analysis.error_popup("Введите значение от 1 до 19")
            return
        clust_result = self.clustering()
        if clust_result == False:
            self.destroy()
        else:
            self.create_clustering_results()

    def clustering(self):
        """
        Метод для кластерного анализа пользователей ВК
        K-means

        Выход:
            Массив переменных типа pandas.DataFrame (содержимого кластеров)
            [Df_3_clusters, Df_4_clusters ... ]
        """
        self.view.db.mycursor.execute("SELECT relatedVkGroup FROM channels WHERE channelId = %s", (self.channel_link,))
        self.vk_group = self.view.db.mycursor.fetchone()
        self.vk_group = self.vk_group[0]
        print(self.vk_group)
        self.limit = None
        if self.limit is not None:
            query = f"""SELECT user_birthday, user_sex, user_followers_count, user_relation, 
                               user_occupation_id, user_occupation_name, user_personal_smoking, user_personal_alcohol, user_id 
                               FROM vk_users WHERE user_id in 
                               (SELECT user_id FROM vk_page_subs WHERE vk_page = %s) LIMIT {self.limit}"""
        else:
            query = """SELECT user_birthday, user_sex, user_followers_count, user_relation, 
                               user_occupation_id, user_occupation_name, user_personal_smoking, user_personal_alcohol, user_id 
                               FROM vk_users WHERE user_id in 
                               (SELECT user_id FROM vk_page_subs WHERE vk_page = %s)"""
        self.view.db.mycursor.execute(query, (self.vk_group,))
        subscribers = self.view.db.mycursor.fetchall()
        if len(subscribers) < 100:
            self.view.base_analysis.error_popup("Анализ не может быть проведен!\n"
                                                "Данные о пользователях не собраны, или их слишком мало (меньше 100)")
            return False
        users_dict = pd.DataFrame(
            {'sex': [], 'age': [], 'age_skipped': [], 'followers': [], 'followers_skipped': [], 'relation': [],
             'occupation': [], 'children_count': [], 'bro_count': []})
        users_list_sex = []
        users_list_age = []
        users_list_age_skipped = []
        users_list_followers = []
        users_list_followers_skipped = []
        users_list_relation = []
        users_list_occupation = []
        users_list_children_count = []
        users_list_bro_count = []
        users_list_smoking = []
        users_list_alcohol = []
        for subscriber in subscribers:
            """ ПЕРЕМЕННЫЕ, собираемые для анализа
            # пол - 1-Ж, 2-М, 0- не указан
            # возраст - число лет, если не указан - 0Б
            # возраст, пропуск 1-пропущено, 0-не пропущено
            # Подписчики - от 1, если скрыто или нет - 0 (пропущено)
            # Подписчики пропущено - если пропущены подписчики - 1, если нет - 0
            # семейное положение -
            #       1 — не женат/не замужем;
            #       2 — есть друг/есть подруга;
            #       3 — помолвлен/помолвлена;
            #       4 — женат/замужем;
            #       5 — всё сложно;
            #       6 — в активном поиске;
            #       7 — влюблён/влюблена;
            #       8 — в гражданском браке;
            #       0 — не указано.
            # Работает - 1-да, 0 - не указано
            # Сколько детей - количество, не указано - 0
            # Сколько братьев и сестер
            # Отношение к курению
            # Отношение к алкоголю
            """
            user_sex = subscriber[1]
            if subscriber[0].year == 1000:
                user_age = 0
                user_age_skipped = 1
            else:
                user_age = datetime.datetime.now().year - subscriber[0].year
                user_age_skipped = 0
            user_followers = subscriber[2]
            user_followers_skipped = 0
            if user_followers == 0:
                user_followers_skipped = 1
            user_relation = subscriber[3]
            if subscriber[4] != 0 or subscriber[5] != "":
                user_occupation = 1
            else:
                user_occupation = 0
            children_count = 0
            bro_count = 0
            user_smoking = subscriber[6]
            user_alcohol = subscriber[7]
            query = "SELECT relative_type FROM vk_users_relatives WHERE user_id = %s"
            values = (subscriber[8],)
            self.view.db.mycursor.execute(query, values)
            relatives = self.view.db.mycursor.fetchall()
            for relative in relatives:
                if relative[0] == "child":
                    children_count += 1
                elif relative[0] == "sibling":
                    bro_count += 1

            users_list_sex.append(user_sex)
            users_list_age.append(user_age)
            users_list_age_skipped.append(user_age_skipped)
            users_list_followers.append(user_followers)
            users_list_followers_skipped.append(user_followers_skipped)
            users_list_relation.append(user_relation)
            users_list_occupation.append(user_occupation)
            users_list_children_count.append(children_count)
            users_list_bro_count.append(bro_count)
            users_list_smoking.append(user_smoking)
            users_list_alcohol.append(user_alcohol)

        users_dict['sex'] = users_list_sex
        users_dict['age'] = users_list_age
        users_dict['age_skipped'] = users_list_age_skipped
        users_dict['followers'] = users_list_followers
        users_dict['followers_skipped'] = users_list_followers_skipped
        users_dict['relation'] = users_list_relation
        users_dict['occupation'] = users_list_occupation
        users_dict['children_count'] = users_list_children_count
        users_dict['bro_count'] = users_list_bro_count
        users_dict['smoking'] = users_list_smoking
        users_dict['alcohol'] = users_list_alcohol

        data_set = np.nan_to_num(users_dict)
        normalized_data_set = StandardScaler().fit_transform(data_set)
        separation = []

        k_means = KMeans(init="k-means++", n_clusters=self.q, n_init=12)
        k_means.fit(normalized_data_set)
        labels = k_means.labels_
        separation.append(labels)

        for clustering_result in separation:
            users_dict['cluster'] = clustering_result
            self.users_dict = users_dict


        self.average_age = format(sum(users_list_age) / len(users_list_age), ".2f")
        print("Средний возраст аудитории - ", self.average_age)
        self.view.db.mydb.commit()

        return True

    def create_clustering_results(self):
        try:
            self.users_dict
        except NameError:
            return
        print(self.users_dict)
        self.users_in_cluster_lbl = tk.Label(self.clustering_left, text="Распределение по кластерам",
                                             fg="white", bg="#666666")
        self.users_in_cluster_lbl.place(x=5, y=90)
        self.users_in_cluster_text = tk.Text(self.clustering_left, width=23, height=10)
        self.users_in_cluster_text.place(x=5, y=110)
        self.means_in_cluster_lbl = tk.Label(self.clustering_right, text="Портреты кластеров по заданным показателям")
        self.means_in_cluster_lbl.place(x=10, y=5)
        self.means_in_cluster_lbl2 = tk.Label(self.clustering_right, text="Параметры установлены заранее")
        self.means_in_cluster_lbl2.place(x=10, y=25)
        '''
        self.means_in_cluster_tree = ttk.Treeview(self.clustering_right,
                                                  columns=('sex', 'age', 'age_skipped', 'followers',
                                                           'followers_skipped', 'relation', 'occupation',
                                                           'children_count', 'bro_count', 'smoking', 'alcohol'),
                                                  show="headings")
        self.means_in_cluster_tree.column("sex", width=30, anchor=tk.CENTER)
        self.means_in_cluster_tree.column("age", width=50, anchor=tk.CENTER)
        self.means_in_cluster_tree.column("age_skipped", width=60, anchor=tk.CENTER)
        self.means_in_cluster_tree.column("followers", width=60, anchor=tk.CENTER)
        self.means_in_cluster_tree.column("followers_skipped", width=70, anchor=tk.CENTER)
        self.means_in_cluster_tree.column("relation", width=80, anchor=tk.CENTER)
        self.means_in_cluster_tree.column("occupation", width=60, anchor=tk.CENTER)
        self.means_in_cluster_tree.column("children_count", width=60, anchor=tk.CENTER)
        self.means_in_cluster_tree.column("bro_count", width=80, anchor=tk.CENTER)
        self.means_in_cluster_tree.column("smoking", width=60, anchor=tk.CENTER)
        self.means_in_cluster_tree.column("alcohol", width=60, anchor=tk.CENTER)
        self.means_in_cluster_tree.heading("sex", text="Пол")
        self.means_in_cluster_tree.heading("age", text="Возраст")
        self.means_in_cluster_tree.heading("age_skipped", text="Проп.\nвозраст")
        self.means_in_cluster_tree.heading("followers", text="Подписчики")
        self.means_in_cluster_tree.heading("followers_skipped", text="Проп.\nподписчики")
        self.means_in_cluster_tree.heading("relation", text="Семейное\nположение")
        self.means_in_cluster_tree.heading("occupation", text="Занятость")
        self.means_in_cluster_tree.heading("children_count", text="Кол-во\nдетей")
        self.means_in_cluster_tree.heading("bro_count", text="Братья/сестры")
        self.means_in_cluster_tree.heading("smoking", text="Кур-е")
        self.means_in_cluster_tree.heading("alcohol", text="Алк-ль")
        '''

        self.users_in_cluster_text.delete(1.0, tk.END)
        self.users_in_cluster_text.insert(1.0, self.users_dict['cluster'].value_counts())

        self.users_dict_grouped = self.users_dict.groupby('cluster').mean()
        #self.means_in_cluster_tree.place(x=10, y=50)
        self.clusters_mean = tk.Text(self.clustering_right, width=90, height=25, font=("Verdana", 8))
        self.clusters_mean.place(x=10, y=50)
        self.clusters_mean.delete(1.0, tk.END)
        self.clusters_mean.insert(1.0, self.users_dict_grouped)


class DB:
    def __init__(self):
        try:
            self.mydb = mysql.connector.connect(host=host, user=user, passwd=password, database=database)
            self.mycursor = self.mydb.cursor()
        except mysql.connector.Error as err:
            print("Ошибка подключения:", err)
            raise SystemExit
        with open('sql_queries.txt', 'r', encoding='utf-8') as self.f:
            self.main_sql_queries = self.f.read().splitlines()
        for self.query in self.main_sql_queries:
            try:
                self.mycursor.execute(self.query)
                self.mydb.commit()
            except mysql.connector.Error as err:
                print("Ошибка при добавлении базовых таблиц -", err)
                raise SystemExit

    def if_exist_videos_for_channel(self, channel_id):
        self.mycursor.execute("SELECT title FROM videos WHERE channel_id = %s", (channel_id,))
        videos = self.mycursor.fetchall()
        return len(videos)


# noinspection PyTypeChecker
class Channel:
    def __init__(self):
        super().__init__()
        self.db = db
        self.localisationCheck = 0.0
        self.base_analysis = Base_analysis()
        self.yt = YouTubeDataAPI(googleApiKey)
        '''Для работы с нейросетью'''
        self.vocab_size = 5000
        self.vocab = []
        self.token_to_idx = []
        self.tokenizer = TweetTokenizer()
        self.stemmer = RussianStemmer()
        self.regex = re.compile('[^а-яА-Я ]')
        self.stem_cache = {}
        self.model = load_model("model/model.h5")
        with open('model/vocab.json') as self.json_file:
            self.data = json.load(self.json_file)
            for stem in self.data:
                self.vocab.append(stem)
        self.token_to_idx = {self.vocab[i]: i for i in range(self.vocab_size)}
        '''/для работы с нейросетью'''

    def get_stem(self, token):
        stem = self.stem_cache.get(token, None)
        if stem:
            return stem
        token = self.regex.sub('', token).lower()
        stem = self.stemmer.stem(token)
        self.stem_cache[token] = stem
        return stem

    # Функция преобразования комментария в вектор размерностью 5000 стем, для предсказаний по модели
    def tweet_to_vector(self, tweet, show_unknowns=False):
        vector = np.zeros(self.vocab_size, dtype=np.int_)
        for token in self.tokenizer.tokenize(tweet):
            stem = self.get_stem(token)
            idx = self.token_to_idx.get(stem, None)
            if idx is not None:
                vector[idx] = 1
            elif show_unknowns:
                print("Неизвестное слово: ", token)
        return vector

    def channel_add(self, channel_link):
        self.channel_link = channel_link
        channelURL = "https://www.youtube.com/" + channel_link
        data = self.return_channel_data()
        if 'totalResults' in data['pageInfo'] and data['pageInfo']['totalResults'] == 0:
            data = "wrong"
        if data == "wrong":
            self.base_analysis.error_popup("Канал с таким ID не найден!")
            print('--ERROR----ERROR----ERROR--')
            print('Канал с таким ID не найден')
            print('ID - ', self.channel_link)
            print('--ERROR----ERROR----ERROR--')
            return
        if "customUrl" in data["items"][0]["snippet"]:
            customUrl = data["items"][0]["snippet"]["customUrl"]
        else:
            customUrl = ""
        channelId = data['items'][0]['id']
        title = data["items"][0]["snippet"]["title"]
        print("Пытаюсь добавить - ", title)

        publishedAt = self.base_analysis.date_to_format(str(data["items"][0]["snippet"]["publishedAt"]))
        if "country" in data["items"][0]["snippet"]:
            country = data["items"][0]["snippet"]["country"]
        else:
            country = ""
        if country == "RU":
            self.localisationCheck += 1.0
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
        if 'image' in data["items"][0]['brandingSettings'] and 'bannerImageUrl' in data["items"][0]['brandingSettings'][
            'image']:
            bannerImage = data["items"][0]['brandingSettings']['image']['bannerImageUrl']
        else:
            bannerImage = ""

        addDateTime = self.base_analysis.date_to_format(str(datetime.datetime.now()))
        lastEditTime = addDateTime
        localisationStatus = self.localisationCheck
        subscriberCount = int(subscriberCount)
        # Проверка канала на существование в базе
        query = 'SELECT title FROM channels WHERE channelId = %s'
        value = (channelId,)
        self.db.mycursor.execute(query, value)
        result = self.db.mycursor.fetchall()
        if len(result) != 0:
            self.base_analysis.error_popup(f"Канал уже существует в базе данных - {title}")
            print("Канал уже существует в базе данных - ", title)
            print("----------------------------------")
            return
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
                if self.base_analysis.has_cyrillic(description):
                    self.localisationCheck += 1.0
                # Поиск всех ссылок на группы ВК, профили ВК, Instagram, Email, Facebook
                foundDomains = self.base_analysis.search_all_links(description, channelURL)
                # Если есть группы или страницы ВК, канал русский (подходит по локализации)
                if len(foundDomains['vkGroups']) != 0 or len(foundDomains['vkElse']) != 0:
                    self.localisationCheck += 1.0
            else:
                #   Даже если описание пусто, вызываем поиск ссылок, он спарсит то, что есть
                # на страничке About
                description = ""
                foundDomains = self.base_analysis.search_all_links(description, channelURL)

            relatedVkGroup = ""
            relatedInstagramPage = ""
            relatedPromotionPage = ""
            relatedOther = ""
            if foundDomains:
                # ВЫБИРАЕМ ЛУЧШУЮ ГРУППУ ДЛЯ АНАЛИЗА ВК
                foundDomains = self.base_analysis.sort_found_domains(foundDomains)

                relatedVkGroup = str(foundDomains['vkGroups'])
                relatedInstagramPage = str(foundDomains['inst'])
                relatedPromotionPage = str(foundDomains['emails'])
                relatedOther = str(foundDomains['vkElse'] + foundDomains['other'])

            if self.localisationCheck < 1:
                self.base_analysis.error_popup(f'Канал не подходит по региону/локализации - {title}')
                print("ВНИМАНИЕ: канал не подходит по региону/локализации (Страна: ", country,
                      ", кириллица в описании: ", self.base_analysis.has_cyrillic(description), ")", sep="")
                return

            if "showRelatedChannels" in data["items"][0]['brandingSettings']['channel']:
                if data["items"][0]['brandingSettings']['channel']['showRelatedChannels']:
                    showRelatedChannels = 1
                    if 'featuredChannelsUrls' in data['items'][0]['brandingSettings']['channel']:
                        featuredChannels = data['items'][0]['brandingSettings']['channel']['featuredChannelsUrls']
                        counter = 0
                        for i in featuredChannels:
                            query = "SELECT * FROM channels WHERE channelId = %s"
                            value = (i,)
                            self.db.mycursor.execute(query, value)
                            result = self.db.mycursor.fetchall()
                            if len(result) == 0:
                                query = "SELECT * FROM channelsToGo WHERE channelId = %s"
                                value = (i,)
                                self.db.mycursor.execute(query, value)
                                result = self.db.mycursor.fetchall()
                                if len(result) == 0:
                                    query = "INSERT INTO channelsToGo (channelId, leadingChannelId, status) VALUES (%s, %s, %s)"
                                    values = (i, channelId, "TO ADD")
                                    self.db.mycursor.execute(query, values)
                                    self.db.mydb.commit()
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
                      commentCount, subscriberCount, hiddenSubscriberCount, videoCount, privacyStatus,
                      showRelatedChannels,
                      bannerImage, addDateTime, localisationStatus, lastEditTime)
            try:
                self.db.mycursor.execute(query, values)
                self.db.mydb.commit()
                print("Канал был добавлен - ", title)
                return channelId
            except mysql.connector.Error as err:
                self.base_analysis.error_popup(f'Не удалось добавить канал - {title}\n{err}')
                print("ERROR - не удалось добавить канал!!!\n", err)
                raise SystemExit

    def return_id(self):
        return self.channel_link.rsplit('/', 1)[-1]

    def return_channel_data(self):
        channel_info = self.channel_link.split('/')
        channel_id = channel_info[1]
        channel_type = channel_info[0]
        if channel_type == "user":
            url = f"https://www.googleapis.com/youtube/v3/channels?part=snippet,contentDetails," \
                  f"statistics,status,brandingSettings,contentOwnerDetails&forUsername={channel_id}" \
                  f"&key={googleApiKey} "
        else:
            url = f"https://www.googleapis.com/youtube/v3/channels?part=snippet,contentDetails," \
                  f"statistics,status,brandingSettings,contentOwnerDetails&id={channel_id}" \
                  f"&key={googleApiKey}"

        json_url = urllib.request.urlopen(url)
        data = json.loads(json_url.read())
        return data

    def return_base_stats(self, channel_id):
        query = "SELECT channelId, title, rating, relatedVkGroup, " \
                "relatedInstagramPage, relatedPromotionPage, publishedAt, viewCount, commentCount, subscriberCount, " \
                "videoCount, er FROM channels WHERE channelId = %s"
        self.db.mycursor.execute(query, (channel_id,))
        channel_data = []
        tmp_data = self.db.mycursor.fetchone()
        for i in tmp_data:
            channel_data.append(i)
        return channel_data

    def check_if_video_exists_in_table(self, video_id: str, table: str):
        if table == 'videos_ids':
            query = "SELECT id FROM videos_ids WHERE video_id = %s"
        elif table == 'videos':
            query = "SELECT title FROM videos WHERE video_id = %s"
        else:
            # ERROR, точно
            query = "Nope."
        values = (video_id,)
        self.db.mycursor.execute(query, values)
        result = self.db.mycursor.fetchall()
        if len(result) > 0:
            return True
        else:
            return False

    def insert_in_videos_ids(self, channel_id: str, video_id: str, add_date: datetime):
        query = "INSERT INTO videos_ids (channel_id, video_id, add_date) VALUES (%s, %s, %s)"
        values = (channel_id, video_id, add_date)
        try:
            self.db.mycursor.execute(query, values)
            self.db.mydb.commit()
            return True
        except:
            return False

    def fetch_all_comments_from_video_by_id(self, video_id: str):
        comments = self.yt.get_video_comments(video_id=video_id,
                                              get_replies=False,
                                              max_results=100,
                                              next_page_token=False,
                                              part=['snippet'])
        counter = 0
        sentiment = 0
        sentiments_list = []
        for i in comments:
            comment_author = i['commenter_channel_id']
            text = i['text']

            # Определение тональности комментария
            # Преобразуем комментарий в векторное представление
            processed_comment = self.tweet_to_vector(text)
            # Изменяем размерность вектора чтобы поместился в модель
            processed_comment = processed_comment.reshape(1, 5000)
            # Предсказание
            prediction = self.model.predict(processed_comment)
            sentiment = float(prediction[0][0])
            sentiments_list.append(sentiment)
            comment_like_count = int(i['comment_like_count'])
            comment_publish_date = i['comment_publish_date']
            comment_publish_date = self.base_analysis.date_to_format(str(comment_publish_date))
            if comment_publish_date == "None":
                comment_publish_date = datetime.datetime.now()

            values = (video_id, comment_author, text, sentiment, comment_like_count, comment_publish_date)
            self.db.mycursor.execute(
                'INSERT INTO comments (video_id, comment_author, text, sentiment, comment_like_count, comment_publish_date) '
                'VALUES (%s, %s, %s, %s, %s, %s)', values)
            self.db.mydb.commit()
            counter += 1
        return counter, sentiments_list

    def collect_video_info(self, video_id: str):
        try:
            video = self.yt.get_video_metadata(video_id, parser=None, part=['contentDetails',
                                                                            'recordingDetails', 'status', 'snippet',
                                                                            'statistics',
                                                                            'topicDetails'])
        except:
            print("Не удалось получить данные о видео")
            return False

        channel_id = video['snippet']['channelId']
        video_id = video['id']
        published_at = self.base_analysis.date_to_format(video['snippet']['publishedAt'])
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
            duration = self.base_analysis.duration_decoder(duration)
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
                i = self.base_analysis.return_id(i)
                category_name = i.replace("_", " ")
                topic_categories.append(category_name)
        comments_result, sentiments = self.fetch_all_comments_from_video_by_id(video_id)
        average_sentiment = float(sum(sentiments) / len(sentiments))
        # Если хоть одно значение равно нулю, то это значение скрыто и калькуляция не может быть проведена
        if like_count == 0 or dislike_count == 0 or view_count == 0:
            engagement_rate = 0.0
        else:
            # Кодировка оценки, чтобы позитивные и негативные имели влияние,
            # а нейтральные - нет
            if average_sentiment > 0.6:
                calc_sentiment = 2
            elif average_sentiment < 0.4:
                calc_sentiment = 0
            else:
                calc_sentiment = 1
            engagement_rate = (int(like_count) - (2 * int(dislike_count)) + (4 * int(comment_count) * int(calc_sentiment))) / int(view_count)
        query = "INSERT INTO videos (channel_id, video_id, published_at, title, description, image_url, " \
                "live_broadcast, category_id, default_language, duration, dimension, definition, caption, " \
                "licensed_content, projection, privacy_status, embeddable, view_count, like_count, dislike_count, " \
                "favorite_count, comment_count, topic_categories, sentiment, engagement_rate)" \
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        values = (channel_id, video_id, published_at, title, description, image_url, live_broadcast, category_id,
                  default_language, duration, dimension, definition, caption, licensed_content, projection,
                  privacyStatus,
                  embeddable, view_count, like_count, dislike_count, favorite_count, comment_count,
                  str(topic_categories), average_sentiment, engagement_rate)

        try:
            self.db.mycursor.execute(query, values)
            self.db.mydb.commit()
            print("-->Видео (", title, ") добавлено!", sep="")
            return True
        except mysql.connector.Error as err:
            print(err)
            print("Ошибка! Не удалось добавить видео")
            return False

    def get_videos_for_table(self, channel_id):
        results = []
        self.db.mycursor.execute("SELECT video_id, title, view_count FROM videos "
                                 "WHERE channel_id = %s ORDER BY published_at DESC", (channel_id,))
        res = self.db.mycursor.fetchall()
        return res

    def update_channels_last_edit(self, channel_id: str):
        query = "UPDATE channels SET lastEditTime = %s WHERE channelId = %s"
        time = self.base_analysis.date_to_format(str(datetime.datetime.now()))
        values = (time, channel_id)
        try:
            self.db.mycursor.execute(query, values)
        except mysql.connector.Error as err:
            self.base_analysis.error_popup(err)
            print(err)


class Base_analysis:
    def __init__(self):
        super().__init__()
        self.vk_session = vk.Session(access_token=vkToken)
        self.vkApi = vk.API(self.vk_session)
        self.db = DB()

    def error_popup(self, text):
        messagebox.showerror('Возникла ошибка!', text)

    def message_popup(self, text):
        messagebox.showinfo('Информация', text)

    def date_to_format(self, date):
        date = date[0: 19]
        date = date.replace("T", " ")
        return date

    def duration_decoder(self, d: str):
        '''
        Функуия учитывает только часы, минуты и секунды, видео длинной в дни учиттываться не будут
        :param d: длительность в формате ИСО 8601
        :return: Итоговая длительность в секундах
        '''
        resH = re.search(r'P.*T(?P<Hours>\d{1,2})H', d)
        if resH is not None:
            hours = resH.group('Hours')
        else:
            hours = 0

        resM = re.search(r'P.*[T,H](?P<Minutes>\d{1,2})M', d)
        if resM is not None:
            min = resM.group('Minutes')
        else:
            min = 0

        resS = re.search(r'P.+[M,H](?P<Seconds>\d{1,2})S', d)
        if resS is not None:
            sec = resS.group('Seconds')
        else:
            sec = 0
        total_duration_seconds = int(sec) + int(min) * 60 + int(hours) * 60 * 60
        return total_duration_seconds

    def has_cyrillic(self, text):
        return bool(re.search('[а-яА-Я]', text))

    def return_id(self, url):
        return url.rsplit('/', 1)[-1]

    def search_domains(self, text, doms):
        pat = r'(https?://[^./\r\n]*?\b(?:{})\b[^\r\n\s,:;]*)'.format('|'.join(doms))
        return re.findall(pat, text)

    def search_emails(self, desc: str):
        pat = r'[a-zA-Z0-9][a-zA-Z0-9\._-]*@[a-zA-Z0-9\.-]+\.[a-z]{2,6}'
        return re.findall(pat, desc)

    def parse_about_page(self, url: str, domain: str):
        url = url + "/about"
        r = requests.get(url)
        linkContent = str(r.content)
        pattern = domain + "\.com%2F[a-zA-Z0-9\._-]+"
        resultList = re.findall(pattern, linkContent)
        finalResultList = []
        if len(resultList) > 0:
            for i in resultList:
                pattern = domain + "\.com%2F"
                i = re.sub(pattern, "", i)
                if i and i not in finalResultList:
                    finalResultList.append(i)
            return finalResultList

    def search_all_links(self, description: str, channelURL: str):
        foundDomains = dict(vkGroups=[], vkElse=[], inst=[], emails=[], other=[])
        if description:
            print("---Поиск ссылок по описанию...")
            # ПОИСК ПО ОПИСАНИЮ ВК
            searchVkDomains = self.search_domains(str(description), ["vk", "vkontakte"])
            for i in searchVkDomains:
                i = self.return_id(i).lower()
                i = "https://vk.com/" + i
                if i not in foundDomains['vkGroups'] and i not in foundDomains['vkElse']:
                    foundDomains['vkElse'].append(i)

            # ПОИСК ПО ОПИСНАИЮ INSTAGRAM
            searchInstDomains = self.search_domains(str(description), ["instagram"])
            for i in searchInstDomains:
                i = self.return_id(i).lower()
                i = "https://instagram.com/" + i
                if i not in foundDomains['inst']:
                    foundDomains["inst"].append(i)

            # ПОИСК ПО ОПИСАНИЮ EMAIL
            searchEmailss = self.search_emails(description)
            for i in searchEmailss:
                if i not in foundDomains['emails']:
                    i = i.lower()
                    foundDomains['emails'].append(i)
        else:
            print("---Описание недоступно для поиска ссылок")
        print('---Поиск ссылок по странице About...')
        resParseAboutVk = self.parse_about_page(channelURL, "vk")
        resParseAboutInst = self.parse_about_page(channelURL, "instagram")
        resParseAboutFacebook = self.parse_about_page(channelURL, 'fb|facebook')
        if resParseAboutVk:
            for i in resParseAboutVk:
                i = "https://vk.com/" + i
                if i not in foundDomains['vkGroups'] and i not in foundDomains['vkElse']:
                    foundDomains['vkElse'].append(i)
        if resParseAboutInst:
            for i in resParseAboutInst:
                i = "https://instagram.com/" + i
                if i not in foundDomains['inst']:
                    foundDomains['inst'].append(i)
        if resParseAboutFacebook:
            for i in resParseAboutFacebook:
                i = "https://facebook.com/" + i
                if i not in foundDomains['other']:
                    foundDomains['other'].append(i)

        print("------Все найденные ссылки: ", foundDomains)
        return foundDomains

    def sort_found_domains(self, foundDomains: dict):
        links = {}
        if foundDomains['vkElse']:
            for i in foundDomains['vkElse']:
                urlId = self.return_id(i)
                try:
                    res = self.vkApi.groups.getMembers(group_id=urlId, v=vkApiVersion)
                    print("------Количество подписчиков в группе ВК (", i, ") - ", res['count'], sep="")
                    links[urlId] = res['count']
                except:
                    try:
                        res = self.vkApi.users.get(user_ids=urlId, v=vkApiVersion)
                        res = self.vkApi.users.getFollowers(user_id=res[0]['id'], v=vkApiVersion)
                        print("------Количество подписчиков на странице (https://vk.com/", urlId, ") - ", res['count'],
                              sep="")
                        links[urlId] = res['count']
                    except:
                        print('------Ссылка на ВК не является ни группой, ни профилем -', urlId)
        if len(links) > 0:
            listDict = list(links.items())
            listDict.sort(key=lambda i: i[1])
            targetVkGroup = "https://vk.com/" + listDict[-1][0]
            foundDomains['vkGroups'] = targetVkGroup
            if targetVkGroup in foundDomains['vkElse']:
                foundDomains['vkElse'].remove(targetVkGroup)
        return foundDomains


if __name__ == "__main__":
    root = tk.Tk()
    db = DB()
    pd.set_option('display.max_columns', None)
    app = Main(root)
    app.pack()
    root.iconbitmap(default='icon.ico')
    root.title("AdEngine - V1.0")
    root.geometry("720x430+100+100")
    root.resizable(False, False)
    root.mainloop()
