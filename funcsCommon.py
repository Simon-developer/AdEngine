import urllib
import re
import json
import urllib.request
import requests
import vk
from config import *
import time
import sys
import mysql.connector


mydb = mysql.connector.connect(
    host=host,
    user=user,
    passwd=password,
    database=database
)

mycursor = mydb.cursor()


def update_progress(progress):
    barLength = 20 # Modify this to change the length of the progress bar
    status = ""
    if isinstance(progress, int):
        progress = float(progress)
    if not isinstance(progress, float):
        progress = 0
        status = "Ошибка: Необходим формат float\r\n"
    if progress < 0:
        progress = 0
        status = "Halt...\r\n"
    if progress >= 1:
        progress = 1
        status = "Готово...\r\n"
    block = int(round(barLength*progress))
    text = "\r---Прогресс: [{0}] {1}% {2}".format("$"*block + "-"*(barLength-block), progress*100, status)
    sys.stdout.write(text)
    sys.stdout.flush()


def parseAboutPage(url: str, domain: str):
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


def returnYTApiUrl(url: str, type: str):
    channelId = url.rsplit('/', 1)[-1]
    channelType = url.rsplit('/')[-2]
    if type == "channel":
        if channelType == "user":
            url = f"https://www.googleapis.com/youtube/v3/channels?part=snippet,contentDetails," \
                  f"statistics,topicDetails,status,brandingSettings,contentOwnerDetails&forUsername={channelId}&key=" \
                  f"{googleApiKey} "
        elif channelType == "channel":
            url = f"https://www.googleapis.com/youtube/v3/channels?part=snippet,contentDetails," \
                  f"statistics,topicDetails,status,brandingSettings,contentOwnerDetails&id={channelId}&key={googleApiKey}"
        return url



def returnId(url: str):
    return url.rsplit('/', 1)[-1]


def ytChannelStatsGet(url: str):
    jsonUrl = urllib.request.urlopen(url)
    data = json.loads(jsonUrl.read())
    return data


def dateToFormat(date: str):
    date = date[0: 19]
    date = date.replace("T", " ")
    return date


def duration_decoder(d:str):
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
    total_duration_seconds: Int = int(sec) + int(min) * 60 + int(hours) * 60 * 60
    return total_duration_seconds



def hasCyrillic(text: str):
    return bool(re.search('[а-яА-Я]', text))


def searchDomains(text, doms):
    pat = r'(https?://[^./\r\n]*?\b(?:{})\b[^\r\n\s,:;]*)'.format('|'.join(doms))
    return re.findall(pat, text)


def searchEmails(desc: str):
    pat = r'[a-zA-Z0-9][a-zA-Z0-9\._-]*@[a-zA-Z0-9\.-]+\.[a-z]{2,6}'
    return re.findall(pat, desc)


def searchAllUrls(desc: str):
    pat = r'(https?://)?[a-zA-Z0-9]+'
    return re.findall(pat, desc)


def searchAllLinks(description: str, channelURL: str):
    foundDomains = dict(vkGroups=[], vkElse=[], inst=[], emails=[], other=[])
    if description:
        print("---Поиск ссылок по описанию...")
        # ПОИСК ПО ОПИСАНИЮ ВК
        searchVkDomains = searchDomains(str(description), ["vk", "vkontakte"])
        for i in searchVkDomains:
            i = returnId(i).lower()
            i = "https://vk.com/" + i
            if i not in foundDomains['vkGroups'] and i not in foundDomains['vkElse']:
                foundDomains['vkElse'].append(i)

        # ПОИСК ПО ОПИСНАИЮ INSTAGRAM
        searchInstDomains = searchDomains(str(description), ["instagram"])
        for i in searchInstDomains:
            i = returnId(i).lower()
            i = "https://instagram.com/" + i
            if i not in foundDomains['inst']:
                foundDomains["inst"].append(i)

        # ПОИСК ПО ОПИСАНИЮ EMAIL
        searchEmailss = searchEmails(description)
        for i in searchEmailss:
            if i not in foundDomains['emails']:
                i = i.lower()
                foundDomains['emails'].append(i)
    else:
        print("---Описание недоступно для поиска ссылок")
    print('---Поиск ссылок по странице About...')
    resParseAboutVk = parseAboutPage(channelURL, "vk")
    resParseAboutInst = parseAboutPage(channelURL, "instagram")
    resParseAboutFacebook = parseAboutPage(channelURL, 'fb|facebook')
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


def sortFoundDomains(foundDomains: dict):
    session = vk.Session(access_token=vkToken)
    vkApi = vk.API(session)
    links = {}
    if foundDomains['vkElse']:
        for i in foundDomains['vkElse']:
            urlId = returnId(i)
            try:
                res = vkApi.groups.getMembers(group_id=urlId, v=vkApiVersion)
                print("------Количество подписчиков в группе ВК (", i, ") - ", res['count'], sep="")
                links[urlId] = res['count']
            except:
                try:
                    res = vkApi.users.get(user_ids=urlId, v=vkApiVersion)
                    res = vkApi.users.getFollowers(user_id=res[0]['id'], v=vkApiVersion)
                    print("------Количество подписчиков на странице (https://vk.com/", urlId, ") - ", res['count'], sep="")
                    links[urlId] = res['count']
                except:
                    print('------Ссылка на ВК не является ни группой, ни профилем -', urlId)
    if len(links) > 0:
        listDict = list(links.items())
        listDict.sort(key=lambda i : i[1])
        targetVkGroup = "https://vk.com/" + listDict[-1][0]
        foundDomains['vkGroups'] = targetVkGroup
        if targetVkGroup in foundDomains['vkElse']:
            foundDomains['vkElse'].remove(targetVkGroup)
    return foundDomains


def delete_channel_all(channel_id: str):
    query = "DELETE FROM channels WHERE channelId = %s"
    value = (channel_id,)
    try:
        mycursor.execute(query, value)
        mydb.commit()
        print("Channels - Канал успешно удален из списка каналов!")
    except:
        print("Channels - Не получилось удалить канал, на котором отсутствуют видео!")
        raise SystemError
    try:
        query = "DELETE FROM channelsToGo WHERE leadingChannelId = %s"
        mycursor.execute(query, value)
        mydb.commit()
        print("Channels_to_go - Каналы, на которые ссылался пустой канал также удалены!")
    except:
        print("Channels_to_go - Не получилось удалить каналы, на которые ссылался этот (возможно, их не было)!")
        raise SystemError
    try:
        query = "DELETE FROM videos_ids WHERE channel_id = %s"
        mycursor.execute(query, value)
        mydb.commit()
        print("Videos_ids - Видео из списка видео удалены!")
    except:
        print("Videos_ids - Не получилось удалить видео из списка видео!")
        raise SystemError
    try:
        query = "DELETE FROM videos WHERE channel_id = %s"
        mycursor.execute(query, value)
        mydb.commit()
        print("Videos - Все видео удалены!")
    except:
        print("Videos - Не получилось удалить видео!")
        raise SystemError

# Для отладки
# Для отладки
def delete_fcking_all():
    mycursor.execute('DELETE FROM channels')
    mydb.commit()
    mycursor.execute('DELETE FROM channelsToGo')
    mydb.commit()
    mycursor.execute('DELETE FROM videos_ids')
    mydb.commit()
    mycursor.execute('DELETE FROM videos')
    mydb.commit()
#delete_fcking_all()