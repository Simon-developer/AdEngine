import pandas as pd
import json
import numpy as np
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense
from tensorflow.keras.models import load_model
from tensorflow.keras import utils
from tensorflow.keras.preprocessing.sequence import pad_sequences
import re
from collections import Counter
from nltk.stem.snowball import RussianStemmer
from nltk.tokenize import TweetTokenizer
import pickle
print("Импортированы модули...")
positive_tweets = r"model/positive.csv"
negative_tweets = r"model/negative.csv"

vocab_size = 5000
tweets_col_number = 3
negative_tweets = pd.read_csv(negative_tweets, header=None, delimiter=';')[[tweets_col_number]]
positive_tweets = pd.read_csv(positive_tweets, header=None, delimiter=";")[[tweets_col_number]]
print("Прочитаны файлы...")
stemmer = RussianStemmer()
regex = re.compile('[^а-яА-Я ]')
stem_cache = {}

def get_stem(token):
    stem = stem_cache.get(token, None)
    if stem:
        return stem
    token = regex.sub('', token).lower()
    stem = stemmer.stem(token)
    stem_cache[token] = stem
    return stem

stem_count = Counter()
tokenizer = TweetTokenizer()

def count_unique_stems_in_tweets(tweets):
    for _, tweet_series in tweets.iterrows():
        tweet = tweet_series[3]
        tokens = tokenizer.tokenize(tweet)
        for token in tokens:
            stem = get_stem(token)
            stem_count[stem] += 1

count_unique_stems_in_tweets(positive_tweets)
count_unique_stems_in_tweets(negative_tweets)
print('Всего найдено стем: ', len(stem_count))

vocab = sorted(stem_count, key=stem_count.get, reverse=True)[:vocab_size]
print(vocab[:200])

with open("model/vocab.json", "w", encoding="utf-8") as file:
    json.dump(vocab, file)

# Поиск стем по популярности от 0 до 4999
idx = 4999
print(vocab[idx], " - ", stem_count.get(vocab[idx]))
idx = 3999
print(vocab[idx], " - ", stem_count.get(vocab[idx]))
idx = 2999
print(vocab[idx], " - ", stem_count.get(vocab[idx]))

token_to_idx = {vocab[i] : i for i in range(vocab_size)}
print(len(token_to_idx))

with open("model/token_to_idx.json", "w", encoding="utf-8") as file:
    json.dump(token_to_idx, file)

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

x = tweet_to_vector("Коллеги сидят рубятся в Urban terror, а я из-за долбанной винды не могу :(")
print(x)
x = x.reshape(1, 5000)
model = load_model('model/model.h5')
r = model.predict(x)
print("result - ", r)