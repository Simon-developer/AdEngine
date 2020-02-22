import pandas as pd
import numpy as np
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense
from tensorflow.keras import utils
from tensorflow.keras.preprocessing.sequence import pad_sequences
import re
from collections import Counter
from sklearn.model_selection import train_test_split
from nltk.stem.snowball import RussianStemmer
from nltk.tokenize import TweetTokenizer


positive_tweets = r"C:\Users\Sam\PycharmProjects\AdEngine\positive.csv"
negative_tweets = r"C:\Users\Sam\PycharmProjects\AdEngine\negative.csv"

vocab_size = 5000
tweets_col_number = 3
negative_tweets = pd.read_csv(negative_tweets, header=None, delimiter=';')[[tweets_col_number]]
positive_tweets = pd.read_csv(positive_tweets, header=None, delimiter=";")[[tweets_col_number]]

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
# Поиск стем по популярности от 0 до 4999
idx = 4999
print(vocab[idx], " - ", stem_count.get(vocab[idx]))
idx = 3999
print(vocab[idx], " - ", stem_count.get(vocab[idx]))
idx = 2999
print(vocab[idx], " - ", stem_count.get(vocab[idx]))

token_to_idx = {vocab[i] : i for i in range(vocab_size)}
print(len(token_to_idx))

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

tweet = negative_tweets.iloc[1][3]
print(negative_tweets.iloc[1][3])
print("Tweet:", tweet)
print("Вектор - ", tweet_to_vector(tweet)[:10])
print(len(tweet_to_vector(tweet)))

tweet_vectors = np.zeros(
    (len(negative_tweets) + len(positive_tweets), vocab_size),
    dtype=np.int_)
tweets = []
for ii, (_, tweet) in enumerate(negative_tweets.iterrows()):
    tweets.append(tweet[3])
    tweet_vectors[ii] = tweet_to_vector(tweet[3])
for ii, (_, tweet) in enumerate(positive_tweets.iterrows()):
    tweets.append(tweet[3])
    tweet_vectors[ii + len(negative_tweets)] = tweet_to_vector(tweet[3])

labels = np.append(
    np.zeros(len(negative_tweets), dtype=np.int_),
    np.ones(len(positive_tweets), dtype=np.int_))

x = tweet_vectors
y = labels
x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.3)

model = Sequential()
model.add(Dense(128, activation='relu', input_shape=(vocab_size,)))
model.add(Dense(64, activation='relu'))
model.add(Dense(1, activation='sigmoid'))

model.compile(optimizer='adam',
              loss='binary_crossentropy',
              metrics=['accuracy'])

history = model.fit(x_train,
                    y_train,
                    epochs=5,
                    batch_size=128,
                    validation_split=0.1)

model.save("lastfuckingmodel.h5")