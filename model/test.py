import json
import numpy as np
from tensorflow.keras.models import load_model
import re
from nltk.tokenize import TweetTokenizer
from nltk.stem.snowball import RussianStemmer

vocab_size = 5000
vocab = []
token_to_idx = []
tokenizer = TweetTokenizer()
stemmer = RussianStemmer()
regex = re.compile('[^а-яА-Я ]')
stem_cache = {}
model = load_model("model.h5")


with open('vocab.json') as json_file:
    data = json.load(json_file)
    for stem in data:
        vocab.append(stem)

token_to_idx = {vocab[i] : i for i in range(vocab_size)}
print(token_to_idx)

def get_stem(token):
    stem = stem_cache.get(token, None)
    if stem:
        return stem
    token = regex.sub('', token).lower()
    stem = stemmer.stem(token)
    stem_cache[token] = stem
    return stem

def tweet_to_vector(tweet, show_unknowns=False):
    vector = np.zeros(vocab_size, dtype=np.int_)
    for token in tokenizer.tokenize(tweet):
        stem = get_stem(token)
        idx = token_to_idx.get(stem, None)
        if idx is not None:
            vector[idx] = 1
        elif show_unknowns:
            print("Неизвестное слово: ", token)
    print(vector[:13])
    return vector
comments_array = ["Как приятно смотреть на настоящие эмоции... вот где возмущение, вот где накал. И обстоятельно, по факту. Друже, вернись к этому формату."]
positive = 0
so_so = 0
negative = 0

for comment in comments_array:
    processed_comment = tweet_to_vector(comment)
    processed_comment = processed_comment.reshape(1, 5000)
    prediction = model.predict(processed_comment)
    print(comment, "\n", prediction)
    if prediction < 0.3:
        negative += 1
    elif prediction >= 0.3 and prediction <=0.7:
        so_so += 1
    else:
        positive += 1

print("Положительных: ", positive)
print("Средних: ", so_so)
print("Отрицательных: ", negative)
