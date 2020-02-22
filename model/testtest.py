import json
token_to_idx = {}
with open('token_to_idx.json') as json_file2:
    data2 = json.load(json_file2)
    for token in data2:
        token_to_idx.append(token)