import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
import os
import json
from data_access import read_from_json

def get_judgement_list(res, query, word):
    hits = pd.DataFrame(res['hits']['hits'])
    hits['popularity'] = hits['_source'].apply(lambda x: x['popularity'])
    hits['popularity_artist'] = hits['_source'].apply(lambda x: x['popularity_artist'])
    hits['popularity_album'] = hits['_source'].apply(lambda x: x['popularity_album'])
    hits['point'] = hits['popularity'] * hits['popularity_album'] * hits['popularity_artist']
    if len(hits) >= 5:
        kmeans = KMeans(n_clusters=4, random_state=4).fit([[i] for i in list(hits['point'])[1:]])
        relevance = pd.DataFrame(list(zip(list(kmeans.labels_),
                                          list(hits['point'])[1:]))).rename(columns={0: 'segments', 1: 'search_results'})
        relevance_pivot = relevance.pivot_table(
                                                index='segments', aggfunc={'search_results': 'mean'}
                                               ).reset_index().sort_values(by='search_results', ascending=True
                                                                          ).reset_index().rename(
                                                                            columns={'index': 'relevance'})
        relevance_pivot['relevance'] = relevance_pivot['relevance'] + 1
        relevance = pd.merge(relevance, relevance_pivot, on='segments', how='left')
        relevance = pd.DataFrame([4] + list(relevance['relevance'])).rename(columns={0:'relevance'})
        relevance = pd.concat([hits, relevance[['relevance']]], axis=1)
    else:
        relevance = hits
        r = []
        for i in range(len(hits)):
            if i == 0:
               r.append(4)
            else:
                r.append(1)
        relevance = pd.concat([relevance, pd.DataFrame(r).rename(columns={0: 'relevance'})], axis=0)
    relevance['query'] = query
    relevance['word'] = word
    return relevance[['query', 'word', '_score', '_source', 'relevance']].to_dict('results')

def ab_test(params, rdds):  
    words = []
    for rdd in rdds:
        words += rdd.map(lambda x: x['name']).distinct().collect()
    words = np.unique(words)
    counter = 0
    judgements = []
    for i in words:
        print("word :", i)
        _query = {
            "query": {
                "multi_match": {"query": i,
                                "fields": ["name", "name_artist", "name_album"]
                                }
            }, "size": 20}
        res = params['es'].search(index='music', body=_query)
        if res['hits']['hits'] != []:
            #try:
            judgements += get_judgement_list(res, counter, i)
            counter += 1
            #except:
            #    print(i, "somtihing wrong!")
        print("-" * 50)
    with open('judgements.json', 'w') as file:
        json.dump(judgements, file)
    return judgements

def compute_judgement(params, rdds):
    if params['judgements_to_json']:
        if os.path.exists(params['judgements_read_from_json']['judgement']):
            judgements = read_from_json(parmas['judgements_read_from_json'])
        else:
            judgements = ab_test(params, rdds)
    else:
        judgements = ab_test(params, rdds)
    return judgements            
