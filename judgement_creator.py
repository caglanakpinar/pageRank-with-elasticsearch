import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
import os
import json
from data_access import read_from_json
import config

def get_judgement_list(res, query, word):
    hits = pd.DataFrame(res['hits']['hits'])
    hits['point'] = hits.apply(lambda row: row['_source']['point'], axis=1)
    hits = hits[hits['point'] != 0]
    #if len(hits) >= 5:
    #    # relevance calculation
    #    kmeans = KMeans(n_clusters=4, random_state=4).fit([[i] for i in list(hits['point'])[1:]])
    #    relevance = pd.DataFrame(list(zip(list(kmeans.labels_),
    #                                      list(hits['point'])[1:]))).rename(columns={0: 'segments',
    #                                                                                 1: 'search_results'})
    #    relevance_pivot = relevance.pivot_table(
    #                                            index='segments', aggfunc={'search_results': 'mean'}
    #                                           ).reset_index().sort_values(by='search_results', ascending=True
    #                                                                      ).reset_index().rename(
    #                                                                        columns={'index': 'relevance_label'})
    #    relevance_pivot['relevance_label'] = relevance_pivot['relevance_label'] + 1
    #    relevance = pd.merge(relevance, relevance_pivot, on='segments', how='left')
    #    relevance = pd.DataFrame([4] + list(relevance['relevance_label'])).rename(columns={0:'relevance_label'})
    #    relevance = pd.concat([hits, relevance[['relevance_label']]],
    #                          axis=1).sort_values(by=['relevance_label', '_score'], ascending=True)
    #    relevance = relevance.reset_index(drop=True).reset_index().rename(columns={'index': 'rank_score'})
    #    relevance['rank_score'] = 10 - relevance['rank_score']
#
    #else:
    #    relevance = hits
    #    r = []
    #    for i in range(len(hits)):
    #        if i == 0:
    #           r.append(4)
    #        else:
    #            r.append(1)
    #    relevance = pd.concat([relevance, pd.DataFrame(r).rename(columns={0: 'relevance'})], axis=0)
    relevance = hits
    relevance = relevance.sort_values(by='point', ascending=False)
    relevance = relevance.reset_index(drop=True).reset_index().rename(columns={'index': 'rank_score'})
    relevance['query'] = query
    relevance['word'] = word
    return relevance.to_dict('results')#relevance[['query', 'word', '_score', '_source', 'relevance']].to_dict('results')

def     ab_test(params, rdds):
    words = []
    for rdd in rdds:
        _w = rdd.map(lambda x: x['name']).distinct()
        words += _w.collect()
    words = np.unique(words)
    judgements = []
    counter = 0
    for i in words:
        print("word :", i)
        _query = {
            "query": {
                "multi_match": {"query": i,
                                "fields": ["name", "name_artist", "name_album"]
                                }
            }, "size": 10}
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
            judgements = read_from_json(params['judgements_read_from_json']['judgement'])
        else:
            judgements = ab_test(params, rdds)
    else:
        judgements = ab_test(params, rdds)
    return judgements            
