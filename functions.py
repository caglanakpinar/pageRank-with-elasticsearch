import config
from data_access import write_to_json, read_from_json

import os
import numpy as np
from main import parameters
import json
from pyspark.sql import SparkSession
from pyspark.sql import Row
from elasticsearch import helpers
from multiprocessing import Pool


def point_calcualtion(obj):
    obj['point'] = obj[config.point_fileds[0]] if obj[config.point_fileds[0]] else 1
    for f in config.point_fileds[1:]:
        if obj[f]:
            obj['point'] *= obj[f]
    obj['point'] = obj['point'] if obj['point'] else 0
    return obj

def find_page_rank(linkmatrix, pages):
    eigval, eigvector = np.linalg.eig(linkmatrix)
    return sorted(zip(np.abs(eigval), pages), reverse=True)

def compute_page_rank(rdd):
    u_ids = rdd.map(lambda x: x['id']).distinct()
    totals = u_ids.count()
    A = np.zeros((totals, totals))
    U = np.ones((totals, totals)) * (1 / totals)
    smoothing = np.ones((1, totals)) * 0.01
    data = rdd.map(lambda x: (x['id'], x['id_2'], x['pl_count'])).collect()
    ids = u_ids.collect()

    word_idx = {}
    count = 0
    for i in ids:
        word_idx[i] = count
        count += 1

    counter = 0
    for w1 in ids:
        data_2 = list(filter(lambda x: x[0] == w1, data))
        u_ids_2 = list(set(list(map(lambda x: x[1], data_2))) & set(ids))
        data_3 = list(filter(lambda x: x[1] in u_ids_2, data_2))
        if counter % 3000 == 0:
            print(counter, "done..")
        for c in data_3:
            A[word_idx[w1], word_idx[c[1]]] = c[2]
            A[word_idx[c[1]], word_idx[w1]] = c[2]
        counter += 1
    droping_prob = parameters['droping_prob'] if parameters['droping_prob'] else config.droping_prob
    A_sum = smoothing + A.sum(axis=0)
    A = (A.T / A_sum.T).T
    G = ((1 - droping_prob) * A) + (droping_prob * U)
    page_rank = find_page_rank(G, ids)
    print("page rank is done!")
    return page_rank

def create_id_to_index(tracks_rdd):
    """
    :param tracks_rdd: rdd with fields in it id, name, artist, album, name_arist, ..
    :return: it returns unique album - track -artist of ids to index number. Ex: idx[artist]['Radiohead'] = 5
    """

    def get_index(v):
        for m in config.metrics:
            key = config.metrics[m] if m != 0 else 'id'
            v[key+'_ind'] = idx_list[m][v[key]] # if v[m] in list(index_dict[m].keys()) else 234234234
        return v
    
    idx_list = []
    for m in config.metrics:
        key = config.metrics[m] if m != 0 else 'id'
        print(key)
        _m = tracks_rdd.map(lambda x: x[key]).distinct().collect()
        counter = 0
        idx = {}
        for i in _m:
            idx[i] = counter
            counter += 1
        idx_list.append(idx)
    asd = tracks_rdd.collect()
    asd = list(map(lambda x: get_index(x), asd))
    tracks_rdd = tracks_rdd.map(get_index)
    return tracks_rdd

def create_index_docs(rdd, page_ranks, spark):

    def convert_group_columns(v):
        obj = {}
        counter = 0
        for i in config.group_columns:
            obj[i] = v[counter]
            counter += 1
        for i, pr in enumerate(page_ranks):
            key = config.metrics[i] if i != 0 else 'id'
            field = 'page_rank_' + config.metrics[i]
            _rank = None
            try:
                _rank = list(filter(lambda x: x[1] == obj[key], pr))[0][0]
            except:
                print("ohh no!!")
            obj[field] = _rank
        obj = point_calcualtion(obj)
        return obj

    df = spark.createDataFrame(rdd)
    df = df.groupBy([col for col in config.group_columns if col.split("_")[-1] != "_ind"]).count()
    return df.rdd.map(tuple).map(convert_group_columns)


def create_index(params, tracks_rdd, page_ranks):
    """
    Creates index with given values : 'name', 'id', 'popularity', 'name_artist',
                                      'popularity_artist', 'name_album',
                                      'popularity_album', 'artist', 'album',
                                      'page_rank_track', 'page_rank_artist', 'page_rank_album'
    every each index obj represents tracks with artist and album in it.
    if there is any prev created index, deletes then creates.
    :param params: initial values are stored at params dictionary.
    :param tracks_rdd: tracks rdd (list of obj): name(tracks), id (tracks), popularity (tracks)
    :param tracks_rank: rank vector of tracks
    :param artists_rank: rank vector of artist
    :param albums_rank: rank vector of albums
    :return: bulk inserts the tracks into music index
    """
    # deleting prev index if there is
    params['es'].indices.delete('search_all', ignore=[400, 404])
    params['es'].indices.create('search_all', body=config.index_settings) # creating again
    # creating rdd to tuple file for index obj
    asd = tracks_rdd.collect()
    print("asd")
    tracks_rdd = create_id_to_index(tracks_rdd)
    tracks_pv_rdd = create_index_docs(tracks_rdd, page_ranks, params['spark'])
    tracks2 = tracks_pv_rdd.collect()
    print(tracks2[0])
    def get_insert_obj(list_of_obj, index):
        ids = list(map(lambda x: x['id'], list_of_obj))
        counter = 0
        for i in list_of_obj:
            add_cmd = {"_index": index,  "_type": "text",  "_id": ids[counter], "_source": i}
            counter += 1
            yield add_cmd
    print("yes")
    helpers.bulk(params['es'], get_insert_obj(tracks2, 'music'))

def get_page_ranks(params, rdds):
    paths = params['page_rank_read_from_json']
    page_ranks = []
    counter = 0
    for rdd in rdds:
        if os.path.exists(paths[config.metrics[counter]]):
            page_rank = read_from_json(paths[config.metrics[counter]])
        else:
            page_rank = compute_page_rank(rdd)
        page_ranks.append(page_rank)
        if params['page_rank_to_json']:
            if not os.path.exists(paths[config.metrics[counter]]):
                write_to_json(page_rank, config.metrics[counter])
        counter += 1
    return page_ranks

