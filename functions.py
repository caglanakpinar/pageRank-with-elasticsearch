import config
import numpy as np
from main import parameters


def findPageRank(linkmatrix, pages, top, user):
    eigval, eigvector= np.linalg.eig(linkmatrix)
    if user:
        return [t for t in sorted(zip(np.abs(eigval), pages, [user for t in range(top)]), reverse=True) if t[0] != 0][0:50]
    else:
        return [t for t in sorted(zip(np.abs(eigval), pages), reverse=True) if t[0] != 0]

def H_matrix_2(v):
    droping_prob = parameters['droping_prob']  if parameters['droping_prob'] else config.droping_prob
    i = list(v.keys())[0] # represents user_id
    _rows = list(map(lambda x: int(x[0].split("_")[1]), v[i])) # track list, user start with
    _columns = list(map(lambda x: int(x[1].split("_")[1]), v[i]))  # track list, user end with
    totals = list(set(_rows + _columns)) # total track list user has listened
    # Transition matrix has Null values rather than assing them 0, I assign 0.01.
    # I convert to G value, so it is not a big deal to assign them 0.01.
    smoothing = np.ones((1, len(totals)))* 0.01
    _A = np.zeros((len(totals), len(totals))) # initialize zero matrix which is Transition matrix
    # assign each transition to to A matrix
    for r in range(len(_rows)):
        for c in range(len(_columns)):
            _A[r, c] += 1
     # calculate columns sum
    _A_sum = smoothing + _A.sum(axis=0)
    # calculate probabilities
    _A = (_A.T / _A_sum.T).T
    # U(i, j) = 1 / (num of transactions)
    U = np.ones((len(_A), len(_A))) * (1 / len(_A))
    _G = ((1-droping_prob) * _A) + (droping_prob * U)
    pageRank = findPageRank(_G, ['t_'+ str(t_n) for t_n in _rows], 5, i)
    return pageRank

def find_artst(a, artist):
    return list(filter(lambda x: x[0] == a, artist))[0][1]

def find_genre(g, genres):
    return list(filter(lambda x: x[1] == g, genres))[0][0]

def compute_user_track_page_rank(transactions_rdd):
    pageRank_user = transactions_rdd.map(H_matrix_2).collect()
    return pageRank_user


def compute_track_page_rank(transactions_all_rdd, track_with_artist, genres):
    track_list = transactions_all_rdd.map(lambda x: int(x[0].split("_")[1]), transactions_all_rdd).distinct()
    total_tracks = track_list.count()
    tracks = track_list.collect()
    A = np.zeros((total_tracks, total_tracks))
    U = np.ones((total_tracks, total_tracks)) * 1 / total_tracks
    smoothing = np.ones((1, total_tracks)) * 0.01

    for r in tracks:
        for c in tracks:
            A[r, c] += 1

    droping_prob = parameters['droping_prob'] if parameters['droping_prob'] else config.droping_prob
    A_sum = smoothing + A.sum(axis=0)
    A = (A.T / A_sum.T).T
    _G = ((1 - droping_prob) * A) + (droping_prob * U)
    page_rank = findPageRank(_G, tracks, total_tracks, None)
    page_rank = list(map(lambda x: (x[0], 't_' + str(x[1])), page_rank))
    page_rank_with_artist = list(map(lambda x: (x[0], x[1], find_artst(x[1], track_with_artist)), page_rank))
    page_rank_with_artist = list(map(lambda x: (x[0], x[1], x[2], find_genre(x[2], genres)), page_rank_with_artist))
    return page_rank_with_artist

def create_index(params, page_rank_user, page_rank_with_artist):
    # track index
    counter = 0
    for r, t, a, g in page_rank_with_artist:
        _e = config.track_index_obj
        _e['track'], _e['artist'], _e['rank'], _e['genre'] = t, a, r, g
        params['es'].index(index="music", id=counter, body=_e)
        counter += 1

    # type of search index
    counter = 0
    type_ind = ['track', 'artist', 'genre']
    for x in page_rank_user:
        count = 0
        for i in x[1:]:
            _e = config.type_index_obj
            _e['type'], _e['search'] = type_ind[count], i
            params['es'].index(index="search_all", id=counter, body=_e)
            count += 1
            counter += 1
    # user tracks index
    counter = 0
    for u in page_rank_user:
        for t in u:
            _e = config.user_index_obj
            _e['user'], _e['track'], _e['rank'] = t[2], t[1], t[0]
            params['es'].index(index="user_track", id=counter, body=_e)
            counter += 1