import data_access
import functions

parameters = {
    'transactions_path': 'user_transaction.json',
    'transactions_all_path': 'user_transaction_all.json',
    'artists_path': 'track_with_artist.json',
    'genres_path': 'genres.json',
    'elastic_search_path': "/elasticsearch-7.3.1/bin/elasticsearch",
    'es': None,
    'artist': None,
    'genre': None,
    'cluster_url': "local[*]",
    'droping_prob': 0.15
}

def main(params):
    params['sc'] = data_access.spark_session_init(params)
    transactions_rdd, transactions_all_rdd, track_with_artist, genres = data_access.data_rdd(params)
    page_rank_user = functions.compute_user_track_page_rank(transactions_rdd)
    page_rank_with_artist = functions.compute_track_page_rank(transactions_all_rdd, track_with_artist, genres)
    params['es'] = data_access.elastic_search_init(params)
    functions.create_index(params, page_rank_user, page_rank_with_artist)


if __name__ == '__main__':
    main(parameters)