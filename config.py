import os

droping_prob = 0.15
metrics = {0: 'tracks', 1: 'artists', 2: 'albums'}

index_fields = ['name', 'id', 'popularity', 'name_artist', 
                'popularity_artist', 'name_album', 
                'popularity_album', 'artist', 'album',
                'page_rank_tracks', 'page_rank_artists', 'page_rank_albums'
                ]

group_columns = ['name', 'id', 'name_artist', 'name_album', 'artists', 'albums',
                        'popularity', 'popularity_artist', 'popularity_album']

index_settings = {
    "settings": {
        "number_of_shards": 2,
        "number_of_replicas": 0,
        "index": {
            "analysis": {},
        },
        "mappings": {}
    }

}
elasticseach_boosting_field =  {"type": "text", "boost": None}

a_b_test_day_count = 30
search_words_count = 1000
top_results = 10 # top 10 results of search engine

elasticseach_path = os.environ['elastic_search_path']
cluster_url = os.environ['cluster_url']
data_path = {
             'tracks': os.environ['tracks_file_path'],
             'artists': os.environ['artists_file_path'],
             'albums': os.environ['albums_file_path']
            }

page_rank_file_path = {
                        'tracks': os.environ['page_rank_tracks_file_path'],
                        'artists': os.environ['page_rank_artists_file_path'],
                        'albums': os.environ['page_rank_albums_file_path']
                       }
judgements_file_path = {'judgement': os.environ['judgements_file_path']}