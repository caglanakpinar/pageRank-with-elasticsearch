import os

droping_prob = 0.15
metrics = {0: 'tracks', 1: 'artists', 2: 'albums'}

index_fields = ['name', 'id', 'popularity', 'name_artist', 
                'popularity_artist', 'name_album', 
                'popularity_album', 'artist', 'album',
                'page_rank_tracks', 'page_rank_artists', 'page_rank_albums'
                ]

group_columns = ['name', 'id', 'name_artist', 'name_album', 'artists', 'albums',
                        'popularity', 'popularity_artist', 'popularity_album', 'id_ind', 'artists_ind', 'albums_ind']

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

# ltr features configs
music_features = "music_features"

#ltr model configs
model_parameters = {'hyper_parameters': os.environ['parameters_path']}

point_fileds = ['popularity', 'popularity_artist', 'popularity_album',
               'page_rank_tracks', 'page_rank_artists', 'page_rank_albums']

source_fields = ['tracks', 'artists', 'albums', 'id_ind', 'artists_ind', 'albums_ind']

features = ['id_ind', 'artists_ind', 'albums_ind' ,'popularity', 'popularity_artist', 'popularity_album',
            'page_rank_tracks', 'page_rank_artists', 'page_rank_albums']
y = '_score'

parameters_for_testing = {
    'colsample_bytree': [0.4,0.6,0.8],
    'gamma': [0,0.03,0.1,0.3],
    'min_child_weight': [1.5,6,10],
    'learning_rate': [0.1,0.07],
    'max_depth': [3,5],
    'n_estimators': [100],
    'reg_alpha': [1e-5, 1e-2,  0.75],
    'reg_lambda' :[1e-5, 1e-2, 0.45],
    'subsample': [0.6,0.95]
}

model_payload = {
    "model": {
        "name": "my_model",
        "model": {
            "type": "model/xgboost+json",
            "definition": None
        }
    }
}
#
#features = ['popularity', 'popularity_artist', 'popularity_album',
#               'page_rank_tracks', 'page_rank_artists', 'page_rank_albums', 'id_ind', 'artist_ind', 'album_ind']
#
#features_temp = {
#                 "name": None, "params": ["keywords"],
#                 "template": {}
#                }
#
#feature =   {"function_score": {
#            "query": {"multi_match": {"query": keyword, "fields": []}},
#            "functions": [{"field_value_factor": {"field": None}}],
#            "boost_mode": "replace"
#        }}
#
#feature_set = {
#    "featureset": {
#        "name": music_features,
#        "features": []
#    }
#
#}
