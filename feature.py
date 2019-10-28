import requests
from urllib.parse import urljoin

import config


def creating_features():
    _feature = config.feature
    _feature['function_score']['query']['multi_match']['query'] = [config.metrics[i] for i in config.metrics]
    _features = config.features

    features_list = []
    for f in query_f_set:
        _f = features
        _f2 = feature
        _f['name'] = f
        _f2['function_score']["functions"][0]["field_value_factor"]["field"] = f
        _f["template"] = _f2.copy()
        features_list.append(_f.copy())

    feature_set = config.feature_set
    feature_set['featureset']["features"] = features_list

    path = '_ltr/_featuresets/' + music_features
    url = "http://localhost:9200"

    path = urljoin(url, '_ltr')
    resp = requests.delete(path, auth=None)
    resp = requests.put(path, auth=None)

    ## loading featuresets
    path = "_ltr/_featureset/%s" % config.music_features
    path = urljoin(url, path)
    head = {'Content-Type': 'application/json'}
    resp = requests.post(path, data=json.dumps(feature_set), headers=head, auth=None)
