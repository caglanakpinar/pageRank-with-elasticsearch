import config
from data_access import write_to_json, read_from_json

from sklearn.metrics import mean_squared_error

def data_preparation(df, source_fields):
    df = df.reset_index()
    source_fields_2 = []
    counter = 0
    for row in list(df['_source']):
        if row == row:
            _fields = {}
            _fields['index'] = counter
            for f in source_fields:
                key = 'id' if f == 'tracks' else f
                _fields[key] = row[key]
            source_fields_2.append(_fields)
        counter += 1
    return pd.merge(df, pd.DataFrame(source_fields_2), how='left', on='index')


def parameter_tunning(X_train, y_train):
    xgb_model = xgb.XGBRegressor(learning_rate=0.1, n_estimators=1000, max_depth=5,
                                 min_child_weight=1, gamma=0, subsample=0.8, colsample_bytree=0.8, nthread=6,
                                 scale_pos_weight=1, seed=27)

    gsearch1 = GridSearchCV(estimator=xgb_model, param_grid=parameters_for_testing,
                            n_jobs=6, iid=False, verbose=10, scoring='neg_mean_squared_error')
    gsearch1.fit(X_train, y_train)

    print (gsearch1.grid_scores_)
    print('best params')
    print (gsearch1.best_params_)
    print('best score')
    print (gsearch1.best_score_)

    return gsearch1.best_params_


def train_xgboost(X_train, y_train):
    # data preparation for xgboost with using DMatrix
    # Categorical features not supported in DMatrix. label them by using indexes.
    data_dmatrix = xgb.DMatrix(data=X, label=y)
    tuned_parameters = check_for_tuned_model_parameters(X_train, y_train, params)
    xg_regressor = xgb.XGBRegressor(**tuned_parameters)
    xg_regressor.fit(X, y)
    return xg_regressor

def test_mode(model, X_test, y_test):
    y_pred = model.predict(X_test)
    return mean_squared_error(y_true, y_pred)

def save_model(model):
    file_name = "trained-model.json"
    model.dump_model(file_name, with_stats=False, dump_format='json')
    return file_name
def upload_model_to_elasticsearch(file_name):
    saved_model = read_from_json(file_name)
    model_payload = config.model_payload
    model_payload['model']['model']['definition'] = saved_model
    path = "_ltr/_featureset/%s/_createmodel" % config.music_features
    path = urljoin(url, path)

def create_ltr_model(judgements, params):
    jd = pd.DataFrame(judgments)
    jd = data_preparation(jd, config.source_fields)
    y = jd[y]
    X = jd[features]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=123)
    xg_regressor = train_xgboost(X_train, y_train, params)
    mse = test_mode(xg_regressor, X_test, y_test)
    file_name = save_model(model)
    head = {'Content-Type': 'application/json'}
    resp = requests.post(path, data=json.dumps(model_payload), headers=head, auth=None)

def check_for_tuned_model_parameters(X_train, y_train, params):
    if os.path.exists(params['parameter_tuning']):
        tuned_parameters = read_from_json(params['parameter_tunning'])
    else:
        tuned_parameters = parameter_tunning(X_train, y_train)
        write_to_json(tuned_parameters, params['parameter_tunning'])
    return tuned_parameters