import json
from pyspark import SparkConf, SparkContext
from elasticsearch import Elasticsearch
import os


def elastic_search_init(params):
    os.system(params['elastic_search_path'])
    return Elasticsearch([{'host':'localhost','port':9200}])

def spark_session_init(params):
    conf = SparkConf().setMaster(params["cluster_url"]).setAppName("pageRank_clients")
    return SparkContext(conf=conf)

def data_rdd(params):
    with open("user_transaction.json") as params['transactions_path']:
        transactions = json.loads(file.read())
    with open("user_transaction_all.json") as params['transactions_all_path']:
        transactions_all = json.loads(file.read())
    with open("track_with_artist.json") as params['artists_path']:
        track_with_artist = json.loads(file.read())
    with open("genres.json") as params['genres_path']:
        genres = json.loads(file.read())

    transactions_rdd = params['sc'].parallelize(transactions)
    transactions_all_rdd = params['sc'].parallelize(transactions_all)
    return transactions_rdd, transactions_all_rdd, track_with_artist, genres