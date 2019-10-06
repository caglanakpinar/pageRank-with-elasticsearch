import config

import json
from pyspark import SparkConf, SparkContext
from elasticsearch import Elasticsearch
import os
from pyspark.sql import SparkSession
from pyspark.sql import Row


def elastic_search_init(params):
    os.system(params['elastic_search_path'])
    return Elasticsearch([{'host':'localhost','port':9200}])

def spark_session_init(params):
    conf = SparkConf().setMaster(params["cluster_url"]).setAppName("pageRank_clients")
    sc = SparkContext(conf=conf)
    spark = SparkSession.builder.master(params["cluster_url"]).appName("test").getOrCreate()
    return sc, spark

def data_rdd(params):
    rdds = []
    for i in config.metrics:
        rdds.append(params['sc'].parallelize(read_from_json(params['data_path'][config.metrics[i]])))
    return rdds

def write_to_json(data, file_name):
    with open('page_rank_'+file_name+'.json', 'w') as outfile:
        json.dump(data, outfile)
        
def read_from_json(file_name):
    with open(file_name, 'r') as file:
        data = json.loads(file.read())
    return data