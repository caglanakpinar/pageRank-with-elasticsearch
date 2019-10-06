import data_access
import functions
import config
from judgement_creator import compute_judgement

parameters = {
    'data_path': config.data_path,
    'elastic_search_path': config.elasticseach_path,
    'sc': None,
    'spark': None,
    'es': None,
    'artist': None,
    'genre': None,
    'cluster_url': config.cluster_url,
    'droping_prob': 0.15,
    'page_rank_to_json': True,
    'page_rank_read_from_json': config.page_rank_file_path,
    'judgements_to_json': True,
    'judgements_read_from_json': config.judgements_file_path
}

def main(params):
    params['sc'], params['spark'] = data_access.spark_session_init(params)
    rdds = data_access.data_rdd(params)
    page_ranks = functions.get_page_ranks(params, rdds)
    params['es'] = data_access.elastic_search_init(params)
    functions.create_index(params, rdds[0], page_ranks)
    judgements = compute_judgement(params, rdds)


if __name__ == '__main__':
    main(parameters)