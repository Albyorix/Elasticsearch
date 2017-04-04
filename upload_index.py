from __future__ import unicode_literals
import unicodedata
import string
from time import time
import argparse

import pandas as pd
from elasticsearch import Elasticsearch, RequestsHttpConnection
from config import get_language_config, \
    level1, level2, level3, level4, level5, \
    parent_doc_type, child_doc_type, negative_child_doc_type, searched_child_doc_type


def remove_accents(data):
    return unicodedata.normalize('NFKD', data).encode('ASCII', 'ignore')


def create_index_docs(service_data):
    docs = []
    for s in service_data.iterrows():
        d = {
            level1: remove_accents(s[1]["Business"]),
            level2: remove_accents(s[1]["Category"]),
            level3: remove_accents(s[1]["Subcategory"]),
            level4: remove_accents(s[1]["Service"]),
            level5: remove_accents(s[1]["Service Name"]),
            "level1_id": s[1]["Wizard index number"][:5],
            "wizard": s[1]["Wizard index number"],
            "picture1": s[1]["Picture 1"],
            "picture2": s[1]["Picture 2"],
            "is_hidden": False,
        }
        docs.append(d)
    return docs


def create_index_if_needed(es, language, index_name, recreate):
    index_exists = es.indices.exists(index=index_name)

    if index_exists and recreate:
        print("Deleting index {0}".format(index_name))
        es.indices.delete(index=index_name)
        index_exists = False
    if not index_exists:
        print("Creating index {0}".format(index_name))
        elasticsearch_index_settings = get_language_config(language)
        es.indices.create(index=index_name, body=elasticsearch_index_settings)


def get_wizard_to_id_mapping(es, index_name):
    all_hits = es.search(index=index_name, doc_type=parent_doc_type, size=10000)['hits']
    wizard_match = {}
    if all_hits['total'] != 0:
        for hit in all_hits['hits']:
            unique_id = hit['_id']
            wizard = hit['_source']['wizard']
            wizard_match[wizard] = unique_id
    return wizard_match


def update_elastic_index(es, index_name, docs):
    for i, doc in enumerate(docs, start=1):
        wizard = doc['wizard']
        es.index(index_name, doc_type=parent_doc_type, body=doc, id=wizard, params={'routing': wizard})
        print("Indexed {0}/{1} Wizard: {2} - {3}".format(i, len(docs), wizard, doc[level5]))


def get_es_connection(env):
    if env == "prod":
        elastic_url = "https://elastic.com/"
        http_auth = ('LOGIN', 'PASSWORD')
        return Elasticsearch(elastic_url, http_auth=http_auth, connection_class=RequestsHttpConnection)
    else:
        return Elasticsearch("localhost:9200")


def upload_index_from_args(language, index_name, services_filepath, env, recreate):
    service_data = pd.read_csv(services_filepath, encoding='utf8')
    service_data = service_data.fillna('')

    docs = create_index_docs(service_data)

    es = get_es_connection(env)
    print "Start uploading {} language to index: {} in Elastic {} environement".format(language, index_name, env)

    start = time()
    create_index_if_needed(es, language, index_name, recreate)
    # wizard_match = get_wizard_to_id_mapping(es, index_name)
    update_elastic_index(es, index_name, docs)
    print "Executed in {0}".format(time() - start)


def delete_index(index_name, env):
    print "Start deleting index: '{}' in env: '{}'".format(index_name, env)
    es = get_es_connection(env)
    r = es.indices.delete(index=index_name, ignore=[400, 404])
    print r


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Service indexing')
    parser.add_argument("services", nargs='?', default="index/new_wizard_english.csv", help="Wizards CSV file")
    parser.add_argument("language", nargs='?', default="english", help="language name")
    parser.add_argument("es_index", nargs='?', default="new_english", help="index name in Elasticsearch")
    parser.add_argument("-r", "--recreate", action='store_true', help='Recreate the index')
    parser.add_argument("-es", "--elasticsearch", default="local", help="local, prod")
    args = parser.parse_args()

    upload_index_from_args(args.language, args.es_index, args.services, args.elasticsearch, args.recreate)
    #
    # delete_index(args.es_index, args.elasticsearch)