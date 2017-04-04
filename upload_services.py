from __future__ import unicode_literals
import argparse
import json
import pandas as pd
from tqdm import tqdm

from query_elasticsearch import QueryElasticsearchTest
from upload_index import get_es_connection, remove_accents
from config import index_language_mapping, \
    parent_doc_type, child_doc_type, negative_child_doc_type, searched_child_doc_type


def format_index_element_from_elastic_hit(hit):
    """
    :param hit: dict, from elasticsearch
    :return: dict, index_element payload for frontend
    """
    index_element = {
        "id": hit['_id'],
        "score": hit['_score'],
        "level1": hit["_source"]["level1"],
        "level2": hit["_source"]["level2"],
        "level3": hit["_source"]["level3"],
        "level4": hit["_source"]["level4"],
        "level5": hit["_source"]["level5"],
        "wizard": hit["_source"]["wizard"],
        "pictures": [hit["_source"]["picture1"],
                     hit["_source"]["picture2"]]
    }
    return index_element


def get_top3_index_elements_from_service(es, es_query, index, doc):
    """
    This send top 3 matched parents to the frontend and frontend should
    :param es: Elasticsearch,
    :param es_query: Obj, containing queries
    :param index: str,
    :param doc, dict,
    :return: dict, parent, triplet and the user_dictionary
    """
    query = es_query.tom_query(doc, size=3)
    res = es.search(index=index, body=query, doc_type=parent_doc_type)

    if res['hits']['total'] > 0:
        return [format_index_element_from_elastic_hit(hit) for hit in res['hits']['hits']]
    else:
        return []


def save_service(es, index, doc, matched_index_element_id, unmatched_index_element_ids, used_search):
    """
    :param es: Elasticsearch,
    :param index: str,
    :param doc: dict,
    :param matched_index_element_id: str,
    :param unmatched_index_element_ids: list of str,
    :param used_search: Boolean, True if the search box was used
    :return:
    """
    for unmatched_index_element_id in unmatched_index_element_ids:
        # save negative service
        es.index(index=index, body=doc, parent=unmatched_index_element_id, doc_type=negative_child_doc_type)
    if used_search:
        # save searched service
        es.index(index=index, body=doc, parent=matched_index_element_id, doc_type=searched_child_doc_type)
    # save service
    es.index(index=index, body=doc, parent=matched_index_element_id, doc_type=child_doc_type)


def smart_upload_service(es, es_query, index_name, doc, matched_index_element_id):
    # get top 3 in elastic
    hits = get_top3_index_elements_from_service(es, es_query, index_name, doc)
    # get matching wizard id in elastic
    unmatched_index_element_ids = [hit["id"] for hit in hits]
    # get index_elements ids into format
    if matched_index_element_id in unmatched_index_element_ids:
        unmatched_index_element_ids = [e for e in unmatched_index_element_ids if e != matched_index_element_id]
        used_search = False
    else:
        used_search = True
    # save service
    save_service(es, index_name, doc, matched_index_element_id, unmatched_index_element_ids, used_search)


def create_service_docs(service_filepath, filter_duplicate=False, limit=-1):
    """
    Read the csv file with pandas and transform it into list of json
    :param service_filepath: str,
    :param filter_duplicate: boolean,
    :param limit: int,
    :return: list of dict,
    """
    service_data = pd.read_csv(service_filepath, encoding='utf8')
    service_data = service_data[:limit].fillna('')
    docs = []
    for s in tqdm(service_data.iterrows()):
        d = {
            "product_category": remove_accents(s[1]["prod_category"]),
            "product_description": remove_accents(s[1]["prod_description"]),
            "wizard_gt": s[1]["prod_standard_cat"],
            "venue_category": remove_accents(s[1]["industry"]),
            "venue_name": remove_accents(s[1]["subdom_name"]),
            "venue_category_id": s[1]["industry_id"],
            "user_id": "-1",
            "check_flag": False,
            "last_fetch_date": "2010-10-10 10:10:10",  # Random date to not leave emtpy
        }
        docs.append(d)
    if filter_duplicate:
        total = len(docs)
        hashs = []
        deletes = []
        for i, d in tqdm(enumerate(docs)):
            h = d["product_description"] + d["product_category"] + d["venue_category"] + d["wizard_gt"]
            if h in hashs:
                deletes.append(i)
            else:
                hashs.append(h)
        for i in deletes[::-1]:
            docs.pop(i)
        print "Deleted {} perfect duplicates out of {}".format(total-len(docs), total)
    return docs


def upload_service_from_args(es, es_query, service_filepath, index_name, smart_upload=False, limit=-1):
    docs = create_service_docs(service_filepath, filter_duplicate=True, limit=limit)
    print "Start uploading {nb_serv} services onto {host}".format(nb_serv=len(docs), host=es.transport.hosts[0]["host"])

    for doc in tqdm(docs):
        parent_search_query = es_query.get_exact_match_query_from_venue_wizard(doc["wizard_gt"])
        parent = es.search(index=index_name, doc_type=parent_doc_type, body=parent_search_query, size=1)
        parent_id = parent["hits"]["hits"][0]["_id"]

        if smart_upload:
            smart_upload_service(es, es_query, index_name, doc, parent_id)
        else:
            es.index(index_name, doc_type=child_doc_type, body=doc, parent=parent_id)


def delete_service_from_index(es, index_name):
    r = es.search(index=index_name, doc_type=child_doc_type, size=1)
    total = r["hits"]["total"]
    print "Delete {num} services in Elastic".format(num=total)
    es.delete_by_query(index=index_name, doc_type=child_doc_type, body={"query": {"match_all": {}}})
    r = es.search(index=index_name, doc_type=searched_child_doc_type, size=1)
    total = r["hits"]["total"]
    print "Delete {num} searched services in Elastic".format(num=total)
    es.delete_by_query(index=index_name, doc_type=searched_child_doc_type, body={"query": {"match_all": {}}})
    r = es.search(index=index_name, doc_type=negative_child_doc_type, size=1)
    total = r["hits"]["total"]
    print "Delete {num} negative services in Elastic".format(num=total)
    es.delete_by_query(index=index_name, doc_type=negative_child_doc_type, body={"query": {"match_all": {}}})


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Venue services indexing from json')
    parser.add_argument("services", nargs='?', default="datasets/product_and_subdomain_fr_for_prod.csv", help="csv file with list of products")
    parser.add_argument("es_index", nargs='?', default="new_french_test", help="index name in Elasticsearch")
    parser.add_argument("-es", "--elasticsearch", default="local", help="local, prod")
    parser.add_argument("--smart_upload", action='store_true', help='Upload in a smart way (only in new index)')
    parser.add_argument("-l", "--limit", default=-1, help="Integer, limit of the number of rows to upload")
    args = parser.parse_args()

    es = get_es_connection(args.elasticsearch)
    es_query = QueryElasticsearchTest()

    upload_service_from_args(es, es_query, args.services, args.es_index, args.smart_upload, args.limit)
    #
    # delete_service_from_index(es, args.es_index)
