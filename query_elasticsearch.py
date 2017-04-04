from copy import deepcopy

from elasticsearch import Elasticsearch

# from modules.warehouse.service_compiler.auto_servicematcher import QueryForElastic
# from nlp_processor import get_verb_and_noun_from_google_language_api
from utils import get_logging

log = get_logging(__name__)


class RunningAutoServicematcher:
    
    def __init__(self, env, queries, result_filter=">=1"):
        """
        :param env: str, environement, default is local env, "prod" for prod env
        :param queries: str, to find the equivalent queries obj, "old", "new", "level5"
        :param result_filter: str, type of the filter, "==1" or ">=1"
        """
        if env == "prod":
            elasticsearch_host = "https://elastic.com/"
            http_auth = ("USER", "PASSWORD")
            use_ssl = True
            port = 443
        else:
            elasticsearch_host = "localhost"
            http_auth = ("", "")
            use_ssl = False
            port = 9200
        self.es = Elasticsearch(elasticsearch_host, port=port, use_ssl=use_ssl, http_auth=http_auth)
        if queries == "new":
            self.queries = QueryElasticsearchTest()
            self.parent_doc_type = "index_element"
            self.child_doc_type = "service"
        else:
            log.warning("Queries {} not defined, choose 'old', 'new' or 'level'".format(queries))
        self.result_filter = result_filter
        log.info("Using elastic {}".format(self.es))
        log.info("Using queries '{}'".format(self.queries.__class__))
        log.info("Using filter '{}' for analysis".format(self.result_filter))

    def get_query_by_name(self, name, data):
        return getattr(self.queries, name)(data)

    def get_result_bool(self, total_result):
        if self.result_filter == "==1":
            return total_result == 1
        elif self.result_filter == ">=1":
            return total_result >= 1
        else:
            log.warning("Filter '{}' baddly defined, needs to be '==1' or '>=1'".format(self.result_filter))
            return  False

    def run_query_by_name(self, name, index, data, doc_type=None):
        if doc_type is None:
            doc_type = self.parent_doc_type
        exact_match_query = self.get_query_by_name(name, data)
        res = self.es.search(index=index, doc_type=doc_type, body=exact_match_query)
        if self.get_result_bool(res["hits"]["total"]):
            parent = res["hits"]["hits"][0]
            parent_wizard = parent["_source"]["wizard"]
            return parent_wizard


def preprocess_dict(data, use_google_language=False, lowercase=True, alphanumspace=True, deldoublespace=True):
    data = deepcopy(data)
    if use_google_language and "product_description" in data:
        try:
            data["product_description"] = get_verb_and_noun_from_google_language_api(data["product_description"])
        except:
            pass
    for key, value in data.iteritems():
        new_value = value
        if isinstance(value, basestring):
            if lowercase:
                new_value = new_value.lower()
            if alphanumspace:
                new_value = "".join([char for char in new_value if char.isalpha() or char.isdigit() or char.isspace()])
            if deldoublespace:
                while "  " in new_value:
                    new_value = new_value.replace("  ", " ")
                if len(new_value) > 0 and new_value[0] == " ":
                    new_value = new_value[1:]
                if len(new_value) > 0 and new_value[-1] == " ":
                    new_value = new_value[:-2]
        elif isinstance(value, dict):
            new_value = preprocess_dict(value)
        else:
            new_value = value
        data[key] = new_value
    return data


class QueryElasticsearchTest:
    """
    Input: dict,
        Keys can be:
            product_description
            venue_category_id
            venue_name
            product_category
            venue_category
    Returns: (dict, str)
        The query for elastic, and the name of the function
    """

    def __init__(self):
        pass #QueryForElastic.__init__(self)

    def get_exact_match_query_from_venue_wizard(self, wizard):
        """
        Get the query to have the exact match of a wizard
        :param wizard: str, 25 numbers code
        :return: dict, query for elasticsearch
        """
        query = {
            "query" : {
                "constant_score": {
                    "filter": {
                        "bool": {
                            "must": [
                                {"term": {"wizard": wizard}}
                            ]
                        }
                    }
                }
            }
        }
        return query

    def perfect_match(self, data):
        data = preprocess_dict(data)
        query = {
            "query": {
                "constant_score": {
                    "filter": {
                        "bool": {
                            "must": [
                                {"term": {"product_description.match_analyzer": data["product_description"]}},
                                {"term": {"venue_category_id.raw": str(data["venue_category_id"])}},
                                {"term": {"product_category.match_analyzer": data["product_category"]}}
                            ]
                        }
                    }
                }
            }
        }
        return query

    def perfect_match_with_stemming(self, data):
        data = preprocess_dict(data, lowercase=True, alphanumspace=True, deldoublespace=True)
        product_category = " AND ".join( data["product_category"].split(" "))
        product_description = " AND ".join( data["product_description"].split(" "))
        venue_category_id = str(data["venue_category_id"])
        query = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "query_string": {
                                "fields": ["product_description.match_analyzer"],
                                "query": product_description,
                                "analyzer": "match_analyzer"
                            }
                        },
                        {
                            "query_string": {
                                "fields": ["venue_category_id"],
                                "query": venue_category_id
                            }
                        },
                        {
                            "query_string": {
                                "fields": ["product_category.match_analyzer"],
                                "query": product_category,
                                "analyzer": "match_analyzer"
                            }
                        }
                    ]
                }
            }
        }
        return query

    def perfect_match_with_stemming_get_parent(self, data):
        data = preprocess_dict(data, lowercase=True, alphanumspace=True, deldoublespace=True)
        product_category = " AND ".join(data["product_category"].split(" "))
        product_description = " AND ".join(data["product_description"].split(" "))
        venue_category_id = str(data["venue_category_id"])
        query = {
            "query": {
                "has_child": {
                    "type": "service",
                    "query": {
                        "bool": {
                            "must": [
                                {
                                    "query_string": {
                                        "fields": ["product_description"],
                                        "query": product_description
                                    }
                                },
                                {
                                    "query_string": {
                                        "fields": ["venue_category_id"],
                                        "query": venue_category_id
                                    }
                                },
                                {
                                    "query_string": {
                                        "fields": ["product_category"],
                                        "query": product_category
                                    }
                                }
                            ]
                        }
                    }
                }
            }
        }
        return query

    def perfect_match_2col_with_stemming_get_parent(self, data):
        data = preprocess_dict(data, lowercase=True, alphanumspace=True, deldoublespace=True)
        product_category = " AND ".join(data["product_category"].split(" "))
        product_description = " AND ".join(data["product_description"].split(" "))
        venue_category_id = str(data["venue_category_id"])
        query = {
            "query": {
                "has_child": {
                    "type": "service",
                    "query": {
                        "bool": {
                            "must": [
                                {
                                    "query_string": {
                                        "fields": ["product_description"],
                                        "query": product_description
                                    }
                                },
                                {
                                    "query_string": {
                                        "fields": ["venue_category_id"],
                                        "query": venue_category_id
                                    }
                                }
                            ]
                        }
                    }
                }
            }
        }
        return query

    def tom_query(self, data, size=3):
        query = {
            "query": {
                "bool": {
                    "should": [
                        {
                            "dis_max": {
                                "tie_breaker": 0.7,
                                "boost": 10.1,
                                "queries": [
                                    {
                                        "has_child": {
                                            "type": "service",
                                            # "score_mode": "sum",
                                            "query": {
                                                "match": {
                                                    "product_description.search_analyzer": data['product_description']
                                                }
                                            }
                                        }},
                                    {
                                        "match": {
                                            "name.search_analyzer": data['product_description']
                                        }
                                    }
                                ]
                            }
                        },
                        {
                            "dis_max": {
                                "tie_breaker": 0.2,
                                # "boost": 1.0,
                                "queries": [
                                    {
                                        "has_child": {
                                            "type": "service",
                                            "query": {
                                                "multi_match": {
                                                    "query": data['product_description'],
                                                    "fields": [
                                                        "product_category",
                                                        "product_description",
                                                        "venue_category",
                                                    ],
                                                }
                                            }
                                        }
                                    },
                                    {
                                        "match": {
                                            "full_wizard": data['product_description']
                                        }
                                    }
                                ]
                            }
                        },
                        {
                            "dis_max": {
                                # "tie_breaker": 0.1,
                                # "boost": 0.5,
                                "queries": [
                                    {
                                        "has_child": {
                                            "type": "service",
                                            # "score_mode": "sum",
                                            "query": {
                                                "match": {
                                                    "product_description.search_analyzer": data['product_category']
                                                }
                                            }
                                        }},
                                    {
                                        "match": {
                                            "name.search_analyzer": data['product_category']
                                        }
                                    }
                                ]
                            }
                        },
                        {
                            "dis_max": {
                                # "tie_breaker": 0.2,
                                # "boost": 1.0,
                                "queries": [
                                    {
                                        "has_child": {
                                            "type": "service",
                                            "query": {
                                                "multi_match": {
                                                    "query": data['product_category'],
                                                    "fields": [
                                                        "product_category",
                                                        "product_description",
                                                        "venue_category",
                                                    ],
                                                }
                                            }
                                        }
                                    },
                                    {
                                        "match": {
                                            "full_wizard": data['product_category']
                                        }
                                    }
                                ]
                            }
                        },
                        {
                            "dis_max": {
                                "tie_breaker": 0.1,
                                "boost": 0.5,
                                "queries": [
                                    {
                                        "has_child": {
                                            "type": "service",
                                            # "score_mode": "sum",
                                            "query": {
                                                "match": {
                                                    "product_description.search_analyzer": data['venue_category']
                                                }
                                            }
                                        }},
                                    {
                                        "match": {
                                            "name.search_analyzer": data['venue_category']
                                        }
                                    }
                                ]
                            }
                        },
                        {
                            "dis_max": {
                                "tie_breaker": 0.2,
                                "boost": 1.0,
                                "queries": [
                                    {
                                        "has_child": {
                                            "type": "service",
                                            "query": {
                                                "multi_match": {
                                                    "query": data['venue_category'],
                                                    "fields": [
                                                        "product_category",
                                                        "product_description",
                                                        "venue_category",
                                                    ],
                                                }
                                            }
                                        }
                                    },
                                    {
                                        "match": {
                                            "full_wizard": data['venue_category']
                                        }
                                    }
                                ]
                            }
                        }
                    ]
                }
            },
            "size": size
        }
        return query

    def tom_query_no_child(self, data):
        query = {
            "query": {
                "bool": {
                    "should": [
                        {
                            "dis_max": {
                                "tie_breaker": 0.7,
                                "boost": 10.1,
                                "queries": [
                                    {
                                        "match": {
                                            "name.search_analyzer": data['product_description']
                                        }
                                    }
                                ]
                            }
                        },
                        {
                            "dis_max": {
                                "tie_breaker": 0.2,
                                # "boost": 1.0,
                                "queries": [
                                    {
                                        "match": {
                                            "full_wizard": data['product_description']
                                        }
                                    }
                                ]
                            }
                        },
                        {
                            "dis_max": {
                                # "tie_breaker": 0.1,
                                # "boost": 0.5,
                                "queries": [
                                    {
                                        "match": {
                                            "name.search_analyzer": data['product_category']
                                        }
                                    }
                                ]
                            }
                        },
                        {
                            "dis_max": {
                                # "tie_breaker": 0.2,
                                # "boost": 1.0,
                                "queries": [
                                    {
                                        "match": {
                                            "full_wizard": data['product_category']
                                        }
                                    }
                                ]
                            }
                        },
                        {
                            "dis_max": {
                                "tie_breaker": 0.1,
                                "boost": 0.5,
                                "queries": [
                                    {
                                        "match": {
                                            "name.search_analyzer": data['venue_category']
                                        }
                                    }
                                ]
                            }
                        },
                        {
                            "dis_max": {
                                "tie_breaker": 0.2,
                                "boost": 1.0,
                                "queries": [
                                    {
                                        "match": {
                                            "full_wizard": data['venue_category']
                                        }
                                    }
                                ]
                            }
                        }
                    ]
                }
            },
            "size": 10
        }
        return query

    def fabien_query_no_child(self, data):
        full_words = " ".join([data['product_description'], data['product_category'], data['venue_category']])
        query = {
            "query": {
                "bool": {
                    "should": [
                        {
                            "dis_max": {
                                "queries": [
                                    {
                                        "match": {
                                            "name.search_analyzer": data['product_description']
                                        }
                                    }
                                ]
                            }
                        },
                        {
                            "dis_max": {
                                "queries": [
                                    {
                                        "match": {
                                            "category.search_analyzer": data['venue_category']
                                        }
                                    }
                                ]
                            }
                        },
                        {
                            "dis_max": {
                                "queries": [
                                    {
                                        "match": {
                                            "service.search_analyzer": data['product_category']
                                        }
                                    }
                                ]
                            }
                        },
                        {
                            "dis_max": {
                                "queries": [
                                    {
                                        "match": {
                                            "full_wizard": full_words,
                                        }
                                    }
                                ]
                            }
                        }
                    ]
                }
            },
            "size": 10
        }
        return query

    def alfred_query(self, data):
        full_words = " ".join([data['product_description'], data['product_category'], data['venue_category']])
        query = {
            "query": {
                "bool": {
                    "should": [
                        {
                            "dis_max": {
                                "queries": [
                                    {
                                        "has_child": {
                                            "type": "service",
                                            "query": {
                                                "match": {
                                                    "name.search_analyzer": data['product_description']
                                                }
                                            }
                                        }},
                                    {
                                        "match": {
                                            "name.search_analyzer": data['product_description']
                                        }
                                    }
                                ]
                            }
                        },
                        {
                            "dis_max": {
                                "queries": [
                                    {
                                        "has_child": {
                                            "type": "service",
                                            "query": {
                                                "match": {
                                                    "industry.search_analyzer": data['venue_category']
                                                }
                                            }
                                        }},
                                    {
                                        "match": {
                                            "category.search_analyzer": data['venue_category']
                                        }
                                    }
                                ]
                            }
                        },
                        {
                            "dis_max": {
                                "queries": [
                                    {
                                        "has_child": {
                                            "type": "service",
                                            "query": {
                                                "match": {
                                                    "venue_category.search_analyzer": data['venue_category']
                                                }
                                            }
                                        }},
                                    {
                                        "match": {
                                            "service.search_analyzer": data['product_category']
                                        }
                                    }
                                ]
                            }
                        },
                        {
                            "dis_max": {
                                "queries": [
                                    {
                                        "has_child": {
                                            "type": "service",
                                            "query": {
                                                "match": {
                                                    "venue_name.search_analyzer": data['venue_name']
                                                }
                                            }
                                        }},
                                    {
                                        "match": {
                                            "name.search_analyzer": data['product_description']
                                        }
                                    }
                                ]
                            }
                        },
                        {
                            "dis_max": {
                                "queries": [
                                    {
                                        "has_child": {
                                            "type": "service",
                                            "query": {
                                                "multi_match": {
                                                    "query": full_words,
                                                    "fields": [
                                                        "venue_category.search_analyzer",
                                                        "product_description.search_analyzer",
                                                        "product_category.search_analyzer"
                                                    ]
                                                }
                                            }
                                        }
                                    },
                                    {
                                        "match": {
                                            "full_wizard": full_words
                                        }
                                    }
                                ]
                            }
                        }
                    ]
                }
            },
            "size": 10
        }
        return query

