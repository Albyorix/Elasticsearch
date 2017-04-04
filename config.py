from pprint import pprint
from utils import get_logging

log = get_logging(__name__)

level1 = "level1"
level2 = "level2"
level3 = "level3"
level4 = "level4"
level5 = "level5"
parent_doc_type = "index_element"
child_doc_type = "service"
negative_child_doc_type = "negative_service"
searched_child_doc_type = "searched_service"

generic_mapping = {
    "mappings": {
        parent_doc_type: {  # data held in level 5 of the index
            "properties": {
                level1: {  # Business
                    "type": "string",
                    "copy_to": "full_wizard",
                    "fields": {
                        "search_analyzer": {
                            "type": "string",
                            "analyzer": "search_analyzer"
                        },
                        "lower": {
                            "type": "string",
                            "analyzer": "lower_analyzer"
                        },
                        "search": {
                            "type": "string",
                            "analyzer": "autocomplete",
                            "search_analyzer": "search_analyzer"
                        },
                        "raw": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                    }
                },
                level2: {  # Category (in the index) or Industry
                    "type": "string",
                    "copy_to": "full_wizard",
                    "fields": {
                        "search_analyzer": {
                            "type": "string",
                            "analyzer": "search_analyzer"
                        },
                        "lower": {
                            "type": "string",
                            "analyzer": "lower_analyzer"
                        },
                        "search": {
                            "type": "string",
                            "analyzer": "autocomplete",
                            "search_analyzer": "search_analyzer"
                        },
                        "raw": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                    }
                },
                level3: {  # Subcategory
                    "type": "string",
                    "copy_to": "full_wizard",
                    "fields": {
                        "search_analyzer": {
                            "type": "string",
                            "analyzer": "search_analyzer"
                        },
                        "lower": {
                            "type": "string",
                            "analyzer": "lower_analyzer"
                        },
                        "search": {
                            "type": "string",
                            "analyzer": "autocomplete",
                            "search_analyzer": "search_analyzer"
                        },
                        "raw": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                    }
                },
                level4: {  # Service
                    "type": "string",
                    "copy_to": "full_wizard",
                    "fields": {
                        "search_analyzer": {
                            "type": "string",
                            "analyzer": "search_analyzer"
                        },
                        "lower": {
                            "type": "string",
                            "analyzer": "lower_analyzer"
                        },
                        "search": {
                            "type": "string",
                            "analyzer": "autocomplete",
                            "search_analyzer": "search_analyzer"
                        },
                        "raw": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                    }
                },
                level5: {  # level 5, Service Name (in the index)
                    "type": "string",
                    "fields": {
                        "search_analyzer": {
                            "type": "string",
                            "analyzer": "search_analyzer"
                        },
                        "lower": {
                            "type": "string",
                            "analyzer": "lower_analyzer"
                        },
                        "search": {
                            "type": "string",
                            "analyzer": "autocomplete",
                        },
                        "raw": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                    }
                },
                "level1_id":{
                    "type": "string",
                    "fields": {
                        "search_analyzer": {
                            "type": "string",
                            "analyzer": "search_analyzer"
                        },
                        "raw": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                    }
                },

                "tags": {  # tags if/when we add them
                    "type": "string",
                    "fields": {
                        "search_analyzer": {
                            "type": "string",
                            "analyzer": "search_analyzer"
                        },
                        "lower": {
                            "type": "string",
                            "analyzer": "lower_analyzer"
                        },
                        "search": {
                            "type": "string",
                            "analyzer": "autocomplete",
                            "search_analyzer": "search_analyzer"
                        },
                        "raw": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                    }
                },
                "is_hidden": {
                    "type": "boolean",
                    "index": "not_analyzed"
                },

                "wizard": {  # 25 digits for unique service
                    "type": "string",
                    "fields": {
                        "search_analyzer": {
                            "type": "string",
                            "analyzer": "search_analyzer"
                        },
                        "raw": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                    }
                },
                "picture1": {
                    "type": "string",
                    "index": "not_analyzed"
                },
                "picture2": {
                    "type": "string",
                    "index": "not_analyzed"
                },
            }
        },
        child_doc_type: {  # equivalent of level 6 : Service from the venue, Product in the warehouse
            "_parent": {
                "type": "index_element"
            },
            "properties": {
                "check_flag": { # junior matcher has check it. now it's going for the senior checker
                    "type": "boolean",
                    "index": "not_analyzed"
                },
                "last_fetch_date":{
                    "type": "date",
                    "format": "yyyy-MM-dd HH:mm:ss",
                    "index": True,
                },
                "user_id": {  # 1st junior id
                    "type": "integer",
                    "index": "not_analyzed"
                },
                "user_email": {  # 1st junior email
                    "type": "string",
                    "fields": {
                        "raw": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                    }
                },
                "use_for_automatch": { # both juniors agreed - can be used for automatch
                    "type": "boolean",
                    "index": "not_analyzed"
                },

                "product_key": { # key of product to map the warehouse
                    "type": "string",
                    "index": "not_analyzed"
                },
                "subdomain_key": { # key of subdomain to map the warehouse
                    "type": "string",
                    "index": "not_analyzed"
                },

                "product_category": {  # category of the service (not always filled & different from index category)
                    "type": "string",
                    "fields": {
                        "search_analyzer": {
                            "type": "string",
                            "analyzer": "search_analyzer"
                        },
                        "match_analyzer": {
                            "type": "string",
                            "analyzer": "match_analyzer"
                        },
                        "lower": {
                            "type": "string",
                            "analyzer": "lower_analyzer"
                        },
                        "search": {
                            "type": "string",
                            "analyzer": "autocomplete",
                            "search_analyzer": "search_analyzer"
                        },
                        "raw": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                    }
                },
                "product_description": {  # name of the service
                    "type": "string",
                    "fields": {
                        "search_analyzer": {
                            "type": "string",
                            "analyzer": "search_analyzer",
                        },
                        "match_analyzer": {
                            "type": "string",
                            "analyzer": "match_analyzer"
                        },
                        "lower": {
                            "type": "string",
                            "analyzer": "lower_analyzer"
                        },
                        "search": {
                            "type": "string",
                            "analyzer": "autocomplete",
                            "search_analyzer": "search_analyzer"
                        },
                        "raw": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                    }
                },
                "venue_category": { # Similar to level 1 but from the scrapes
                    "type": "string",
                    "fields": {
                        "search_analyzer": {
                            "type": "string",
                            "analyzer": "search_analyzer"
                        },
                        "match_analyzer": {
                            "type": "string",
                            "analyzer": "match_analyzer"
                        },
                        "search": {
                            "type": "string",
                            "analyzer": "autocomplete",
                            "search_analyzer": "search_analyzer"
                        },
                        "lower": {
                            "type": "string",
                            "analyzer": "lower_analyzer"
                        },
                        "raw": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                    }
                },
                "venue_name": {  # name of the venue (not used for coupling the level 6)
                    "type": "string",
                    "fields": {
                        "search_analyzer": {
                            "type": "string",
                            "analyzer": "search_analyzer"
                        },
                        "lower": {
                            "type": "string",
                            "analyzer": "lower_analyzer"
                        },
                        "search": {
                            "type": "string",
                            "analyzer": "autocomplete",
                            "search_analyzer": "search_analyzer"
                        },
                        "raw": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                    }
                },

                "venue_category_id": {
                    "type": "string",
                    "fields": {
                        "raw": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                    }
                },
            }
        },
        negative_child_doc_type: {  # Same as service but for negative boost
            "_parent": {
                "type": "index_element"
            },
            "properties": {
                "user_id": {  # 1st junior id to filter query for the second junior
                    "type": "integer",
                    "index": "not_analyzed"
                },
                "user_email": {  # 1st junior email
                    "type": "string",
                    "fields": {
                        "raw": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                    }
                },
                "product_category": {
                    "type": "string",
                    "fields": {
                        "search_analyzer": {
                            "type": "string",
                            "analyzer": "search_analyzer"
                        },
                        "lower": {
                            "type": "string",
                            "analyzer": "lower_analyzer"
                        },
                        "search": {
                            "type": "string",
                            "analyzer": "autocomplete",
                            "search_analyzer": "search_analyzer"
                        },
                        "raw": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                    }
                },
                "product_description": {
                    "type": "string",
                    "fields": {
                        "search_analyzer": {
                            "type": "string",
                            "analyzer": "search_analyzer",
                        },
                        "lower": {
                            "type": "string",
                            "analyzer": "lower_analyzer"
                        },
                        "search": {
                            "type": "string",
                            "analyzer": "autocomplete",
                            "search_analyzer": "search_analyzer"
                        },
                        "raw": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                    }
                },
                "venue_category": {
                    "type": "string",
                    "fields": {
                        "search_analyzer": {
                            "type": "string",
                            "analyzer": "search_analyzer"
                        },
                        "lower": {
                            "type": "string",
                            "analyzer": "lower_analyzer"
                        },
                        "search": {
                            "type": "string",
                            "analyzer": "autocomplete",
                            "search_analyzer": "search_analyzer"
                        },
                        "raw": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                    }
                },
                "venue_name": {
                    "type": "string",
                    "fields": {
                        "search_analyzer": {
                            "type": "string",
                            "analyzer": "search_analyzer"
                        },
                        "lower": {
                            "type": "string",
                            "analyzer": "lower_analyzer"
                        },
                        "raw": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                    }
                },
            }
        },
        searched_child_doc_type: {  # Same as service but that was searched for instead of pick from top 3 for higher boost
            "_parent": {
                "type": "index_element"
            },
            "properties": {
                "user_id": {  # 1st junior id to filter query for the second junior
                    "type": "integer",
                    "index": "not_analyzed"
                },
                "user_email": {  # 1st junior email
                    "type": "string",
                    "fields": {
                        "raw": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                    }
                },
                "product_category": {
                    "type": "string",
                    "fields": {
                        "search_analyzer": {
                            "type": "string",
                            "analyzer": "search_analyzer"
                        },
                        "lower": {
                            "type": "string",
                            "analyzer": "lower_analyzer"
                        },
                        "search": {
                            "type": "string",
                            "analyzer": "autocomplete",
                            "search_analyzer": "search_analyzer"
                        },
                        "raw": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                    }
                },
                "product_description": {
                    "type": "string",
                    "fields": {
                        "search_analyzer": {
                            "type": "string",
                            "analyzer": "search_analyzer",
                        },
                        "lower": {
                            "type": "string",
                            "analyzer": "lower_analyzer"
                        },
                        "search": {
                            "type": "string",
                            "analyzer": "autocomplete",
                            "search_analyzer": "search_analyzer"
                        },
                        "raw": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                    }
                },
                "venue_category": {
                    "type": "string",
                    "fields": {
                        "search_analyzer": {
                            "type": "string",
                            "analyzer": "search_analyzer"
                        },
                        "lower": {
                            "type": "string",
                            "analyzer": "lower_analyzer"
                        },
                        "search": {
                            "type": "string",
                            "analyzer": "autocomplete",
                            "search_analyzer": "search_analyzer"
                        },
                        "raw": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                    }
                },
                "venue_name": {
                    "type": "string",
                    "fields": {
                        "search_analyzer": {
                            "type": "string",
                            "analyzer": "search_analyzer"
                        },
                        "lower": {
                            "type": "string",
                            "analyzer": "lower_analyzer"
                        },
                        "raw": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                    }
                },
            }
        },
    }
}

generic_filter = {
    "mynGram": {  # Create tokens of length 3 to 50 from each word
        "type": "nGram",
        "min_gram": 3,
        "max_gram": 50
    },
    "unique": {  # Keep only unique token
        "type": "unique",
        "only_on_same_position": True,
    },
    "autocomplete_filter": {  # For the responsive search
        "type": "edge_ngram",
        "min_gram": 1,
        "max_gram": 20
    },
    "ascii_folding": {  # Delete accents and other weird letters
        "type": "asciifolding",
        "preserve_original": True
    },
    # Built in filters :
    # - lowercase / change to lowercase only
}

generic_analyzer = {
    "autocomplete": {
        "type": "custom",
        "tokenizer": "standard",
        "filter": [
            "lowercase",
            "autocomplete_filter"
        ]
    },
    "lower_analyzer": {
        "tokenizer": "keyword",
        "filter": "lowercase"
    }
}

language_settings = {
    #  Explanation of the settings
    #  "country":
    #     "settings": {
    #         "analysis": {
    #             "filter": {
    #                 "country_stop": {
    #                   stop words from the country
    #                   Delete words like "the" from the analysis
    #                 },
    #                 "country_stemmer": {
    #                   stemmer from the country
    #                   Transform "fisherman" into "fish"
    #                 },
    #             },
    #             "tokenizer": {
    #                 "analyzer_name": {
    #                   "letter" tokenizer means that only alpha caracters are kept
    #                   It works well for most european languages
    #                   words are broke down by spaces
    #                 }
    #             },
    #             "analyzer": {
    #                 "analyzer_name": {
    #                     "type": "custom",
    #                     "tokenizer": "letter",
    #                     "filter": [
    #                       filters are done one after the other in elastic
    #                     ]
    #                 },
    #             }
    #         }
    #     }
    # },
    "english": {
        "settings": {
            "analysis": {
                "filter": {
                    "english_stop": {
                        "type": "stop",
                        "stopwords": "_english_"
                    },
                    "english_stemmer": {
                        "type": "stemmer",
                        "language": "english"
                    },
                    "english_possessive_stemmer": {
                        "type": "stemmer",
                        "language": "possessive_english"
                    },
                },
                "analyzer": {
                    "match_analyzer": {
                        "type": "custom",
                        "tokenizer": "letter",
                        "filter": [
                            "keyword_repeat",
                            "lowercase",
                            "ascii_folding",
                            "english_possessive_stemmer",
                            "english_stop",
                            "english_stemmer",
                        ]
                    },
                    "search_analyzer": {
                        "tokenizer": "letter",
                        "filter": [
                            "lowercase",
                            "keyword_repeat",
                            "ascii_folding",
                            "english_possessive_stemmer",
                            "english_stop",
                            "english_stemmer",
                            "mynGram",
                            "unique",
                        ]
                    },
                },
            }
        }
    },
    "french": {
        "settings": {
            "analysis": {
                "filter": {
                    "french_elision": {
                        "type": "elision",
                        "articles_case": True,
                        "articles": [
                            "l", "m", "t", "qu", "n", "s",
                            "j", "d", "c", "jusqu", "quoiqu",
                            "lorsqu", "puisqu"
                        ]
                    },
                    "french_stop": {
                      "type":       "stop",
                      "stopwords":  "_french_"
                    },
                    "french_stemmer": {
                        "type": "stemmer",
                        "language": "light_french"
                    },
                },
                "analyzer": {
                    "match_analyzer": {
                        "type": "custom",
                        "tokenizer": "letter",
                        "filter": [
                            "keyword_repeat",
                            "lowercase",
                            "ascii_folding",
                            "french_elision",
                            "french_stop",
                            "french_stemmer"
                        ]
                    },
                    "search_analyzer": {
                        "tokenizer": "letter",
                        "filter": [
                            "lowercase",
                            "keyword_repeat",
                            "ascii_folding",
                            "french_elision",
                            "french_stop",
                            "french_stemmer",
                            "mynGram",
                            "unique",
                        ]
                    },
                }
            }
          }
        },
    "german": {
        "settings": {
            "analysis": {
                "filter": {
                    "german_stop": {
                        "type": "stop",
                        "stopwords": "_german_"
                    },
                    "german_stemmer": {
                        "type": "stemmer",
                        "language": "light_german"
                    }
                },
                "analyzer": {
                    "match_analyzer": {
                        "type": "custom",
                        "tokenizer": "letter",
                        "filter": [
                            "keyword_repeat",
                            "lowercase",
                            "ascii_folding",
                            "german_stop",
                            "german_stemmer",
                        ]
                    },
                    "search_analyzer": {
                        "tokenizer": "letter",
                        "filter": [
                            "lowercase",
                            "keyword_repeat",
                            "ascii_folding",
                            "german_stop",
                            "german_stemmer",
                            "mynGram",
                            "unique",
                        ]
                    }
                }
            }
        }
    },
    "spanish":  {
        "settings": {
            "analysis": {
                "filter": {
                    "spanish_stop": {
                      "type": "stop",
                      "stopwords": "_spanish_"
                    },
                    "spanish_stemmer": {
                      "type": "stemmer",
                      "language": "light_spanish"
                    }
                  },
                "analyzer": {
                    "match_analyzer": {
                        "type": "custom",
                        "tokenizer": "letter",
                        "filter": [
                            "keyword_repeat",
                            "lowercase",
                            "ascii_folding",
                            "spanish_stop",
                            "spanish_stemmer",
                        ]
                    },
                    "search_analyzer": {
                        "tokenizer": "letter",
                        "filter": [
                            "lowercase",
                            "keyword_repeat",
                            "ascii_folding",
                            "spanish_stop",
                            "spanish_stemmer",
                            "mynGram",
                            "unique",
                        ]
                    },
                }
            }
        }
    },
    "italian":  {
        "settings": {
            "analysis": {
                "filter": {
                    "italian_elision": {
                        "type": "elision",
                        "articles": [
                            "c", "l", "all", "dall", "dell",
                            "nell", "sull", "coll", "pell",
                            "gl", "agl", "dagl", "degl", "negl",
                            "sugl", "un", "m", "t", "s", "v", "d"
                        ]
                        },
                    "italian_stop": {
                      "type": "stop",
                      "stopwords": "_italian_"
                    },
                    "italian_stemmer": {
                        "type": "stemmer",
                        "language": "light_italian"
                    }
                },
                "analyzer": {
                    "match_analyzer": {
                        "type": "custom",
                        "tokenizer": "letter",
                        "filter": [
                            "keyword_repeat",
                            "lowercase",
                            "ascii_folding",
                            "italian_elision",
                            "italian_stop",
                            "italian_stemmer",
                        ]
                    },
                    "search_analyzer": {
                        "tokenizer": "letter",
                        "filter": [
                            "lowercase",
                            "keyword_repeat",
                            "ascii_folding",
                            "italian_elision",
                            "italian_stop",
                            "italian_stemmer",
                            "mynGram",
                            "unique",
                        ]
                    },
                },
            }
        }
    },
    "dutch":  {
        "settings": {
            "analysis": {
                "filter": {
                    "dutch_stop": {
                        "type": "stop",
                        "stopwords": "_dutch_"
                    },
                    "dutch_stemmer": {
                        "type": "stemmer",
                        "language": "dutch"
                    },
                    "dutch_override": {
                      "type":       "stemmer_override",
                      "rules": [
                        "fiets=>fiets",
                        "bromfiets=>bromfiets",
                        "ei=>eier",
                        "kind=>kinder"
                      ]
                    }
                },
                "analyzer": {
                    "match_analyzer": {
                        "type": "custom",
                        "tokenizer": "letter",
                        "filter": [
                            "keyword_repeat",
                            "lowercase",
                            "ascii_folding",
                            "dutch_stop",
                            "dutch_stemmer",
                            "dutch_override",
                        ]
                    },
                    "search_analyzer": {
                        "tokenizer": "letter",
                        "filter": [
                            "lowercase",
                            "keyword_repeat",
                            "ascii_folding",
                            "dutch_stop",
                            "dutch_stemmer",
                            "dutch_override",
                            "mynGram",
                            "unique",
                        ]
                    },
                }
            }
        }
    }
}

index_language_mapping = {
    "gb": "english",
    "fr": "french",
    "de": "german",
    "it": "italian",
    "es": "spain",
    "nl": "dutch",
    # "be": ["french", "dutch"],
    # "ch": ["french", "german", "italian"],
    # "au": ["german"],
    # "cz": ["czech"],
    # "dk": ["danish"],
    # "ie": ["english"],
    # "mc": ["french"],
    # "ru": ["russian"],
    # "se": ["swedish"],
    # "pt": ["portuguese"],
    # "gr": [""],
    # "ro": [""],
}


def update_config_with_synonyms(config, synonyms_path):
    # synonyms_path = "/home/alfred/projects/auto_servicematcher/my_csv/synonyms/pydict_1000_syn_no_digit.txt"
    with open(synonyms_path) as f:
        synonyms_infile = f.read()
    synonyms = synonyms_infile.split("\n")[:-1]
    synonyms_filter = {"type": "synonym", "synonyms": synonyms}
    filter_name = "synonyms_filter"
    config["settings"]["analysis"]["filter"][filter_name] = synonyms_filter
    # Add synonyms after lowercase and keyword repeat filters
    config["settings"]["analysis"]["analyzer"]["search_analyzer"]["filter"].insert(2, filter_name)


def get_language_config(my_language, use_synonym=False):
    """
    Get the config file for elasticsearch for the language
    :param my_language: st,
    :param use_synonym: str or False, path of the file
    :return:
    """
    if my_language not in language_settings:
        log.error("Settings for {} index does not exists yet.".format(my_language))
        return
    config = generic_mapping
    config.update(language_settings[my_language])
    config["settings"]["analysis"]["filter"].update(generic_filter)
    config["settings"]["analysis"]["analyzer"].update(generic_analyzer)
    if use_synonym:
        update_config_with_synonyms(config, use_synonym)
    return config


if __name__ == "__main__":
    for lang in ["french", "german", "spanish", "italian", "dutch"]:
        a = get_language_config(lang)
        pprint(a)