import pandas as pd
from tqdm import tqdm
import re
import csv
import json
from nltk.corpus import wordnet as wn
import copy
from nltk.corpus import stopwords
from itertools import chain
from collections import Counter, defaultdict
from PyDictionary import PyDictionary
import traceback


INTERESTING_COUNTRY = ['fr', 'gb', 'it', 'es', 'de', 'nl']


def concat_one_list(df_words):
    country_res = defaultdict(Counter)
    stops = set(stopwords.words("english"))
    stops.update(stopwords.words("french"))
    stops.update(stopwords.words("dutch"))
    stops.update(stopwords.words("german"))
    stops.update(stopwords.words("italian"))
    stops.update(stopwords.words("spanish"))
    for i in tqdm(range(len(df_words))):
        try:
            words_split = re.split(r'\W', df_words.loc[i, 'prod_desc_index'])
            word_to_add = [word for word in words_split if word not in stops and word != '' and word != ' ']
            country_res[df_words.loc[i, 'subdom_country']].update(word_to_add)
        except Exception:
            print df_words.loc[i, 'subdom_country']
            print df_words.loc[i, 'prod_desc_index']
    return country_res


def count_couples(df_words):
    country_dict = defaultdict(dict)
    stops = set(stopwords.words("english"))
    stops.update(stopwords.words("french"))
    stops.update(stopwords.words("dutch"))
    stops.update(stopwords.words("german"))
    stops.update(stopwords.words("italian"))
    stops.update(stopwords.words("spanish"))
    for i in tqdm(range(len(df_words))):
        country = df_words.loc[i, 'subdom_country']
        standard_cat = df_words.loc[i, 'prod_standard_cat']
        standard_cat_split = standard_cat.split("_")
        category_index = "_".join([standard_cat_split[0]]+[standard_cat_split[1]])  # level 2 index

        if not country_dict.get(country):
            country_dict[country] = defaultdict(dict)
        if not country_dict[country].get(category_index):
            country_dict[country][category_index] = defaultdict(Counter)
        try:
            only_words = re.split(r'\W', df_words.loc[i, 'prod_desc_index'])
        except Exception as error:
            print country
            print df_words.loc[i, 'prod_desc_index']
            print traceback.format_exc(error)
        else:
            only_words_wo_stops = [word for word in only_words if word not in stops and word != '' and word != ' ']
            for word in only_words_wo_stops:
                cp_words_list = copy.copy(only_words_wo_stops)
                cp_words_list.remove(word)
                country_dict[country][category_index][word].update(cp_words_list)
    return country_dict


def read_one_col(filename, cols):
    return pd.read_csv(filename, dtype=str)[cols]


def write_csv_freq(_dict):
        for ctry in _dict:

            if ctry in INTERESTING_COUNTRY:
                with open('my_csv/freq_by_country_'+ctry+'.csv', 'wb') as f:
                    fieldnames = ['country', 'word', 'nb']
                    writer = csv.DictWriter(f, fieldnames=fieldnames)

                    writer.writeheader()
                    list_words = sorted(_dict[ctry].items(), key=lambda x: x[1], reverse=True)

                    for item in list_words:
                        syno = ''
                        if ctry == 'gb':
                            syno = list(set(chain.from_iterable([word.lemma_names() for word in wn.synsets(item[0])])))
                            dictionary = PyDictionary(item[0])
                            # syno = list(dictionary.getSynonyms())
                        writer.writerow({'country': ctry, 'word': item[0], 'nb': item[1]})


def write_csv_couple(_dict):
        for ctry in tqdm(_dict):
            if ctry in INTERESTING_COUNTRY:
                with open('my_csv/couple_by_country_'+ctry+'.csv', 'wb') as f:
                    fieldnames = ['country', 'index', 'word_1', 'word_2', 'nb']
                    writer = csv.DictWriter(f, fieldnames=fieldnames)

                    writer.writeheader()
                    list_words = _dict[ctry].items()
                    for item in list_words:
                        index = item[0]
                        for ite in item[1]:
                            counter_dict = item[1][ite]
                            for syn in counter_dict:
                                if counter_dict[syn] > 10:
                                    writer.writerow({'country': ctry, 'index': index, 'word_1': ite, 'word_2': syn, 'nb': counter_dict[syn]})


if __name__ == "__main__":

    # df_service_name = read_one_col('my_csv/product_and_subdomain.csv', ['prod_desc_index', 'subdom_country', 'prod_standard_cat'])
    # df_service_name.to_csv('my_csv/count.csv', index=False, quoting=csv.QUOTE_ALL)
    df_service_name = pd.read_csv('my_csv/count.csv', dtype=str)
    df_service_name.drop_duplicates(inplace=True)
    df_service_name.reset_index(drop=True, inplace=True)
    # words_list_freq = concat_one_list(df_service_name)
    # with open('my_csv/cache_freq.json', 'wb') as f:
    #     json.dump(words_list_freq, f)
    # words_list_assoc = count_couples(df_service_name)
    # with open('my_csv/cache_assoc.json', 'wb') as f:
    #     json.dump(words_list_assoc, f)

    with open('my_csv/cache_freq.json', 'rb') as f:
        result = json.load(f)
    words_list_freq = result

    with open('my_csv/cache_assoc.json', 'rb') as f:
        result = json.load(f)
    words_list_assoc = result

    write_csv_couple(words_list_assoc)
    # write_csv_freq(words_list_freq)

