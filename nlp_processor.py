import re
import json
import os
from pprint import pprint

from pattern.text.en import singularize
from nltk.stem import PorterStemmer
from tqdm import tqdm
from google.cloud import language

from modules.warehouse.service_compiler.auto_servicematcher import AutoServicematcher


class PreProcessor:

    def __init__(self):
        self.stemmer = PorterStemmer()

    def preprocessing(self, string_to_process):
        self.x = string_to_process.lower()
        self.keep_only_alphanumeric()
        self.x_list = self.x.split(" ")
        self.stem_sentences()
        self.del_stopwords()
        self.x = "".join(self.x_list)
        return self.x

    def singularize_sentences(self):
        """
        !!! wtf: transform "colour" into "colmy" !!!
        Transform this:
            ["caresses", "flies", "dies", "mules", "geese", "mice", "bars", "foos", "families", "dogs", "children", "wolves", "colour"]
        Into this:
            ["caress", "fly", "dy", "mule", "goose", "mouse", "bar", "foo", "family", "dog", "child", "wolf", "colmy"]
        :return: list of str, sentence containing only singular words
        """
        self.x_list = [singularize(word) for word in self.x_list]

    def stem_sentences(self):
        """
        Transform this:
            ["caresses", "flies", "dies", "mules", "geese", "mice", "bars", "foos", "families", "dogs", "children", "wolves", "colour"]
        Into this:
            ["caress", "fli", "die", "mule", "gees", "mice", "bar", "foo", "famili", "dog", "children", "wolv", "colour"]
        :return: list of str, sentence containing only singular words
        """
        self.x_list = [self.stemmer.stem(word) for word in self.x_list]

    def del_stopwords(self):
        """
        Transform this :
            ["this", "is", "a", "simple", "sentence"]
        Into this :
            ["this", "simple", "sentence"]
        :return: list of str, sentence containing only singular words
        """
        stop_words = ['the', 'a', 's', 'to', 'or', 'by', 'on', 'is', 'i', 'am', 'it', 'as', 'an', 'so', 'if', 'had']
        self.x_list = [word for word in self.x_list if word not in stop_words]

    def keep_only_alphanumeric(self):
        self.x = re.sub(r"[\W_]", " ", self.x)
        self.x = re.sub(r" +", " ", self.x)


def analyse_stem():
    def get_datasets():
        cache = "my_csv/stem_cache.json"
        if os.path.isfile(cache):
            with open(cache) as f:
                d = json.loads(f.read())
                train_set, test_set, train_keys, test_keys = d
        else:
            train_filepath = "my_csv/data_train.json"
            test_filepath = "my_csv/data_test.json"
            with open(train_filepath) as f:
                train_set = json.loads(f.read())
            with open(test_filepath) as f:
                test_set = json.loads(f.read())

            train_keys = []
            test_keys = []

            p = PreProcessor()
            for product in tqdm(train_set):
                key = "|".join([p.preprocessing(product["product_description"]),
                                p.preprocessing(product["product_category"]),
                                product["venue_category_id"]])
                train_keys.append(key)
            for product in tqdm(test_set):
                key = "|".join([p.preprocessing(product["product_description"]),
                                p.preprocessing(product["product_category"]),
                                product["venue_category_id"]])
                test_keys.append(key)
            d = [train_set, test_set, train_keys, test_keys]
            with open(cache, "w") as f:
                f.write(json.dumps(d))
        print "Datasets loaded"
        return train_set, test_set, train_keys, test_keys

    def alfred_stem_for_print(train_keys, test_keys, train_set, test_set):
        # Alfred stem
        alfred_stem = 0
        k = 0
        brk = False
        for i in tqdm(range(len(test_keys))):
            for j in range(len(train_keys)):
                if test_keys[i] == train_keys[j]:
                    pprint(test_set[i])
                    pprint(train_set[j])
                    alfred_stem += 1
                    k += 1
                    if k > 1:
                        brk = True
                    break

            if brk:
                break
        print alfred_stem

    def alfred_stem(train_keys, test_keys, train_set, test_set):
        alfred_stem = 0
        for key in tqdm(test_keys):
            if key in train_keys:
                alfred_stem += 1
        return alfred_stem

    def elastic_stem(train_set, test_set):
        asm = AutoServicematcher()
        elastic_stem = 0
        for product in tqdm(test_set):
            r = asm.try_to_automatch_service(product)
            if r is not None:
                elastic_stem += 1
        return elastic_stem

    train_set, test_set, train_keys, test_keys = get_datasets()
    stem1 = alfred_stem(train_keys, test_keys, train_set, test_set)
    stem2 = elastic_stem(train_set, test_set)
    pct1 = round(stem1 / float(len(test_keys)) * 100, 2)
    pct2 = round(stem2 / float(len(test_keys)) * 100, 2)
    point = abs(pct1 - pct2)
    pct = round(point / pct2 * 100, 2)
    print "Alfred stem: {pct1}% compared to Elastic stem: {pct2}%".format(pct1=pct1, pct2=pct2)
    print "There is: {point} points diff, or {pct}% diff".format(point=point, pct=pct)

    def simple_test():
        test1 = {u'chain': False,
                 u'country': u'gb',
                 u'product_category': u'Hair Colour',
                 u'product_description': u'highlights full head',
                 u'venue_category': u'other',
                 u'venue_category_id': u'1000',
                 u'venue_name': u'Toni and Guy Glasgow',
                 u'wizard_gt': u'01000_00100_00200_00100_00700'}
        train1 = {u'chain': False,
                  u'country': u'gb',
                  u'product_category': u'Hair Colour',
                  u'product_description': u'highlights full head',
                  u'venue_category': u'other',
                  u'venue_category_id': u'1000',
                  u'venue_name': u'Toni and Guy Epsom',
                  u'wizard_gt': u'01000_00100_00200_00100_00700'}

        asm = AutoServicematcher()
        r = asm.try_to_automatch_service(test1)
        print r
        r = asm.try_to_automatch_service(train1)
        print r


def get_verb_and_noun_from_google_language_api(text):
    language_client = language.Client()
    document = language_client.document_from_text(text)
    syntax = document.analyze_syntax()
    words = []
    for i in syntax:
        if i.part_of_speech == "NOUN" or i.part_of_speech == "VERB":
            words.append(i.lemma)
    return " ".join(words)


if __name__ == "__main__":
    text = 'Hairs are washed, and Colorations Woman under (13 year)'
    print get_verb_and_noun_from_google_language_api(text)



