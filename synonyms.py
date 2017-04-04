from collections import defaultdict, MutableMapping
import json
from pprint import pprint
import operator

from tqdm import tqdm
import pandas as pd
from nltk.stem import PorterStemmer

from utils import DDodIntDD, IntDefaultDict, get_names_from_ml_index, get_logging, sort_dico_in_list, flatten

log = get_logging(__name__)

stemmer = PorterStemmer()


class PowerWordMatcher:

    def __init__(self, power_filepath="datasets/power_words.csv"):
        with open(power_filepath) as f:
            self.word_to_wizard = json.loads(f.read())

    def run_level1(self, product):
        """
        Get the level 1 category of the found power word in the services.
        It there are more than one, then return None
        """
        service = product["product_description"]
        service = "".join([char for char in service if (char.isalpha() or char == " ")])
        words = service.split(" ")
        wizards = []
        for word in words:
            if word in self.word_to_wizard:
                if self.word_to_wizard[word] not in wizards:
                    wizards.append(self.word_to_wizard[word])
        if len(wizards) == 1:
            return wizards[0]


def get_word_usage_dict_in_service(use_cache=False, save_cache=False, limit=100):
    """
    Get a dict with all words in service name, with the number of time it's been used
    :param use_cache: str,
    :param save_cache: str,
    :param limit: int,
    :return: dict,
    """
    if use_cache:
        with open(use_cache) as f:
            dico = json.loads(f.read())
    else:
        my_services_filepath = "datasets/product_and_subdomain_full_gb.csv"
        df = pd.read_csv(my_services_filepath)
        dico = defaultdict(int)
        for service in tqdm(df["prod_description"][:limit].values):
            words = service.split(" ")
            for word in words:
                dico[word] += 1
        dico = dict(dico)
        if save_cache:
            with open(save_cache, "w") as f:
                f.write(json.dumps(dico))
    return dico


def get_potential_synonyms(input_filepath, level, limit=1000, stem=True):
    """
    Get the synonyms from the services from a production csv
    :param input_filepath: str,
    :param level: int, between 0 to 5
    :param limit: int,
    :param stem: boolean, should be use stemming to increase chances of merge
    :return: dict of dict, {"wizard": {"word1": "word2"} ... }
    """
    df = pd.read_csv(input_filepath)
    dico = defaultdict(DDodIntDD)
    for i in tqdm(range(len(df[:limit]))):
        wizard = df.loc[i, "prod_standard_cat"]
        if level == 0:
            key = "full"
        else:
            key = wizard[:6 * level - 1]
        service = df.loc[i, "prod_desc_index"]
        words = service.split(" ")
        if len(words) == 2:
            word1, word2 = words
            dico[key][word1][word2] += 1
            dico[key][word2][word1] += 1

    syn_dico = defaultdict(DDodIntDD)
    for key, subdico in dico.iteritems():
        for word1, word1dico in subdico.iteritems():
            for syn1 in word1dico.keys():
                for syn2 in word1dico.keys():
                    if syn1 >= syn2:
                        break
                    if stem:
                        if stemmer.stem(syn1.decode('utf-8')) != stemmer.stem(syn2.decode('utf-8')):
                            syn_dico[key][syn1][syn2] += 1
                    else:
                        if syn1 != syn2:
                            syn_dico[key][syn1][syn2] += 1

    output_dico = {}
    for key, subdico in syn_dico.iteritems():
        output_dico[key] = dict( (word, dict(subsubdico)) for word, subsubdico in subdico.iteritems() if len(list(subsubdico)) > 1)
    output_dico = dict( (word, dict(subdico)) for word, subdico in output_dico.iteritems() if len(list(subdico)) > 0)

    return output_dico


def save_synonym(dico, save_filepath):
    """
    Save the synonyms in a human readble format
    :param dico: dict,
    :param save_filepath: str,
    """
    dico = flatten(dico)
    data = []
    for key, value in tqdm(sorted(list(dico.iteritems()))):
        data.append(key + "," + str(value))
    with open(save_filepath, "w") as f:
        f.write("wizard,word1,word2,occurences\n")
        f.write("\n".join(data))


def save_all_level_synonym():
    """
    Run the synonyms analyses
    """
    limit = -1
    input_filepath = "datasets/product_and_subdomain_full_gb.csv"
    for level in range(6):
        save_filepath = "my_csv/synonyms/syn_stem_from_legacy_level" + str(level) + ".csv"
        dico = get_potential_synonyms(input_filepath, level, limit=limit)
        save_synonym(dico, save_filepath)


def get_power_word(input_filepath, level, limit=1000, min_appearance=2, top_per_cat=100):
    """
    Get the power words from the services from a production csv
    :param input_filepath: str,
    :param level: int, between 1 to 5
    :param limit: int,
    :param min_appearance: int, the minimum number of times a word appears
    :param top_per_cat: int, percentage of the words to keep by category
    :return: dict of list, {"wizard": ["word1", "word2"] ... }
    """
    if level not in range(1, 6):
        log.error("Level {} not allowed, only between 1 to 5".format(level))
        return

    df = pd.read_csv(input_filepath)
    dico = defaultdict(IntDefaultDict)
    for i in tqdm(range(len(df[:limit]))):
        wizard = df.loc[i, "prod_standard_cat"]
        key = wizard[:6 * level - 1]
        service = df.loc[i, "prod_desc_index"]
        service = "".join([char for char in service if (char.isalpha() or char == " ")])
        words = service.split(" ")
        for word in words:
            if len(word) > 1:
                dico[key][word] += 1

    wizards = sorted(list(dico.keys()))
    for i in tqdm(range(len(wizards))):
        for word1, _ in list(dico[wizards[i]].iteritems()):
            to_pop = False
            for j in range(i + 1, len(wizards)):
                for word2, _ in list(dico[wizards[j]].iteritems()):
                    if word1 == word2:
                        del dico[wizards[j]][word2]
                        to_pop = True
                        break
            if to_pop:
                del dico[wizards[i]][word1]

    new_dico = {}
    for wizard, subdico in dico.iteritems():
        sorted_words = sort_dico_in_list(subdico)
        words = [word for word, value in sorted_words if value >= min_appearance]
        new_dico[wizard] = words[:int(top_per_cat*len(words)/100.)]

    return new_dico


def save_readable_power_word(dico, save_filepath):
    """
    Save the power words in a human readble format
    :param dico: dict,
    :param save_filepath: str,
    """
    ml_index_filepath = "index/multilingual_index.csv"
    ml_df = pd.read_csv(ml_index_filepath, index_col="code")

    data = []
    for wizard, value in tqdm(sorted(list(dico.iteritems()))):
        names = get_names_from_ml_index(ml_df, wizard)
        line = '"' + '","'.join(names) + '","' + wizard + '","' + '","'.join(value) + '"'
        data.append(line)

    with open(save_filepath, "w") as f:
        f.write('"' + '","'.join(["level_" + str(i) for i in range(1,6)]))
        f.write('","wizard","power_words"\n')
        f.write("\n".join(data))


def save_power_word_for_algo(dico, save_filepath):
    """
    output in a good format for PowerWordMatcher
    :param dico: dict,
    :param save_filepath: str,
    """
    word_to_wizard = {}
    for wizard, words in tqdm(sorted(list(dico.iteritems()))):
        for word in words:
            word_to_wizard[word] = wizard
    with open(save_filepath, "w") as f:
        f.write(json.dumps(word_to_wizard))


def save_all_level_power_word():
    """
    Run the power word analyses
    """
    limit = -1
    min_appearance = 2
    top_per_cat = 70  # pct per category
    input_filepath = "datasets/product_and_subdomain_full_gb.csv"
    for level in range(1, 6):
        dico = get_power_word(input_filepath, level, limit=limit, min_appearance=min_appearance, top_per_cat=top_per_cat)
        readable_save_filepath = "my_csv/synonyms/power_word2_from_legacy_level" + str(level) + ".csv"
        save_readable_power_word(dico, readable_save_filepath)
        # for_algo_save_filepath = "my_csv/power_word_for_algo" + str(level) + ".csv"
        # save_power_word_for_algo(dico, for_algo_save_filepath)


if __name__ == "__main__":
    # save_all_level_power_word()
    PowerWordMatcher()
