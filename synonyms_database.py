from tqdm import tqdm
from PyDictionary import PyDictionary

from synonyms import get_word_usage_dict_in_service
from utils import sort_dico_in_list


dictionary = PyDictionary()


def get_file_length(filepath):
    with open(filepath) as f:
        for i, _ in enumerate(f):
            pass
    return i + 1


def clean_synonym_data(dico, limit=-1):
    """
    # Elasticsearch neads a text file with linea:
    i-pod, i pod => ipod,
    sea biscuit, sea biscit => seabiscuit

    # Multiple synonym mapping entries are merged.
    foo => foo bar
    foo => baz
    # is equivalent to
    foo => foo bar, baz
    """
    synonym_db_filepath = "my_csv/synonyms/th_en_US"
    synonym_db_filepath_save = "my_csv/synonyms/th_en_US_processed"

    filelen = get_file_length(synonym_db_filepath)
    if limit != -1:
        filelen = limit

    synonym_db = open(synonym_db_filepath)
    synonym_db.readline()
    save_db = open(synonym_db_filepath_save, "w")

    i = 1
    while i < filelen:
        i += 1
        line = synonym_db.readline()[:-1]
        words = line.split("|")
        word, nb_line = words[0], words[1]
        synonyms = [word]
        for _ in range(int(nb_line)):
            i += 1
            line = synonym_db.readline()[:-1]
            words = line.split("|")
            synonyms += words[1:]
        synonyms = [syn for syn in synonyms if len(syn) >= 2]
        addline = False
        for word in synonyms:
            if word in dico:
                addline = True
                break
        if addline:
            newline = word + " => " + ", ".join(synonyms) + "\n"
            save_db.write(newline)

    synonym_db.close()
    save_db.close()


def my_nltk():
    import textwrap
    from nltk.corpus import wordnet

    POS = {
        'v': 'verb', 'a': 'adjective', 's': 'satellite adjective',
        'n': 'noun', 'r': 'adverb'}
    print wordnet

    def info(word, pos=None):
        print wordnet.synsets(word, pos)
        for i, syn in enumerate(wordnet.synsets(word, pos)):
            print syn.lemma_names
            syns = [n for n in syn.lemma_names]
            print syns
            ants = [a for m in syn.lemmas for a in m.antonyms()]
            ind = ' ' * 12
            defn = textwrap.wrap(syn.definition, 64)
            print 'sense %d (%s)' % (i + 1, POS[syn.pos])
            print 'definition: ' + ('\n' + ind).join(defn)
            print '  synonyms:', ', '.join(syns)
            if ants:
                print '  antonyms:', ', '.join(a.name for a in ants)
            if syn.examples:
                print '  examples: ' + ('\n' + ind).join(syn.examples)
            print
    info('near')


def save_syn_from_service_words(dico, limit=1000):
    l = sort_dico_in_list(dico)
    synonym_db_filepath_save = "my_csv/synonyms/PyDict_processed_no_digit"
    save_db = open(synonym_db_filepath_save, "w")
    i = 0

    for word, count in tqdm(l):
        word = word.replace(",", "").replace("(", "").replace(")", "")
        synonyms =  dictionary.synonym(word)
        if not synonyms:
            continue
        dont_add_this = False
        for my_word in [word] + synonyms:
            for char in my_word:
                if char.isdigit():
                    dont_add_this = True
                    break
            if dont_add_this:
                break
        if dont_add_this:
            continue

        try:
            newline = word + " => " + ", ".join(synonyms) + "\n"
            save_db.write(newline)
            i += 1
            if i > limit:
                break
        except:
            pass
    save_db.close()


if __name__ == "__main__":
    word_usage_filepath = "my_csv/synonyms/word_usage_cache.json"
    use_cache = False
    limit = -1
    dico = get_word_usage_dict_in_service(use_cache=use_cache, save_cache=word_usage_filepath, limit=limit)

    # limit = -1
    # clean_synonym_data(dico, limit)
    save_syn_from_service_words(dico)