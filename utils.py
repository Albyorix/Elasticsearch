from collections import defaultdict, MutableMapping
import logging
import time

import matplotlib.pyplot as plt


def get_logging(logger_name):
    logging.basicConfig()
    log = logging.getLogger(logger_name)
    log.setLevel(logging.INFO)
    return log


time_log = get_logging("Time")


def time_function(func):
    def timed_func(*args, **kwargs):
        start_time = time.clock()
        r = func(*args, **kwargs)
        time_log.info("Running {func} took {sec} sec".format(func=func.__name__, sec=round(time.clock() - start_time, 2)))
        return r
    return timed_func


class IntDefaultDict(defaultdict):
    def __init__(self):
        super(IntDefaultDict, self).__init__(int)


class DDodIntDD(defaultdict):
    def __init__(self):
        super(DDodIntDD, self).__init__(IntDefaultDict)


def get_names_from_ml_index(ml_df, wizard):
    names = []
    wizards = wizard.split("_")
    for j in range(1, 6):
        try:
            name = ml_df.loc["_".join(wizards[:j]), "english"]
        except:
            return ["" for _ in range(5)]
        if name in names:
            names.append("")
        else:
            names.append(name)
    return names


def sort_dico_in_list(dico):
    """
    Transform dict into sorted list
    :param dico: dict,
    :return: list,
    """
    l = list(dico.iteritems())
    l.sort(key=operator.itemgetter(1), reverse=True)
    return l


def flatten(d, parent_key='', sep=','):
    """
    Flattens a nested dictionary
    :param d: dict,
    :param parent_key: for recursion
    :param sep: what to seperated nested dict with
    :return: dict with only one level depth
    """
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


class DictPlotter:

    def __init__(self, dico):
        """
        :param dico: {"name": int, value ...}
        """
        self.dico = dico

    def plot_dict_value_by_key_name(self):
        values = []
        legends = []
        for key, value in self.dico.iteritems():
            values.append(value)
            legends.append(key)
        plt.bar(range(len(self.dico)), values, align="center")
        plt.xticks(range(len(self.dico)), legends)
        plt.show()

    def get_appearance_dict(self):
        self.appearance = defaultdict(int)
        for value in self.dico.values():
            self.appearance[value] += 1

    def get_buckets(self):
        maxvalue = max(self.appearance.keys())
        if maxvalue < self.size:
            maxvalue = self.size
        self.buckets = [i * maxvalue / self.size + 1 for i in range(self.size)]

    def get_legend_from_buckets(self):
        self.legends = []
        for i in range(len(self.buckets) - 1):
            if self.buckets[i] == self.buckets[i + 1] - 1:
                self.legends.append(str(self.buckets[i]))
            else:
                self.legends.append(str(self.buckets[i]) + "-" + str(self.buckets[i + 1] - 1))
        self.legends.append("<=" + str(self.buckets[-1]))

    def get_value_from_buckets(self):
        self.values = [0 for _ in range(len(self.buckets))]
        for appearance, value in self.appearance.iteritems():
            for i in range(len(self.buckets))[::-1]:
                if appearance >= self.buckets[i]:
                    self.values[i] += value
                    break

    def plot_dict_freq(self, size):
        self.size = size
        self.get_appearance_dict()
        self.get_buckets()
        self.get_legend_from_buckets()
        self.get_value_from_buckets()

        plt.bar(range(size), self.values, align="center")
        plt.xticks(range(size), self.legends)
        plt.show()
