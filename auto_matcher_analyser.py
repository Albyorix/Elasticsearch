from collections import defaultdict
from csv import QUOTE_ALL
from pprint import pprint

from numpy import std
import pandas as pd
from tqdm import tqdm

from io_tools import get_filtered_data
from query_elasticsearch import RunningAutoServicematcher
from utils import IntDefaultDict, time_function, get_names_from_ml_index, get_logging

log = get_logging(__name__)


class ElasticsearchAnalyser:

    def __init__(self, formatted_inputs, autoservicematcher, query_name, filtering_querie_names, out_filepath, indexs, limit=-1):
        """
        :param formatted_input: List of dict, with at least:
            "category_id", "service_name", "venue_name", "country", "venue_category"
        :param autoservicematcher: AutoServicematcher,
        :param limit: int,
        """
        self.products = []
        for formatted_input in formatted_inputs:
            if 0 < limit < len(formatted_input):
                self.products.append(formatted_input[:limit])
            else:
                self.products.append(formatted_input)
        self.nb_services = [len(products) for products in self.products]
        self.indexs = indexs

        self.asm = autoservicematcher
        self.query_name = query_name
        self.filtering_querie_names = filtering_querie_names
        self.out_filepath = out_filepath
        self.nb_data_set = len(formatted_inputs)
        self.nb_level = 5

        self.dds = [defaultdict(IntDefaultDict) for _ in range(self.nb_data_set)]
        self.save_dd = defaultdict(IntDefaultDict)

        self.level_names = ["level_" + str(i) for i in range(1, 6)]
        self.level_values = [5, 11, 17, 23, 29]
        self.levels = [(self.level_names[i], self.level_values[i]) for i in range(5)]
        self.sum_i = ["sum_" + str(i) for i in range(1, 6)]
        self.pct_i = ["pct_" + str(i) for i in range(1, 6)]
        self.index_columns = ["Business", "Category", "Subcategory", "Service", "Service Name"]
        core_columns = ["wizard", "filtered", "chosen", "not_chosen"]
        kpi_columns = core_columns + self.sum_i
        save_columns = self.level_names + core_columns + self.sum_i + self.pct_i + self.index_columns

        self.kpi_dfs = [pd.DataFrame(columns=kpi_columns) for _ in range(self.nb_data_set)]
        self.save_df = pd.DataFrame(columns=save_columns)
        self.nb_chain = 0

        multilingual_index_filepath = "index/wizard2016/multilingual_index.csv"
        self.ml_index = pd.read_csv(multilingual_index_filepath, index_col="code")

    @time_function
    def run(self):
        for i in range(self.nb_data_set):
            log.info("Start analysing dataset {} on index {}".format(i, self.indexs[i]))
            log.info("Using query name : {}".format(self.query_name))
            is_filtered = False
            for product in tqdm(self.products[i]):
                if "chain" in product and product["chain"]: self.nb_chain += 1
                # if "chain" in product and not product["chain"]:
                #     continue
                for query_name in self.filtering_querie_names:
                    # Filtering queries - #TODO
                    # self.query, self.query_name = self.asm.get_query_by_name(self.query_name, product)
                    #
                    #
                    # out = self.asm.try_to_automatch_new_index(self.indexs[i], product)
                    # if out:
                    #     is_filtered = True
                    #     product["wizard"], _ = out
                    #     break
                    pass
                if not is_filtered:
                    # exact_match_query = getattr(queries, self.query_name)(product)
                    # res = self.asm.es.search(index=self.indexs[i], doc_type=parent_doc_type, body=exact_match_query)
                    # l = res["hits"]["total"]
                    # if l == 1:
                    #     parent = res["hits"]["hits"][0]
                    #     parent_wizard = parent["_source"]["wizard"]
                    #     return parent_wizard

                    out = self.asm.run_query_by_name(self.query_name, self.indexs[i], product)
                    if out:
                        product["wizard"] = out
                    else:
                        # print "NOT FOUND", product["product_description"], product["product_category"], product["venue_category_id"]
                        product["wizard"] = None

                for level_name, level_len in self.levels:
                    if is_filtered:
                        # Filtered from already working algo
                        self.dds[i][product["wizard_gt"][:level_len]]["filtered"] += 1
                        self.save_dd[product["wizard_gt"][:level_len]]["filtered"] += 1
                    elif product["wizard"] is None:
                        # Send to Human Matcher
                        self.dds[i][product["wizard_gt"][:level_len]]["not_chosen"] += 1
                        self.save_dd[product["wizard_gt"][:level_len]]["not_chosen"] += 1
                    else:
                        # Automatched
                        self.dds[i][product["wizard_gt"][:level_len]]["chosen"] += 1
                        self.save_dd[product["wizard_gt"][:level_len]]["chosen"] += 1
                        value = 1 if (product["wizard"][:level_len] == product["wizard_gt"][:level_len]) else 0
                        for level_name2, level_len2 in self.levels:
                            self.dds[i][product["wizard_gt"][:level_len2]][level_name] += value
                            self.save_dd[product["wizard_gt"][:level_len2]][level_name] += value

    @time_function
    def run_and_save_for_qc(self):
        columns = ["product_description", "product_category", "venue_category_id", "wizard_gt"] \
                  + self.index_columns \
                  + ["level_" + str(i) for i in range(1, 6)] \
                  + ["found_number", "found_product_description", "found_product_category", "found_venue_category_id", "found_wizard"] \
                  + self.index_columns
        self.qc_df = pd.DataFrame(columns=columns)
        for i in range(self.nb_data_set):
            for product in tqdm(self.products[i]):

                prod = [product["product_description"],
                        product["product_category"],
                        str(product["venue_category_id"]),
                        product["wizard_gt"]]
                found_prod = None

                exact_match_query = self.asm.get_query_by_name(self.query_name, product)
                res = self.asm.es.search(index=self.indexs[i], doc_type=self.asm.child_doc_type, body=exact_match_query)
                if self.asm.get_result_bool(res["hits"]["total"]):
                    parent = res["hits"]["hits"][0]
                    parent_wizard = parent["_source"]["wizard_gt"]
                    found_prod = [str(res["hits"]["total"]),
                                  parent["_source"]["product_description"],
                                  parent["_source"]["product_category"],
                                  str(parent["_source"]["industry_id"]),
                                  parent_wizard]
                    found_prod = [p.encode('ascii', 'ignore') for p in found_prod]
                prod = [p.encode('ascii', 'ignore') for p in prod]
                if found_prod is None:
                    row = prod + [ "" for _ in range(20)]
                else:
                    row = prod
                    row += get_names_from_ml_index(self.ml_index, product["wizard_gt"])
                    for k in self.level_values:
                        value = 1 if parent_wizard[:k] == product["wizard_gt"][:k] else 0
                        row.append(value)
                    row += found_prod
                    row += get_names_from_ml_index(self.ml_index, parent_wizard)

                self.qc_df.loc[len(self.qc_df)] = row
        log.info("Saving file here: {}".format(self.out_filepath))
        self.qc_df.to_csv(self.out_filepath, index=False, quoting=QUOTE_ALL)

    @time_function
    def print_kpi(self):
        self.process_output_dd()
        sum_chosen = sum([sum(df["chosen"]) for df in self.kpi_dfs]) / float(self.nb_level)
        sum_total = sum([self.nb_services[i] for i in range(self.nb_data_set)])
        pct_chosen = 0 if not sum_total else round(sum_chosen / sum_total * 100, 2)
        log.info("{nb_set} datasets containing {pct_chosen}% chosen services out of {nb_serv}".format(
            nb_serv=sum_total, pct_chosen=pct_chosen, nb_set=self.nb_data_set))

        kpis = [[] for _ in range(self.nb_level)]
        for i in range(self.nb_data_set):
            df = self.kpi_dfs[i]
            total_chosen = sum(df["chosen"])
            total = total_chosen + sum(df["not_chosen"])
            pct_chosen = 0 if not total else round(total_chosen / total * 100, 2)
            log.info("Dataset {set} contains {pct_chosen}% chosen services out of {nb_serv}".format(
                set=i+1, pct_chosen=pct_chosen, nb_serv=self.nb_services[i]))
            for j in range(self.nb_level):
                kpis[j].append(sum(df[self.sum_i[j]] / float(total_chosen)))

        for i in range(self.nb_level):
            pct = round(sum(kpis[i]) / float(self.nb_data_set) * 100, 2)
            level_std = round(std(kpis[i]) * 100, 2)
            log.info("Level {level} accuracy: {pct:2}% +/- {std:2}% standard dev".format(level=i+1, pct=pct, std=level_std))

    def process_output_dd(self):
        for i in range(self.nb_data_set):
            log.info("Analysing data set {set}/{total_set}".format(set=i+1, total_set=self.nb_data_set))
            for wizard, subdico in list(self.dds[i].iteritems()):
                row = [wizard, subdico["filtered"], subdico["chosen"], subdico["not_chosen"]]
                row += [subdico[name] for name in self.level_names]
                self.kpi_dfs[i].loc[len(self.kpi_dfs[i])] = row

    def process_save_dd(self):
        log.info("Analysing save data to save file")
        for wizard, subdico in tqdm(list(self.save_dd.iteritems())):
            row = []
            for value in self.level_values:
                if wizard[:value] in row:
                    row.append("")
                else:
                    row.append(wizard[:value])
            row += [wizard, subdico["filtered"], subdico["chosen"], subdico["not_chosen"]]
            sums = [subdico[name] for name in self.level_names]
            pcts = []
            for i in range(self.nb_level):
                pcts.append(float(sums[i]) / subdico["chosen"] if subdico["chosen"] else 0)
            row += sums + pcts
            row += get_names_from_ml_index(self.ml_index, wizard)
            self.save_df .loc[len(self.save_df)] = row

    @time_function
    def save(self):
        self.process_save_dd()
        log.info("Saving file here: {}".format(self.out_filepath))
        self.save_df.to_csv(self.out_filepath, index=False, quoting=QUOTE_ALL)

if __name__ == "__main__":
    limit = -1
    datasets = [0]
    filtering_querie_names = []
    studied_query_name = "perfect_match_get_parent"
    out_filepath = "my_csv/for_qcXX.csv"
    queries = "old"
    generic_index = "english"
    result_filter = ">=1"
    env = "local"

    indexs = [generic_index + str(i) for i in datasets]
    datatest_filepaths = ["datasets/data_test_0" for i in datasets]

    datas = [get_filtered_data(use_cache=datatest_filepath) for datatest_filepath in datatest_filepaths]
    asm = RunningAutoServicematcher(env, queries, result_filter)

    esa = ElasticsearchAnalyser(datas, asm, studied_query_name, filtering_querie_names, out_filepath, indexs=indexs, limit=limit)
    esa.run()
    esa.print_kpi()
    esa.save()
    # esa.run_and_save_for_qc()