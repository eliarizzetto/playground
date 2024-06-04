from zipfile import ZipFile
from tqdm import tqdm
from csv import DictReader
from io import TextIOWrapper
from datetime import datetime
import logging
import csv
import json
from collections import defaultdict
import warnings
from pprint import pprint

def read_compressed_meta_dump(csv_dump_path: str):
    """
    Reads the archive zipping the CSV files of OC Meta CSV dump.
    :param csv_dump_path:
    :return:
    """
    csv.field_size_limit(131072 * 12)
    with ZipFile(csv_dump_path) as archive:
        for csv_file in tqdm(archive.namelist()):
            if csv_file.endswith('.csv'):
                logging.debug(f'Processing file {csv_file}')
                with archive.open(csv_file, 'r') as f:
                    reader = DictReader(TextIOWrapper(f, encoding='utf-8'), dialect='unix')
                    for row in reader:
                        yield row


def default_to_regular(d):
    """
    Recursively converts infinitely nested defaultdict object to a nested regular dictionary.
    :param d:
    :return:
    """
    if isinstance(d, defaultdict):
        d = {k: default_to_regular(v) for k, v in d.items()}
    return d


def convert_keys_to_int(d):
    """
    Recursively type-casts into integers all the keys of an infinitely nested dictionary that can be converted into integers.
    :param d:
    :return:
    """
    if isinstance(d, dict):
        new_dict = {}
        for key, value in d.items():
            try:
                new_key = int(key)
            except ValueError:
                new_key = key
            new_dict[new_key] = convert_keys_to_int(value)
        return new_dict
    else:
        return d


def sort_dict(d):
    """
    Recursively sorts infinitely nested dictionary by keys. Numerical strings are interpreted as integers for sorting, but not converted into integers in the output.
    :param d:
    :return:
    """

    if isinstance(d, dict):
        sorted_items = sorted(
            d.items(),
            key=lambda item: (int(item[0]) if (isinstance(item[0], str) and item[0].isdigit()) else item[0])
        )
        sorted_dict = {k: sort_dict(v) for k, v in sorted_items}
        return sorted_dict
    return d


def recursive_dict_sum(d):
    """
    Recursively sums all the numerical values of an infinitely nested dictionary, regardless of the key they are associated with.
    :param d:
    :return:
    """
    total = 0
    for value in d.values():
        if isinstance(value, dict):
            total += recursive_dict_sum(value)
        elif isinstance(value, int):
            total += value
    return total


def recursive_dict_filter(d, **kwargs):
    """
    Recursively filters out from an infinitely nested dictionary all key-value pairs where the key is a number and
    is lesser than the min value, or greater than the max value, or both.
    :param d: dictionary to be filtered
    :param kwargs:
        - :key: min (int, Optional): the minimum value for numerical keys to be kept.
        - :key: max (int, Optional): the maximum value for numerical keys to be kept.
    :return: filtered dictionary
    """

    min: int | None = int(kwargs.get('min', None)) if kwargs.get('min') else None
    max: int | None = int(kwargs.get('max', None)) if kwargs.get('max') else None

    if isinstance(d, dict):
        filtered_dict = {}
        for k, v in d.items():
            if isinstance(k, int):
                if min and max:
                    if min <= k <= max:
                        filtered_dict[k] = recursive_dict_filter(v, min=min, max=max)
                elif min and not max:
                    if k >= min:
                        filtered_dict[k] = recursive_dict_filter(v, min=min)
                elif max and not min:
                    if k <= max:
                        filtered_dict[k] = recursive_dict_filter(v, max=max)
            else:
                filtered_dict[k] = recursive_dict_filter(v, min=min, max=max)
        return filtered_dict
    else:
        return d

def count_br_ids(meta_zip_path, out_file):
    """
    Creates a JSON file storing the distribution (by type of resource, type of ID, and number of ID values per ID)
    of those bibliographic resources in the OC Meta CSV dump that have more than one value for one or more of the
    supported ID types (DOI, PMID, PMCID, ISSN, ISBN and OpenAlex ID). The function also prints and logs the bare total number of
    such bibliographic resources.
    :param meta_zip_path:
    :param out_file:
    :return:
    """
    res = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    more_than_1_x_id = 0  # counts the BRs that have more than 1 value for **ANY** ID scheme
    # live_monitor = 0
    for row in read_compressed_meta_dump(meta_zip_path):
        multiple_x_ids = False
        ids = row['id'].split()
        if len(ids) > 2:  # every br has an OMID as internal ID, and optionally other external IDs
            type = row['type']
            duplicates: bool = len(ids) != len(set(ids))
            if duplicates:
                print(row)
                logging.warning(f'DUPLICATE: {row}')
                continue
            
            doi_count = len([i for i in ids if i.startswith('doi')])
            pmid_count = len([i for i in ids if i.startswith('pmid')])
            pmcid_count = len([i for i in ids if i.startswith('pmcid')])
            openalex_count = len([i for i in ids if i.startswith('openalex')])
            omid_count = len([i for i in ids if i.startswith('omid')])
            issn_count = len([i for i in ids if i.startswith('issn')])
            isbn_count = len([i for i in ids if i.startswith('isbn')])

            if doi_count > 1:
                res[type]['doi'][doi_count] += 1
                multiple_x_ids = True
            if pmid_count > 1:
                res[type]['pmid'][pmid_count] += 1
                multiple_x_ids = True
            if pmcid_count > 1:
                res[type]['pmcid'][pmcid_count] += 1
                multiple_x_ids = True
            if openalex_count > 1:
                res[type]['openalex'][openalex_count] += 1
                multiple_x_ids = True
            if omid_count > 1:
                res[type]['omid'][omid_count] += 1
                multiple_x_ids = True
            if issn_count > 1:
                res[type]['issn'][issn_count] += 1
                multiple_x_ids = True
            if isbn_count > 1:
                res[type]['isbn'][isbn_count] += 1
                multiple_x_ids = True
            if multiple_x_ids:
                more_than_1_x_id += 1

        else:
            continue

        # live_monitor += 1
        # if live_monitor % 500000 == 0:
        #     print(f'Processed {live_monitor} rows so far...')
        #     print(default_to_regular(res))

    res = sort_dict(default_to_regular(res))
    with open(out_file, 'w', encoding='utf-8') as out:
        json.dump(res, out, indent=4)

    print(f'There are {more_than_1_x_id} bibliographic resources that have at least 2 values for at least one of the supported ID schemes')
    logging.info(f'There are {more_than_1_x_id} bibliographic resources that have at least 2 values for at least one of the supported ID schemes')
    return res


def filter_distribution(data, dist=True, **kwargs):  # type, id_scheme, max=2
    """
    Filters a dictionary consisting of the JSON output by count_br_ids to retrieve specific sections of the results.
    :param data:
    :param dist: (bool) If True, return a dictionary. Else return the sum of values. See warning if set to False and no kwargs are specified.
    :param kwargs:
        - :key type (str, Optional): one of the bibliographic resource types supported by OC Meta (e.g. journal article). If None, the output refers to all the bibliographic resource types.
        - :key id_scheme (str, Optional): one of the ID types supported by OC Meta for bibliographic resources (e.g. journal article). If None, the output refers to all the ID types.
        - :key min (int, Optional): if specified, the result exclude the key-value pairs where the key is lesser than min.
        - :key max (int, Optional): if specified, the result exclude the key-value pairs where the key is greater than max.
    :return: dict|int
    """
    type = kwargs.get('type', None)
    id_scheme = kwargs.get('id_scheme', None)
    min = kwargs.get('min', None)
    max = kwargs.get('max', None)

    if type and id_scheme:
        rich_out = {k: v for k,v in data[type][id_scheme].items()}
    elif type and not id_scheme:
        rich_out = defaultdict(lambda: defaultdict(int))
        for scheme, d in data[type].items():
            for x, y in d.items():
                rich_out[scheme][x] += y
    elif id_scheme and not type:
        rich_out = defaultdict(lambda: defaultdict(int))
        for type, d1 in data.items():
            for scheme, d2 in d1.items():
                for x, y in d2.items():
                    if scheme == id_scheme:
                        rich_out[type][x] += y

    if not dist and (not type or not id_scheme):
        w = ("You have not applied filters on BR type and/or id_scheme, and have called the function with dist=False. "
             "As bibliographic resources cannot be disambiguated when working with purely quantitative data, "
             "the returned number does not represent the number of distinct bibliographic "
             "resources that have at least <min> values for the same ID scheme; instead it represents the number of times "
             "the event described by the passed parameters (i.e. filters) happens. "
             "For example, when calling filter_distribution(data, 'journal article', dist=False), if the same journal "
             "article in Meta has 3 DOIs and 4 PMIDs, the returned number would be 2 "
             "(as 2 is the frequency of the event of having multiple values for the same ID type among journal articles),"
             " even though the affected BR is only one.")
        warnings.warn(w, UserWarning)
        if not type and not id_scheme:
            rich_out = data

    res = default_to_regular(rich_out)

    if min or max:
        res = recursive_dict_filter(res, min=min, max=max)
    # elif min and not max:
    #     res = recursive_dict_filter(res, min=min)
    # elif max and not min:
    #     res = recursive_dict_filter(res, max=max)

    if dist:
        return res
    else:
        return recursive_dict_sum(res)


if __name__ == '__main__':
    meta_zip_archive = ''  # "D:/meta_csv_v8.zip"
    out_file = ''  # 'brs_multiple_ids.json'
    log_file = f'count_brs_ids_{datetime.now().strftime("%Y-%m-%d")}.log'  # 'count_brs_ids.log'
    logging.basicConfig(filename=log_file, encoding='utf-8', level=logging.DEBUG)
    # count_br_ids(meta_zip_archive, out_file)  # takes more than 30 minutes
    all_data = sort_dict(convert_keys_to_int(json.load(open(out_file, 'r', encoding='utf8'))))  # read the JSON as a dict


    # ---- USAGE EXAMPLE ----

    # 1) See how many journal articles have more than one DOI and how are they distributed based on the number of DOIs per journal article
    doi_ja_count = filter_distribution(all_data, type='journal article', id_scheme='doi', dist=False)
    doi_ja_dist = filter_distribution(all_data, type='journal article', id_scheme='doi', dist=True)
    print(doi_ja_count)
    # OUTPUT: 179519
    print(doi_ja_dist)
    # OUTPUT:
    # {2: 157916, 3: 17507, 4: 2810, 5: 654, 6: 231, 7: 91, 8: 51, 9: 18, 10: 23, 11: 20, 12: 14, 13: 17, 14: 10, 15: 10,
    #  16: 7, 17: 11, 18: 12, 19: 5, 20: 5, 21: 4, 22: 4, 23: 7, 24: 5, 25: 6, 26: 2, 27: 3, 28: 3, 30: 3, 31: 3, 32: 2,
    #  33: 3, 34: 1, 35: 1, 37: 1, 38: 2, 39: 1, 41: 2, 42: 2, 43: 5, 44: 1, 45: 1, 46: 1, 47: 1, 48: 1, 50: 3, 52: 1,
    #  53: 1, 59: 2, 60: 1, 61: 2, 64: 1, 67: 1, 70: 1, 73: 1, 75: 1, 76: 1, 77: 1, 78: 1, 80: 1, 83: 2, 85: 1, 94: 2,
    #  96: 1, 97: 1, 98: 2, 100: 1, 106: 2, 111: 2, 112: 2, 115: 1, 123: 1, 130: 1, 135: 1, 160: 1, 197: 1, 1051: 1}

    # 2) Count HOW MANY TIMES **NON-DISAMBIGUATED** journals have more than one and up to 4 values for any ID type
    print(filter_distribution(all_data, dist=False, type='journal', max=4))
    # OUTPUT: 46090



