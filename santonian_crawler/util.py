#!/usr/bin/env python
# coding: utf-8

# Copyright 2021 by BurnoutDV, <development@burnoutdv.com>
#
# This file is part of SantonianCrawler.
#
# SantonianCrawler is free software: you can redistribute
# it and/or modify it under the terms of the GNU General Public
# License as published by the Free Software Foundation, either
# version 3 of the License, or (at your option) any later version.
#
# SantonianCrawler is distributed in the hope that it will
# be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
# of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
# @license GPL-3.0-only <https://www.gnu.org/licenses/gpl-3.0.en.html>

import json
import os
import copy
import logging
from math import floor
from statistics import mean, median, pvariance
from collections import defaultdict
import hashlib
import re
from typing import Union
from pathlib import Path
from datetime import date, datetime

try:
    from shutil import get_terminal_size
except ImportError:
    def get_terminal_size():
        try:
            return os.get_terminal_size()
        except OSError:
            # when executing in PyCharm Debug Window there is no "real" console and you get an Errno 25
            return 80, 24

logger = logging.getLogger(__name__)
_santonian_fields = ['endpoint', 'hdd', 'hdd_details', 'file', 'readfile']
__AVG_TOLERANCE = 3  # * how much bigger as average a column is allowed to be to not get trimmed
__COL_SPACING = 1  # * empty space between columns

def load_config(file="./config.json"):
    global _santonian_fields
    try:
        with open(file, "r") as config_file:
            data = json.load(config_file)
            if all(key in data for key in _santonian_fields):
                return data
    except FileNotFoundError:
        logger.warning("cannot find 'config.json'")
    
    except json.JSONDecodeError:
        logger.critical("Config file is malformed")
    return False


def sha256_string(text: str):
    return hashlib.sha256(text.encode('utf-8')).hexdigest()


def sha256_file(filename: Union[str, Path]):
    h = hashlib.sha256()
    b = bytearray(128 * 1024)
    mv = memoryview(b)
    with open(filename, 'rb', buffering=0) as f:
        for n in iter(lambda: f.readinto(mv), 0):
            h.update(mv[:n])
    return h.hexdigest()


def str_refinement(collection: list or dict, catalyst: dict):
    """
    'Refines' the given set of strings with the application of some rules, super specific solution for a specific
    problem.

    :param collection: the given collection, either a direct dictionary or a list of dictionary
    :param catalyst: the key:value dictionary describing what to do with the content
    :return: the same type of collection that was given
    """
    if not isinstance(collection, (list, dict)):
        return None
    if isinstance(collection, list):
        returnal = []
        for keyvalue in collection:
            returna2 = {}
            for key, val in keyvalue.items():
                if key in catalyst:
                    if value := _single_str_refinement(val, catalyst[key]):
                        returna2[key] = value
            if returna2:
                returnal.append(returna2)
        return returnal
    elif isinstance(collection, dict):
        returnal = {}
        for key, val in collection.items():
            if key in catalyst:
                if value := _single_str_refinement(val, catalyst[key]):
                    returnal[key] = value
        return returnal
    else:
        return None  # i cannot imagine how we would ever get her


def _single_str_refinement(in_str, rule: str):
    if rule == 'str':
        return str(in_str)
    elif rule == 'date_short' or rule == 'date_long':
        try:
            this_date = datetime.strptime(in_str, "%Y-%m-%d %H:%M:%S.%f")
        except ValueError:
            return None
        if rule == 'date_short':
            return this_date.date().strftime("%Y-%m-%d")
        if rule == 'date_long':
            return this_date.strftime("%Y-%m-%d %H:%M:%S")
    elif groups := re.search(r"^(trim:)([0-9]+)$", rule):
        in_str = in_str.replace("\n", " ")
        in_str = in_str.replace("\t", "   ")  # not all contain tabs, this is most interesting
        return in_str[:int(groups[2])]
    return in_str


def find_date(text_block: str) -> date or None:
    """
    Finds mentions of dates in given text block, will only look for certain patterns

    Known pattern:

    * 9/25/43 - 09-25-2043
    * May 2049 - 01-05-2049
    * January 1st, 2053 - 01-01-2053
    * July 3rd, 2047 - 03-07-2047
    * March 18th, 2053 - 18-03-2053
    * 531008 092419 - 08-10-2053 09-24-19
    * January 25 2028 - 25-01-2028

    We probably could determine the timezone by context but this is like *xkcd 1425*

    :param text_block: text of arbitrary length
    :return: a datetime of the extracted date or None
    :rtype: date or None
    """
    date_matches = {
        # * 9/25/43
        'short': r"\b([1-9]|1[0-2])\/([1-9]|[12][0-9]|3[01])\/(\d{2})\b",
        # * 04/05/2054  - its rather unclear if this DAY MONTH YEAR as it only exists in FTR-044-V.LOG
        'short2': r"\b([012][0-9]|3[01])\/(0[1-9]|1[0-2])\/(\d{4})\b",
        # * May 2049
        'approx': r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s(20\d{2})\b",
        # * January 25 2028
        'long1': r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s"
                 r"(\d{1,2})\s"
                 r"(20\d{2})\b",
        # * July 3rd, 2043
        # * June 18th 2049
        'long2': r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s"
                 r"(\d{1,2})"  # 1-99
                 r"(st|nd|rd|th|st,|nd,|rd,|th,)?\s"  # 1st, 2nd, 3rd, 4th
                 r"(20\d{2})\b",  # 20xx
        # * Mar 18 th 2053 (either to differenciate between capture groups
        'longshort': r"\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s"
                     r"(\d{1,2})"  # 1-99
                     r"(st|nd|rd|th|st,|nd,|rd,|th,)?\s"  # 1st, 2nd, 3rd, 4th
                     r"(20\d{2})\b",  # 20xx
        # * Biocom - 531008 092419 (only the date part, time will be discarded for now)
        'bio': r"\b([0-9]{2})(1[0-2]|0[1-9])(3[01]|[12][0-9]|0[1-9])"
               r"(\s|.)"
               r"(0[0-9]|1[0-9]|2[0-3])(0[0-9]|[1-5][0-9])(0[0-9]|[1-5][0-9])\b",
        # * Biocom K-UX-DeepScan-4-7-52 - Only exists in WKRP-817-CIN.LOG, unclear if DAY-MONTH-YEAR
        'bio_short': r"\b([1-9]|[12][0-9]|3[01])-([1-9]|1[0-2])-(\d{2})\b"
    }
    # ! complex pattern for extended dates that are short Hand eg. 'Jan 2nd (19)45' or '12 Mar 1978'
    r"""(\b\d{1,2}\D{0,3})?
        \b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?
        |Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|(Nov|Dec)(?:ember)?)\D?
        (\d{1,2}\D?
        (st|nd|rd|th|st,|nd,|rd,|th,)?\s
        \D?((19[7-9]\d|20\d{2})|\d{2})
    """
    template = {  # describes where which part of a month is stored
        'short': {
            'year': 3,
            'month': 1,
            'day': 2
        },
        'short2': {
            'year': 3,
            'month': 2,
            'day': 1
        },
        'approx': {
            'year': 2,
            'month': 1,
            'map_month': True,
            'day': -1
        },
        'long1': {
            'year': 3,
            'month': 1,
            'map_month': True,
            'day': 2
        },
        'long2': {
            'year': 4,
            'month': 1,
            'map_month': True,
            'day': 2
        },
        'longshort': {
            'year': 4,
            'month': 1,
            'map_month': True,
            'day': 2
        },
        'bio': {
            'year': 1,
            'month': 2,
            'day': 3
        },
        'bio_short': {
            'year': 3,
            'month': 2,
            'day': 1
        }
    }
    month_map = {'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
                 'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12}
    for proc, pattern in date_matches.items():
        if (reg := re.search(pattern, text_block)) is not None:
            try:
                start, end = reg.regs[template[proc]['year']]
                year = int(text_block[start:end])
                start, end = reg.regs[template[proc]['month']]
                month = text_block[start:end]
                start, end = reg.regs[template[proc]['day']]
                if template[proc]['day'] == -1:
                    day = 1
                else:
                    day = int(text_block[start:end])
                if year < 100:  # also known as 0-99 assuming positive numbers and hoping for not events pre-2000
                    year += 2000
                if 'map_month' in template[proc]:
                    month = month_map[month[:3]]
                else:
                    month = int(month)
                return date(year, month, day)
            except TypeError:  # if for some reasons this thing goes south we try the next pattern
                continue
            except ValueError:  # should only be thrown by date()
                continue
    return None


# copied from audio metadata shuttle project
def calc_distribution(val_list: dict, method="median"):
    if method == "average" or method == "mean":
        lens = {col: mean(val_list[col]) for col in val_list}
    else:  # method == "median"
        lens = {col: median(val_list[col]) for col in val_list}
    total_len = sum([x for x in lens.values()])
    return {col: x/total_len for col, x in lens.items()}


def _calc_console_widths_absolute_method(headers: list, data: list, max_width=0, columns_width=None):
    global __AVG_TOLERANCE, __COL_SPACING
    col, row = get_terminal_size()
    if max_width == 0 or max_width > col:
        max_width = col
    max_width = 150
    # default things we use later
    max1_len = defaultdict(int)
    val_list = defaultdict(list)
    stat = defaultdict(int)
    # maximum needed space
    for line in data:
        for col in headers:
            if col in line and len(line[col]) > 0:
                stat[col] += 1
                tmp = len(line[col])
                max1_len[col] = tmp if tmp > max1_len[col] else max1_len[col]
                val_list[col].append(tmp)
    # in case that the headers are bigger than the content
    # * if column width set, divide avail_space by amount of set columns and make set_len static to those length for
    # * the first get around
    max2_len = copy.copy(max1_len)
    for val in headers:
        max2_len[val] = len(val) if len(val) > max2_len[val] else max2_len[val]
    # various numbers
    disp_cols = [x for x in headers if stat[x] > 0]  # columns that get displayed
    num_col = len(disp_cols)
    space_len = (num_col-1) * __COL_SPACING
    avail_space = max_width - space_len

    # * edge case 1 - available space is not enough to even display all columns
    # * edge case 2 - there is enough space to show all content-lines
    # * edge case 3 - there is enough space to show all header & content-lines
    # * 'edge' case 4 - there is not enough space for all columns (this is what i actually expected to happen)
    # median of length, longest get available free space
    per_len = calc_distribution(val_list)
    set_len = {col: floor(val*avail_space) for col, val in per_len.items()}
    big_cols = []
    for col in set_len:
        if set_len[col] >= max1_len[col]:
            set_len[col] = max1_len[col]
        elif set_len[col] <= 0:  # a very tiny column will be at least partially displayed
            set_len[col] = 1
        else:
            big_cols.append(col)
    rest_space = avail_space - sum([x for x in set_len.values()])
    num_big_cols = len(big_cols)
    if num_big_cols <= 0:  # aka. there is enough space for ALL columns, we are not prioritizing headers because i
                           # assume stuff like columns that only contain small numbers but big titles, in case we have
                           # an abudance of space we now priotize padding rows with cut headers first before padding oth
        for col in set_len:
            mss_head_space = max2_len[col]-set_len[col]
            if mss_head_space > 0:
                if mss_head_space <= rest_space:
                    set_len[col] += mss_head_space
                    rest_space -= mss_head_space
                else:
                    set_len[col] += rest_space
                    rest_space = 0
            if rest_space == 0:
                break
        if rest_space > 0:  # there is still space to distribute
            big_cols = [x for x in set_len]  # everyone is a big column now that is allowed to grow
            num_big_cols = len(big_cols)
    if num_big_cols > 0:  # if there is space left or big_cols
        rst_per_col = floor(rest_space/num_big_cols)
        for col in big_cols:
            set_len[col] += rst_per_col
        # ? all this does is giving the column with the highest variance the remaining space if there is any
        var_len = {col: pvariance(val_list[col]) for col in disp_cols if col in val_list}
        var_len = {k: v for k, v in sorted(var_len.items(), key=lambda item: item[1], reverse=True)}
        bigg = next(iter(var_len.keys()))
        set_len[bigg] = set_len[bigg] + (avail_space - sum([x for x in set_len.values()]))
    return set_len, stat


def simple_console_view(keys: list, data: list, max_width=0, columns_width=None):
    r"""
    Displays a simple console view of any given data, will horribly break if too much data is supplied

    :param list keys: list of relevant dictionary keys, used as heading if occuring
    :param list data: list of dict
    :param int max_width: optional maximum width of the whole table
    :param int column_widths: specifies the maximum widht of any one giving heading, 0 is dynamic
    :return: nothing, writes directly to console
    """
    global __AVG_TOLERANCE, __COL_SPACING
    set_len, stat = _calc_console_widths_absolute_method(keys, data, max_width)
    header = ""
    for prop in set_len:
        header += f"{prop} [{stat[prop]}]{' '*set_len[prop]}"[:set_len[prop]] + " "*__COL_SPACING
    print(header[:-__COL_SPACING])

    for line in data:
        body = ""
        for i, col in enumerate(set_len):
            if col in line:
                body += f"{line[col]}{' '*set_len[col]}"[:set_len[col]] + " "*__COL_SPACING
            else:
                body += f"{' '*set_len[col]}"[:set_len[col]] + " "*__COL_SPACING
        print(body[:-__COL_SPACING])

