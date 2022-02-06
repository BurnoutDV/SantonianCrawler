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

from datetime import datetime, date

import requests
import logging
import json
import html
import hashlib
import os
import re
from typing import Union
from pathlib import Path

logger = logging.getLogger(__name__)


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


def list_folders(config: dict):
    url = f"{config['endpoint']}/{config['hdd']}"
    status, body = _generic_get_simplifier(url)
    if status:
        if 'type' in body:
            return False, body['message']
        else:
            return True, body
    return False, body


def folder_id(config: dict, folder: str):
    """
    Translates the name of a folder into the ID, for now we can assume thats its 8 to 13 for CORRUPTED, TO ARCHIVE001
    till ARCHIVE006
    :param config:
    :param folder:
    :return:
    """
    url = f"{config['endpoint']}/{config['hdd_details']}/{folder}"
    status, body = _generic_get_simplifier(url)
    if status:
        if 'type' in body:
            if body['type'] == "OK":
                return True, body['message'][0]
            else:
                return False, body['message']
    return False, body


def read_log(config: dict, file_id: str):
    url = f"{config['endpoint']}/{config['readfile']}/{file_id}"
    status, body = _generic_get_simplifier(url)
    if status:
        if body == 'NO ITEM WITH THAT NAME':
            return False, body
    return status, html.unescape(body)


def folder_content(config: dict, folder_id: str):
    url = f"{config['endpoint']}/{config['file']}/{folder_id}"
    status, body = _generic_get_simplifier(url)
    if not status:
        return False, body
    return True, body


def split_log_name(name: str, filter=""):
    parts = name.split(".")
    if filter and parts[1] != filter:
        return None
    return parts[0]


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
               r"(0[0-9]|1[0-9]|2[0-3])(0[0-9]|[1-5][0-9])(0[0-9]|[1-5][0-9])\b"
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


def _generic_get_simplifier(url: str):
    payload = requests.get(url)
    if payload.status_code != 200:
        return False, {'code': payload.status_code, 'type': "code"}
    try:
        data = json.loads(payload.text)
    except json.JSONDecodeError:
        error = {'code': payload.status_code,
                 'type': "non-json",
                 'content': payload.text}
        return False, error
    return True, data


class SantonianLog:
    def __init__(self, name="", content="", audio_path=None):
        self._hash = ""
        self.name = name
        self.content = content
        self.audio = False
        self.audio_binary = None

        if audio_path:
            self.import_audio(audio_path)

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name: str):
        self._name = name

    @property
    def content(self):
        return self._content

    @content.setter
    def content(self, content_text: str):
        self._hash = sha256_string(content_text)
        self._content = content_text

    @property
    def audio(self):
        return self._audio

    @audio.setter
    def audio(self, audio_state: bool):
        if not isinstance(audio_state, bool):
            self._audio = False
        if audio_state:
            self.content = ""
        self._audio = audio_state

    def import_audio(self, path: Union[str, Path]):
        if size := os.path.getsize(path) > 16*1024*1024:
            logging.error(f"Log>importAudio:: file bigger than 16 Mbyte (found {size} bytes)")
        try:
            with open(path, "rb") as aud:
                self.audio_binary = aud.read()
                self.audio = True
                self._hash = sha256_file(path)
        except FileNotFoundError:
            logger.error(f"Log>importAudio:: failed to load file '{path}'")
            return None

    def getHash(self):
        return self._hash

