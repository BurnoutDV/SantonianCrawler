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

import requests
import logging
import json
import html
import hashlib
import os
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

