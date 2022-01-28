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

import logging
import sqlite3
import datetime

from santonian import SantonianLog, sha256_file, sha256_string

logger = logging.getLogger(__name__)

__PREFIX = ""
SHM = {}
SHM['folders'] = f"""
                    CREATE TABLE IF NOT EXISTS {__PREFIX}folders (
                        uid INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT UNIQUE,
                        file_id TEXT UNIQUE,
                        temporary INT NOT NULL CHECK (temporary in (0, 1)) DEFAULT 0,
                        last_check TIMESTAMP,
                        first_entry TIMESTAMP
                    );"""
SHM['log'] = f"""
                CREATE TABLE IF NOT EXISTS {__PREFIX}log (
                    uid INTEGER PRIMARY KEY AUTOINCREMENT,
                    folder INT REFERENCES {__PREFIX}folders(uid)
                    content TEXT,
                    audio INT NOT NULL CHECK (audio in (0, 1)) DEFAULT 0,
                    aud_fl BLOB,
                    hash TEXT NOT NULL,
                    revision INT NOT NULL,
                    last_check TIMESTAMP NOT NULL,
                    first_entry TIMESTAMP NOT NULL,
                    sha256 TEXT
                );"""
SHM['stats'] = f"""
                CREATE TABLE IF NOT EXISTS {__PREFIX}stats (
                    uid INTEGER PRIMARY KEY AUTOINCREMENT,
                    property TEXT UNIQUE,
                    value TEXT
                );"""
SHM['tags'] = f"""
                CREATE TABLE IF NOT EXITS {__PREFIX}tag (
                    uid INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT UNIQUE
                    );"""
SHM['tag_link'] = f"""
                CREATE TABLE IF NOT EXISTS {__PREFIX}tag_link (
                    uid INTEGER PRIMARY KEY AUTOINCREMENT,
                    log INTEGER REFERENCES {__PREFIX}log(uid),
                    tag INTEGER REFERENCES {__PREFIX}tag(uid)
                );"""


class SantonianDB:
    def __init__(self, db_file):
        global __PREFIX
        self.__pre = __PREFIX
        try:
            self.db = sqlite3.connect(f"file:{db_file}?mode=rw", uri=True)
            self.db.row_factory = sqlite3.Row
            self.cur = self.db.cursor()
        except sqlite3.OperationalError as err:
            logger.error(f"Error while opening database file: {err}")

    def close(self):
        """
        Closes database
        :return:
        """
        self.db.close()

    def _create_scheme(self):
        for value in SHM.values():
            self.cur.execute(value)
        self.db.commit()

    def insert_text_log(self, content: str, name: str, folder: str):
        temp_hash = sha256_string(content)
        query = f"""SELECT 
                        uid, name, hash, revision
                    FROM {self.__pre}log
                    WHERE name = '{name}'
                    ORDER BY revision DEC;"""
        rows = self.cur.execute(query).fetchall()
        if len(rows) > 0:
            if rows[0]['hash'] == temp_hash:
                self._touch_log(rows[0]['uid'])
        else:
            folder_id = 0
            raw_data = (
                folder_id,
                content,
                temp_hash,
                1,
                datetime.datetime.now(),
                datetime.datetime.now()
            )
            self.insert_raw_log(raw_data)

    def insert_raw_log(self, raw: tuple):
        query = f"""INSERT INTO {self.__pre}log
                    (folder, content, hash, revision, last_check, first_entry)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ;"""
        self.cur.execute(query, raw)
        self.db.commit()

    def _touch_log(self, uid: int):
        query = f"""UPDATE {self.__pre}log
                   SET last_check = ?
                   WHERE id = ?"""
        self.cur.execute(query, (datetime.datetime.now(), uid))

    def import_log(self, log_object: SantonianLog):
        pass

    def get_log_content(self, logname: str):
        query = f"""SELECT 
                        name, folder, content, audio, last_check 
                    FROM {self.__pre}log
                    WHERE name LIKE ?
                    ORDER BY revision DEC
                ;"""
        rows = self.cur.execute(query, (logname)).fetchall()
        len_rows = len(rows)
        if len_rows <= 0:
            return None
        elif len_rows > 1:
            r = []
            for entry in rows:
                r.append({key: entry[key] for key in entry.keys()})
            return r
        else:
            return {key: rows[0][key] for key in rows[0].keys()}

