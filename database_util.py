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

import datetime
import logging
import os
import sqlite3

from santonian import SantonianLog, sha256_string
from config import _PREFIX, SHM

logger = logging.getLogger(__name__)


class SantonianDB:
    def __init__(self, db_file):
        self.__pre = _PREFIX
        if not os.path.exists(db_file):
            self.db = sqlite3.connect(db_file)
            self.cur = self.db.cursor()
            self._create_scheme()
            self.db.close()
        try:
            self.db = sqlite3.connect(f"file:{db_file}?mode=rw", uri=True)
            self.db.row_factory = sqlite3.Row  # ! changes behaviour of all future cursors
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

    def insert_text_log(self, content: str, name: str, folder_name: str):
        temp_hash = sha256_string(content)
        query = f"""SELECT 
                        uid, name, hash, revision
                    FROM {self.__pre}log
                    WHERE name = '{name}'
                    ORDER BY revision DESC;"""
        rows = self.cur.execute(query).fetchall()
        if len(rows) > 0:
            if rows[0]['hash'] == temp_hash:
                self._touch_log(rows[0]['uid'])
            # TODO: the case where the log name already exists BUT the content is different
        else:
            if folder_id := self.get_folder_uid(folder_name) is None:
                return False
            raw_data = (
                name,
                folder_id,
                content,
                temp_hash,
                0,
                datetime.datetime.now(),
                datetime.datetime.now()
            )
            self.insert_raw_log(raw_data)

    def insert_raw_log(self, raw: tuple):
        query = f"""INSERT INTO {self.__pre}log
                    (name, folder, content, hash, revision, last_check, first_entry)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ;"""
        self.cur.execute(query, raw)
        self.db.commit()

    def _touch_log(self, uid: int):
        query = f"""UPDATE {self.__pre}log
                   SET last_check = ?
                   WHERE id = ?;"""
        self.cur.execute(query, (datetime.datetime.now(), uid))
        self.db.commit()

    def _touch_folder(self, uid: int):
        query = f"""UPDATE {self.__pre}folders
                   SET last_check = ?
                   WHERE id = ?;"""
        self.cur.execute(query, (datetime.datetime.now(), uid))
        self.db.commit()

    def import_log(self, log_object: SantonianLog):
        pass

    def insert_folder(self, folder_name: str, folder_id: int):
        # first check if an exact value already exists
        query = f"""SELECT uid, name, file_id 
                    FROM {self.__pre}folders
                    WHERE name = ? OR file_id = ?;"""
        self.cur.execute(query, (folder_name, folder_id))
        sameish = self.cur.fetchall()
        if len(sameish) == 1:
            logger.warning(f"DB>InsFolder: {folder_name}:{folder_id} already exists")
            self._touch_folder(int(sameish[0]['uid']))

            return False
        elif len(sameish) > 1:  # logically, as name/file_id are both unique there should ever be two
            logger.critical(f"DB>InsFolder: name and id in different entries: \n\t\t {sameish[0]['name']}:{sameish[0]['folder_id']} | {sameish[1]['name']}:{sameish[1]['folder_id']}")
            return False
        # insert data
        query = f"""INSERT INTO {self.__pre}folders
                    (name, file_id, temporary, last_check, first_entry)
                    VALUES (?, ?, ?, ?, ?);"""
        try:
            self.cur.execute(query, (folder_name,
                                     folder_id,
                                     0,
                                     datetime.datetime.now(),
                                     datetime.datetime.now()))
            self.db.commit()
        except sqlite3.IntegrityError:
            logger.warning(f"DB>InsFolder: unique constraints violated (despite checks?)")

    def get_folder_uid(self, input_str: str):
        # ! change this to return UID instead of id
        """
        Archive Folders are actually referenced by an ID and not by there name, therefore there needs to be some
        kind of abstraction, this will create a new folder if none by that name cannot be found, in creation
        case the id will be huge
        """
        query = f"""
                SELECT DISTINCT uid, file_id, name
                FROM {self.__pre}folders
                WHERE name = ? or file_id = ?;
                """
        self.cur.execute(query, (str(input_str).lower(), input_str))
        folder = self.cur.fetchall()
        if len(folder) <= 0:
            # ? getting the highest id
            query = f"""SELECT file_id
                        FROM {self.__pre}folders
                        ORDER file_id DESC
                        LIMIT 1;"""
            self.cur.execute(query)
            numb = self.cur.fetchone()
            if not numb:  # no entry at all
                highest_id = 100001
            else:
                try:
                    highest_id = int(numb['file_id'])
                except TypeError:
                    logger.error(
                        f"DB>getFileIdAlt - folder id: {numb.get('file_id', 'ID#ERR')}, folder name: '{input_str}'")
                    return None
                if highest_id < 100000:
                    highest_id = 100001
                else:
                    highest_id += 1

            query = f"""INSERT INTO {self.__pre}folders
                    (name, file_id, temporary, last_check, first_check)
                    VALUES(?, ?, ?, ?, ?);"""
            self.cur.execute(query, (input_str.lower(),
                                     highest_id,
                                     1,
                                     datetime.datetime.now(),
                                     datetime.datetime.now()))
            self.db.commit()
            return highest_id
        elif len(folder) == 1:
            try:
                return int(folder[0]['uid'])
            except TypeError:
                logger.error(f"DB>getFileId - id: {folder[0].get('file_id', 'ID#ERR')}| name: input: {folder[0].get('name', 'name#ERR')}'{input_str}'")
                return None
        else:
            logger.error("DB>getFileId - found unclear result with input: '{input_str}'")
            for i, line in enumerate(folder):
                if i > 3:  # halt condition if something really explodes
                    break
                logger.info(f"DB>getFileId: {line['uid']} - {line['file_id']} / {line['name']}")
            return None

    def get_log_content(self, logname: str):
        query = f"""SELECT 
                        name, folder, content, audio, last_check 
                    FROM {self.__pre}log
                    WHERE name LIKE ?
                    ORDER BY revision ?;"""
        rows = self.cur.execute(query, (f"%{logname}%", "DESC")).fetchall()
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

