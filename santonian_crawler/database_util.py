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

from datetime import datetime
import logging
import os
import sqlite3

from util import sha256_string, find_date
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
            if (folder_id := self.get_folder_uid(folder_name)) is None:
                return False
            data = (name,
                    folder_id,
                    content,
                    temp_hash,
                    0,
                    datetime.now(),
                    datetime.now())
            query = f"""INSERT INTO {self.__pre}log
                        (name, folder, content, hash, revision, last_check, first_entry)
                        VALUES (?, ?, ?, ?, ?, ?, ?);"""
            self.cur.execute(query, data)
            self.db.commit()

    def _touch_log(self, uid: int):
        query = f"""UPDATE {self.__pre}log
                   SET last_check = ?
                   WHERE id = ?;"""
        self.cur.execute(query, (datetime.now(), uid))
        self.db.commit()

    def _touch_folder(self, uid: int):
        query = f"""UPDATE {self.__pre}folders
                   SET last_check = ?
                   WHERE uid = ?;"""
        self.cur.execute(query, (datetime.now(), uid))
        self.db.commit()

    #def import_log(self, log_object: SantonianLog):
    #    pass

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
                                     datetime.now(),
                                     datetime.now()))
            self.db.commit()
        except sqlite3.IntegrityError:
            logger.warning(f"DB>InsFolder: unique constraints violated (despite checks?)")

    def get_folder_uid(self, input_str: str or int):
        # ! change this to return UID instead of id
        """
        Archive Folders are actually referenced by an ID and not by there name, therefore there needs to be some
        kind of abstraction, this will create a new folder if none by that name cannot be found, in creation
        case the id will be huge
        """
        if isinstance(input_str, str):
            condition = "name = ?"
        elif isinstance(input_str, int):
            condition = "file_id = ?"
        else:
            logger.warning(f"DB>getFolderId: wrong input format: {type(input_str)}")
            return None
        query = f"""
                SELECT DISTINCT uid, file_id, name
                FROM {self.__pre}folders
                WHERE {condition};
                """
        self.cur.execute(query, [input_str])
        folder = self.cur.fetchall()
        if len(folder) <= 0:
            # ? getting the highest id
            query = f"""SELECT file_id
                        FROM {self.__pre}folders
                        ORDER BY file_id DESC
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
                    (name, file_id, temporary, last_check, first_entry)
                    VALUES(?, ?, ?, ?, ?);"""
            self.cur.execute(query, (input_str.lower(),
                                     highest_id,
                                     1,
                                     datetime.now(),
                                     datetime.now()))
            self.db.commit()
            query = f"SELECT uid FROM {self.__pre}folders WHERE file_id = ?;"
            data = self.cur.execute(query, [highest_id]).fetchone()
            return data['uid']
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
        query = f"""SELECT name, folder, content, audio, last_check, revision 
                    FROM {self.__pre}log
                    WHERE name LIKE "{logname}"
                    ORDER BY revision DESC;"""
        rows = self.cur.execute(query).fetchall()
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

    def update_stat(self, key: str, value: str) -> False:
        """
        Updates a singular named stat in the database (or creates it if it does not exist)

        I suspect that there is some fancy sql(ite?) statement that does this easier

        :param key: unique key
        :param value: arbitrary value
        :return: True if a value was replace, False if a new one was created
        """
        query = f"""SELECT uid FROM {self.__pre}stats WHERE property = ?;"""
        check = self.cur.execute(query, [key]).fetchone()
        if check:
            query = f"""UPDATE {self.__pre}stats
                        SET value = ?
                        WHERE uid = ?;"""
            self.cur.execute(query, (str(value), check['uid']))
        else:
            query = f""""INSERT INTO {self.__pre}stats
                         (property, value)
                         VALUES (?, ?);"""
            self.cur.execute(query, (key, str(value)))
        self.db.commit()
        return check is not None  # * general sanity callback without any real value

    def create_modify_tag(self, tag_name: str, tag_type: str) -> tuple:
        """
        Creates a tag with the given name, if that tag already exists it will overwrite the type

        :param tag_name: unique name of that tag
        :param tag_type: one of the three types: names, dates, entities (will default to names)
        :return: a tuple of the written name and type, None if the operation could not take place
        :rtype: tuple or None
        """
        allowed_types = ["name", "date", "entity"]  # * i pondered implementing this directly in the database
        # check if the field exists
        query = f"""SELECT name, type 
                    FROM {self.__pre}tag 
                    WHERE name = ?;"""
        res = self.cur.execute(query, [tag_name]).fetchone()
        # operations
        if tag_type not in allowed_types:
            tag_type = allowed_types[0]
        if res:  # tag exists
            if res['type'] != tag_type:
                query = f"""UPDATE {self.__pre}tag
                            SET type = ?,
                            WHERE name = ?;"""
                self.cur.execute(query, (tag_type, tag_name))
                self.db.commit()
                return tag_name, tag_type
        else:
            query = f"""INSERT INTO {self.__pre}tag
                        (name, type)
                        VALUES (?, ?);"""
            self.cur.execute(query, (tag_name, tag_type))
            self.db.commit()
            return tag_name, tag_type
        logger.warning(f"DB>c&m_tag: failed to actually create or modify tag '{tag_name}' with type '{tag_type}'")
        return None

    def tag_file(self, file_name: str, tag_name: str):
        """
        adds a specific tag to that specific file

        :param file_name: name of a file, unique
        :param tag_name: name of the tag, unique
        :return: False if the tag does not exist
                 None if the file does not exist / cannot be found
                 True if the operation was successful (or unnecessary)
        """
        # * check for existence
        query = f"""SELECT uid 
                    FROM {self.__pre}tag 
                    WHERE name = ?;"""
        tag = self.cur.execute(query, [tag_name]).fetchone()
        if not tag:
            logger.warning(f"DB>tag_file: could not locate tag with name '{tag_name}'")
            return False
        query = f"""SELECT uid 
                    FROM {self.__pre}log
                    WHERE name LIKE ?;"""  # like to ignore casesensivity
        log = self.cur.execute(query, [file_name]).fetchone()
        if not log:
            logger.warning(f"DB>tag_file: could not locate file with name '{file_name}'")
            return None
        # * creating of link
        # ! TODO: check for duplicate
        query = f"""INSERT INTO {self.__pre}tag_link
                    (log, tag, changed)
                    VALUES (?, ?, ?);"""
        self.cur.execute(query, (log['uid'], tag['uid'], datetime.now()))
        self.db.commit()
        return True

    # ? complex procedures that do things
    def procedure_tag_date(self) -> dict:
        """
        Goes over all logs that do not have a date tag already assigned to them and tags them with a date
        extracted from the text, uses first occurence of a match, only is date precise, not by the hour

        :return: a dictionary of newly tagged entries with dates, format {log_name: date_tag}
        :rtype: dict
        """
        # * retrieving all logs that do not posses a tag of type date, i would appreciate help here, as i lag the
        # * sql skill to actually extract all entries i want, which would be "do not have date tag or no tag at all"
        filter_tag_type = "date"
        _ = self.__pre
        # select tags that DO have a date tag
        query = f"""SELECT DISTINCT {_}log.name as name FROM {_}log
                    INNER JOIN {_}tag_link ON log.uid = {_}tag_link.log
                    INNER JOIN {_}tag ON {_}tag_link.tag = {_}tag.uid
                    WHERE {_}tag.type == ?;
                """
        res = self.cur.execute(query, [filter_tag_type]).fetchall()
        ignore_list = set([x['name'] for x in res])  # sets are faster upon lookup (should not matter but good practice)
        # select all logs
        query = "SELECT name, content FROM log"
        res = self.cur.execute(query).fetchall()  # just querying everything is usually faster than asking every single
                                                  # thing, this might not scale to all eternity
        contents = {x['name']: x['content'] for x in res}
        changes = {}
        for name in contents:
            if name in ignore_list:
                continue
            tag = find_date(contents[name])
            if tag:
                self.create_modify_tag(str(tag), "date")
                self.tag_file(name, str(tag))
                changes[name] = str(tag)
        if len(changes) > 0:
            logger.info(f"Created {len(changes)} tag_links, rough date: {datetime.now().isoformat()}")
        return changes

    # ? "simple" procedures that just replace a simple select

    def list_logs_of_folder(self, folder: str, mode="simple", page=0, per_page=20) -> list:
        """
        Lists all logs that belong to the specified folder, if folder is unknown, an empty list is returned

        :param str folder:
        :param str mode: "simple" for just str, "complex for full logs of every entry
        :param int page: if mode is complex, pagination
        :return: list
        """
        # ? second get actual data
        _ = self.__pre
        if mode != "complex":
            query = f"""SELECT {_}log.name 
                        FROM {_}log
                        INNER JOIN {_}folders ON {_}folders.uid = {_}log.folder
                        WHERE {_}folders.name LIKE '{folder}'
                        ORDER BY {_}log.uid ASC
                        LIMIT {per_page} OFFSET ?;"""
            raws = self._general_fetch_query(query, page*per_page)
            return [x['name'] for x in raws]
        else:
            query = f"""SELECT {_}log.name as name, content, {_}folders.name as folder, audio, hash, 
                                revision, {_}log.last_check, {_}log.first_entry
                        FROM {_}log
                        INNER JOIN {_}folders ON {_}folders.uid = {_}log.folder
                        WHERE {_}folders.name LIKE '{folder}'
                        ORDER BY {_}log.uid ASC
                        LIMIT {per_page} OFFSET ?;"""
            raws = self._general_fetch_query(query, page * per_page)
            return raws
        return []

    def get_all_folders(self, start=0, limit=25, order="ASC", order_field="uid"):
        """
        Simple procedure that queries simply all entries and returns their content, in this case for folders
        Returns 25 entries each call

        :param int start: Offset Parameter, number of entries to hop over
        :param int limit: maximum of rows that are retrieved
        :param str order: either ASC or DESC, will default to ASC if anything else is choosen
        :param str order_field: field that is used to order, can only be 'uid', 'file_id', 'name',
                                'last_check' or 'first_entry'
        :return: a list of arrays, eg: [{'uid': 2, 'name': 'Archive002', 'file_id': 8}]
        """
        if order.upper() != "ASC" and order.upper() != "DESC":
            order = "ASC"
        possible_orders = ['uid', 'file_id', 'name', 'last_check', 'first_entry', 'revision']
        if order_field not in possible_orders:
            order_field = "uid"
        query = f"""SELECT uid, file_id, name, temporary, last_check, first_entry
                    FROM {self.__pre}folders
                    ORDER BY {order_field} {order}
                    LIMIT {limit} OFFSET ?;"""
        return self._general_fetch_query(query, start)

    def get_all_logs(self, start=0, limit=25, order="ASC", order_field="uid", tags=False):
        if order.upper() != "ASC" and order.upper() != "DESC":
            order = "ASC"
        _ = self.__pre  # for readability
        allowed_order = {"uid": f"{_}log.uid", "name": f"{_}log.name", 'content': f"{_}log.content",
                         'folder': f"{_}folders.name", 'revision': "revision", 'last_check': f"{_}log.last_check",
                         'first_entry': f"{_}log.first_entry", 'tag_date': f"{_}tag.name"}
        if order_field in allowed_order:
            order_field = allowed_order[order_field]
        else:
            order_field = allowed_order['uid']
        # * switch for tags
        if not tags:
            query = f"""SELECT {_}log.uid, {_}log.name as name, content, {_}folders.name as folder, audio, hash, 
                                revision, {_}log.last_check, {_}log.first_entry
                        FROM {_}log
                        INNER JOIN {_}folders ON {_}log.folder = {_}folders.uid
                        ORDER BY {order_field} {order}
                        LIMIT {limit} OFFSET ?;"""
        else:
            query = f"""SELECT {_}log.uid, {_}log.name as name, content, {_}folders.name as folder, audio, hash, 
                                            revision, {_}log.last_check, {_}log.first_entry,
                                            COALESCE(group_concat(tag.name, ', '), '') as tags
                                    FROM {_}log
                                    INNER JOIN {_}folders ON {_}log.folder = {_}folders.uid
                                    LEFT JOIN {_}tag_link on {_}log.uid = {_}tag_link.log
                                    LEFT JOIN {_}tag on {_}tag_link.tag = {_}tag.uid
                                    GROUP BY {_}log.name
                                    ORDER BY {order_field} {order}
                                    LIMIT {limit} OFFSET ?;"""
        if order_field == f"{_}tag.name":  # this more or less ignores the 'tags' paremeter nad just says, we do tags
            query = f"""SELECT {_}log.name as name, 
                               {_}folders.name as folder, 
                               {_}log.last_check, 
                               {_}log.first_entry, 
                               content,
                               audio,
                               aud_fl,
                               hash,
                               COALESCE(group_concat({_}tag.name, ', '), '') as tags
                        FROM {_}log
                        INNER JOIN {_}folders on {_}log.folder = {_}folders.uid
                        LEFT JOIN {_}tag_link on {_}log.uid = {_}tag_link.log
                        LEFT JOIN {_}tag on {_}tag_link.tag = {_}tag.uid
                        WHERE {_}tag.type = 'date' OR {_}tag.type is Null
                        GROUP BY {_}log.name
                        ORDER BY {order_field} {order}
                        LIMIT {limit} OFFSET ?;"""
        return self._general_fetch_query(query, start)

    def count_logs(self):
        query = f"SELECT COUNT(DISTINCT name) as num FROM {self.__pre}log"
        return self.cur.execute(query).fetchone()['num']

    def count_files(self):
        query = f"SELECT COUNT(DISTINCT name) as num FROM {self.__pre}folders"
        return self.cur.execute(query).fetchone()['num']

    def _general_fetch_query(self, query, start):
        """
        A bit of boilerplate so i don't have to write it again, this feels like its almost at the fragmentation
        threshold, maybe a line less and this function would feel entirely useless

        :param query: valid sqlite query, wont be validated, will just fail through
        :param int start: Offset Parameter, number of entries to hop over
        :return:
        """
        raw_data = self.cur.execute(query, [start]).fetchall()
        refined_data = []
        for each in raw_data:
            refined_data.append({x: each[x] for x in each.keys()})  # only possible because of row factory
        return refined_data
