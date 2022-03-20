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

from flask import Flask, jsonify
from database_util import SantonianDB
from pathlib import PurePath

db_name = "santonian.db"
#backend = SantonianDB('santonian.db', check_same_thread=False)
app = Flask(__name__)


# ? the double routes are only there because the is the exact behaviour the real website gives us

@app.route("/backend/hdd/", methods=['GET', 'POST'])
@app.route("/backend/hdd", methods=['GET', 'POST'])
def all_folders():
    backend = SantonianDB(db_name)
    raw = backend.get_all_folders()
    backend.close()
    return jsonify([x['name'] for x in raw])


@app.route("/backend/hdd_details/<disk>/", methods=['GET', 'POST'])
@app.route("/backend/hdd_details/<disk>", methods=['GET', 'POST'])
def id_folder(disk: str):
    backend = SantonianDB(db_name)
    raw = backend.get_folder_santa_id(disk)
    backend.close()
    if raw:
        return jsonify({'type': "OK", 'message': [raw]})
    else:
        return jsonify({'type': "ERR", 'message': "[[b;#b0e6fd;]{disk}] is not a valid folder.".format(disk=disk)})


@app.route("/backend/file/<int:disk_id>/", methods=['GET', 'POST'])
@app.route("/backend/file/<int:disk_id>", methods=['GET', 'POST'])
def dir_folder(disk_id: int):
    """
    this function is the most nonsensical of them all, internally the real santonian website only accepts the folder
    request by internal id which the crawler id mimics but we could easily just accept the names as they are defined as
    unique in the sqlite3. To mimic properly we unfortunately need to go a roundabout way i really don't like
    :param disk:
    :return:
    """
    backend = SantonianDB(db_name)
    folder_name = backend.get_folder_by_santa_id(disk_id)
    if not folder_name:
        backend.close()
        return jsonify("")  # emptiest of all jsons
    files = backend.list_logs_of_folder(folder_name, per_page=200)  # magic nummer that makes assumptions
    backend.close()
    return jsonify(files)


@app.route("/backend/readFile/<file_name>/", methods=['GET', 'POST'])
@app.route("/backend/readFile/<file_name>", methods=['GET', 'POST'])
def read_file(file_name: str):
    """
    This function on the original santonian website is a bit puzzling as it only wants the log name without the
    extension, therefore some additional magic is needed to make it work

    :param file_name: name of the log file without extension
    :return:
    """
    backend = SantonianDB(db_name)
    real_name = backend.get_log_name_extension_blind(file_name)
    full_file = backend.get_log_content(real_name)
    backend.close()
    if full_file:
        # crawler has revisions, give only newest one if some exist:
        if isinstance(full_file, list):
            full_file = full_file[0]
        return jsonify(full_file['content'])
    else:
        return jsonify("NO ITEM WITH THAT NAME")

# raw = backend.list_logs_of_folder(disk)
