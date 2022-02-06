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
from time import sleep

# ? own stuff / local points
import santonian
from database_util import SantonianDB
from config import api_calls as CONFIG, req_retries, req_wait

# ! base logger configuration
logging.basicConfig(filename='dreyfus.log', format='[%(asctime)s] %(levelname)s:%(message)s', level=logging.INFO)


def fetch_full_santonian(database: str):
    db = SantonianDB(database)
    # ! fetching folder list
    print("fetching folder list")
    status, folders = santonian.list_folders(CONFIG)
    # ["ARCHIVE006","CORRUPTED","ARCHIVE005","ARCHIVE004","ARCHIVE003","ARCHIVE002","ARCHIVE001"]
    if not status:
        logging.critical(f"FFS>Cannot retrieve folder list from '{CONFIG['endpoint']}/{CONFIG['hdd']}'")
        return False
    # ! fetching the id of each folder
    files = []
    for i, file_name in enumerate(folders):
        print(f"[{i}] {file_name}", end="")
        while True:
            repeats = req_retries
            status, details = santonian.folder_id(CONFIG, file_name)
            if not status:
                logging.warning(f"FFS>fetch files failed, waiting {req_wait}, {req_retries} more tries")
                sleep(req_wait)
                repeats -= 1
            else:
                break
            if repeats <= 0:
                break
        if not status:
            print(" ##FAIL")
            continue
        files.append(details)
        db.insert_folder(file_name, details)
        print(f" - {details}")
    if len(files) < 0:
        logging.warning("FFS>no files in list")
        return False
    # DIR for every file
    for _, file_id in enumerate(files):
        print(f"[{_}] Fetching ID {file_id}:", end="")
        repeats = req_retries
        while True:
            status, logs = santonian.folder_content(CONFIG, file_id)
            if not status:
                logging.warning(f"FFS>fetch files failed, waiting {req_wait}s, {repeats} more tries")
                logging.debug(f"FFS>DEBUG>REQ_BODY>'{logs}'")
                sleep(req_wait)
                repeats -= 1
            else:
                break
            if repeats <= 0:
                break
        if not status:
            print(" ##FAIL")
            logging.warning(f"FFS>fetching file list id='{file_id}' failed ultimately")
            continue
        # * nesting, second round for each file in files
        if not logs:
            print(" Empty folder, commencing...")
            continue
        print(f" {{{len(logs)}}} log files found")
        for _i, log_name in enumerate(logs):
            print(f"  [{_i}] Fetching log name {log_name}", end="")
            if name := santonian.split_log_name(log_name, "LOG"):
                repeats = req_retries
                while True:
                    status, body = santonian.read_log(CONFIG, name)
                    if not status:
                        logging.warning(f"FFS>fetching log failed, waiting {req_wait}s, {repeats} more tries")
                        sleep(req_wait)
                        repeats -= 1
                    else:
                        break
                    if repeats <= 0:
                        break
                if not status:
                    print(" ##FAIL")
                    continue
                db.insert_text_log(body, log_name, file_id)
                print(f" - {len(body)}")
            else:
                print("##AUD//NoSUPPORT")
    print("...Process finished")
    return True


if __name__ == "__main__":
    print(fetch_full_santonian("santonian.db"))

