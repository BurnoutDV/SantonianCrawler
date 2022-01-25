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

import santonian
from util import load_config

if __name__ == "__main__":
    CONFIG = load_config()
    status, folders = santonian.list_folders(CONFIG)
    if not status:
        print(folders)
        exit(0)
    print(folders)  # ["ARCHIVE006","CORRUPTED","ARCHIVE005","ARCHIVE004","ARCHIVE003","ARCHIVE002","ARCHIVE001"]
    status, details = santonian.folder_id(CONFIG, folders[3])
    if not status:
        print("Failure")
        print(details)
    print(details)

    status, files = santonian.folder_content(CONFIG, details)
    for file in files:
        if name := santonian.split_log_name(file, "LOG"):
            status, body = santonian.read_log(CONFIG, name)
            if status:
                print(body)
            break

