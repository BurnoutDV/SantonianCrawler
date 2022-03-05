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

import cmd
import database_util
import json
__ver__ = 0.19


class SantonianShell(cmd.Cmd):
    intro = f"Welcome to Santonian Shell {__ver__}"
    prompt = "> "

    def __init__(self, db_path="santonian.db"):
        super().__init__()
        self.backend = database_util.SantonianDB(db_path)

    def do_list(self, args):
        """usage: list <folder/*>
            [filter: 'text']
            [order: 'ASC/DESC' <property>]
            [limit: <int>]
            [page: <int>]
            [view: <'column'/'detail'>]

        lists all logs within parameters, default limit is 20 per page
        """
        arguments = args.split(" ")
        if len(arguments) == 0:
            raw_data = self.backend.get_all_logs()
            print(json.dumps(raw_data, indent=3))
            return False
        if arguments[0] == "*":
            pass
        else:
            files = self.backend.list_logs_of_folder(arguments[0])
            for line in files:
                print(line)

    def do_folders(self, args):
        folders = self.backend.get_all_folders()
        print(f"DB knows about {len(folders)} folders")
        for line in folders:
            print(line['name'], end="\t")
        else:
            print("")

    def do_read(self, args):
        """usage: read <log_name> [revision: <int>]

        displays the log if it exists, names should be unique
        revision allows to see older versions if they exists, defaults to newest
        """
        arguments = args.split(" ")
        if len(arguments) > 0:
            raw_data = self.backend.get_log_content(arguments[0])
            if raw_data and isinstance(raw_data, dict):
                print(f"Revision: {raw_data['revision']}, Last Check: {raw_data['last_check']}")
                print(raw_data['content'])
                return False
            elif raw_data and isinstance(raw_data, list):
                rev = -1
                if arguments[1] == "revision:":
                    try:
                        rev = int(arguments[2])
                    except TypeError:
                        print("Revision Number could not be deciphered")
                for each in raw_data:
                    if each['revision'] == rev:
                        choosen_log = each
                        break
                else:
                    choosen_log = raw_data[0]
                print(f"Revision: {choosen_log['revision']}, Last Check: {choosen_log['last_check']}")
                print(choosen_log['content'])
                return False
            print(f"Cannot locate log with name '{arguments[0]}'.")
        print("Need to specify a log name")

    def do_exit(self, line):
        """usage: exits application, saving open changes to database
        """
        return True

    #def postcmd(self, stop, line):
    #def precmd(self, line):

    def close(self):
        pass