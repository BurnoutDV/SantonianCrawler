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
import re
import logging
import json
from util import simple_console_view, str_refinement
__ver__ = 0.19


logger = logging.getLogger(__name__)


class SantonianShell(cmd.Cmd):
    intro = f"Welcome to Santonian Shell {__ver__}"
    prompt = "> "

    def __init__(self, db_path="santonian.db"):
        super().__init__()
        self.backend = database_util.SantonianDB(db_path)

    def do_list(self, args):
        """usage: list <folder/*>
            [filter: 'text']
            [order: <property> 'ASC/DESC'] (uid, name, folder, last_check, first_entry, revision)
            [limit: <int>]
            [page: <int>]
            [view: <'complex'/'detail'>]
        example: list ARCHIVE005
                 list * filter: BIOCOM limit:10 page:3 view: details order: name ASC

        lists all logs within parameters, default limit is 20 per page
        """
        arguments = args.split(" ")
        catalyst = {'uid': "str", 'name': "str", 'content': "trim:50", 'folder': "str",
                    'revision': "str", 'last_check': "date_short"}
        if len(arguments) == 1 and arguments[0].strip() == "":
            raw_data = self.backend.get_all_logs()
            simple_console_view([x for x in catalyst.keys()], str_refinement(raw_data, catalyst))
            return False
        if arguments[0] == "*":
            para_desc = {'filter': 'text', 'order': '2text', 'limit': "int", 'page': "int", 'view': 'text'}
            fine_args = {'filter': "", 'order': ['uid', 'ASC'], 'limit': 25, 'page': 1, 'view': 'simple'}
            fine_args.update(SantonianShell._extract_argument_parameter(args, para_desc))
            if fine_args['page'] < 1:
                fine_args['page'] = 1
            start = (fine_args['page']-1)*fine_args['limit']
            raw_data = self.backend.get_all_logs(start, fine_args['limit'], fine_args['order'][1], fine_args['order'][0])
            if fine_args['view'] != "complex":
                for line in raw_data:
                    print(line['name'])
            else:
                simple_console_view([x for x in catalyst.keys()], str_refinement(raw_data, catalyst))
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

    def do_tag(self, args):
        arguments = args.split(" ")
        if len(arguments) == 2:
            status = self.backend.tag_file(arguments[0], arguments[1])
            if status:
                print(f"Created new connection between file {arguments[0]} and tag {arguments[1]}")
            elif status is False:
                print(f"The tag '{arguments[1]}' does not exist")
            else:
                print(f"Cannot locate file '{arguments[0]}'")
        else:
            print("This functions needs exactly two parameters, file_name and tag_name")

    def do_createtag(self, args):
        arguments = args.split(" ")
        if len(arguments) == 2:
            new_name, new_type = self.backend.create_modify_tag(arguments[0], arguments[1])
            print(f"Created new tag with name '{new_name}' and type '{new_type}'")
        else:
            print("This functions needs exactly two parameters, tag_name and tag_type")

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
                para_desc = {'revision': "int"}
                fine_args = SantonianShell._extract_argument_parameter(args, para_desc)
                rev = -1
                if 'revision' in fine_args:
                    rev = fine_args['revision']
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

    def do_nextpage(self, line):
        """usage: calls next page of last search
        """
        print("not implemented yet")

    def do_exit(self, line):
        """usage: exits application, saving open changes to database
        """
        return True

    #def postcmd(self, stop, line):
    #def precmd(self, line):

    @staticmethod
    def _extract_argument_parameter(text: str, expectation: dict):
        # match between quotation marks with escaping (?<=(["']))(?:(?=(\\?))\2.)*?(?=\1)
        params = {}
        for exstr, modus in expectation.items():
            # throwing a bunch of regex sure does sound expensive
            if modus == "text":  # 'property: value'
                if find := re.search(fr"\b{exstr}:\s*(\w+)\b", text):
                    if str(find[1]).strip() != "":
                        params[exstr] = str(find[1]).strip()
            elif modus == "2text":
                if find := re.search(fr"\b{exstr}:\s*(\w+)\s+(\w+)\b", text):
                    if str(find[1]).strip() != "" and str(find[2]).strip() != "":
                        params[exstr] = [str(find[1]).strip(), str(find[2]).strip()]
            elif modus == "int":
                if find := re.search(fr"\b{exstr}:\s*([0-9]+)\b", text):
                    if str(find[1]).strip() != "":
                        params[exstr] = int(find[1])
            else:
                logger.warning(f"SHELL>extract_argument: called with unknown method '{modus}'")
        return params

    def close(self):
        self.backend.close()

