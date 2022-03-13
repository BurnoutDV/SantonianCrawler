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

from cmd import Cmd
import database_util
import re
import logging
import json
from datetime import datetime, timedelta
from util import simple_console_view, str_refinement
__ver__ = 0.22


logger = logging.getLogger(__name__)


class SantonianShell(Cmd):
    intro = f"Welcome to Santonian Shell {__ver__}"
    prompt = "> "

    def __init__(self, db_path="santonian.db"):
        super().__init__()
        self.backend = database_util.SantonianDB(db_path)
        # this has a simple mode for just names, the get all logs would give us superflous info we dont want
        self.log_names = self.backend.list_logs_of_folder(folder="%", per_page=500)  # ? for autocomplete

    def do_list(self, args):
        """usage: list <folder/*>
            [filter: 'text']
            [order: <property> 'ASC/DESC'] (uid, name, folder, last_check, first_entry, revision, tag_date)
            [limit: <int>]
            [page: <int>]
            [view: <'complex'/'detail'>]
            [tags: <True/False>]
        example: list ARCHIVE005
                 list * filter: BIOCOM limit:10 page:3 view: details order: name ASC

        lists all logs within parameters, default limit is 20 per page, the order "tag_date" is special in the sense
        that it sorts after a unique tag type and automatically includes tags in the view and forces complex
        viewport
        """
        arguments = args.split(" ")
        catalyst = {'uid': "str", 'name': "str", 'content': "trim:50", 'folder': "str",
                    'revision': "str", 'last_check': "date_short", "tags": "str"}
        if len(arguments) == 1 and arguments[0].strip() == "":
            raw_data = self.backend.get_all_logs()
            simple_console_view([x for x in catalyst.keys()], str_refinement(raw_data, catalyst))
            return False
        if arguments[0] == "*":
            para_desc = {'filter': 'text', 'order': '2text', 'limit': "int",
                         'page': "int", 'view': 'text', 'tags': "bool"}
            fine_args = {'filter': "", 'order': ['uid', 'ASC'], 'limit': 25, 'page': 1, 'view': 'simple', 'tags': False}
            interpret_args = SantonianShell._extract_argument_parameter(args, para_desc)
            if 'order' in interpret_args and interpret_args['order'][0] == "tag_date" and 'view' not in interpret_args:
                interpret_args['view'] = "complex"  # all this to switch to complex view but give option to revert that
            fine_args.update(interpret_args)
            if fine_args['page'] < 1:
                fine_args['page'] = 1
            start = (fine_args['page']-1)*fine_args['limit']
            raw_data = self.backend.get_all_logs(start,
                                                 fine_args['limit'],
                                                 fine_args['order'][1],
                                                 fine_args['order'][0],
                                                 fine_args['tags'])
            if fine_args['view'] != "complex":
                for line in raw_data:
                    print(line['name'])
            else:
                simple_console_view([x for x in catalyst.keys()], str_refinement(raw_data, catalyst))
        else:
            files = self.backend.list_logs_of_folder(arguments[0])
            for line in files:
                print(line)

    def complete_list(self, line, text, start, end):
        sub_para = ["filter:", "order:", "limit:", "page:", "view:", "tags:"]
        if text:
            return [para for para in sub_para if para.startswith(text)]
        else:
            return []

    def do_folders(self, args):
        """usage: folders {no arguments}

        will list all folders in the database
        """
        folders = self.backend.get_all_folders()
        print(f"DB knows about {len(folders)} folders")
        for line in folders:
            print(line['name'], end="\t")
        else:
            print("")

    def do_tag(self, args):
        """usage: tag <log_name> <tag_name>

        tags a log with a previous created tag, will not work if tag does not exist"""
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

    def complete_tag(self, text, line, start, end):
        return self._complete_log_names(text, line, start, end, "tag")

    def do_create_tag(self, args):
        """usage: create_tag <name> <type:name, date or entity>

Creates a new tag or modifies the type of an already existing tag, if type is other than one of the three
it will default back to name"""
        arguments = args.split(" ")
        if len(arguments) == 2 and arguments[0].strip() != "" and arguments[1].strip() != "":
            if (packed := self.backend.create_modify_tag(arguments[0], arguments[1])) is not None:
                new_name, new_type = packed
                print(f"Created new tag with name '{new_name}' and type '{new_type}'")
            else:
                print(f"No Change, a tag with that name and type already exists")
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
                top_line = f"Revision: {choosen_log['revision']}, Last Check: {choosen_log['last_check']}"
                print(top_line)
                if choosen_log['tags'].strip() != "":
                    print(f"Assigned Tags: {choosen_log['tags']}")
                print(Cmd.ruler*len(top_line))    # ====
                print(choosen_log['content'])     # content
                print(Cmd.ruler * len(top_line))  # ====
                return False
            elif len(arguments[0]) > 0:
                possibilities = [name for name in self.log_names if name.startswith(arguments[0])]
                if 0 < len(possibilities) < 5:
                    print(f"Log '{arguments[0]}' unknown, do you mean: {', '.join(possibilities)}")
                elif len(possibilities) >= 6:
                    print(f"{len(possibilities)} matches that start with '{arguments[0]}', be more precise.")
                else:
                    print(f"Cannot locate log with name '{arguments[0]}'.")
        print("Need to specify a log name, try 'list *'")

    def complete_read(self, text, line, start, end):
        return self._complete_log_names(text, line, start, end, "read")

    def do_proc(self, args):
        """usage: proc <pro_name>

        executes set procedures over the data, mostly maintenance things

        * date_tag - puts a date tag on each log that does not posess a date tag yet, uses first date it finds"""
        arguments = args.split(" ")
        if len(arguments) != 1 or str(args).strip() == "":
            return False
        if arguments[0] == "date_tag":
            time_zero = datetime.now()
            dates = self.backend.procedure_tag_date()
            time_one = datetime.now()
            print(f"Created {len(dates)} date tags based on regex match on all entries without a date tag")

    def do_nextpage(self, line):
        """usage: nextpage {no parameters}

        calls next page of last search
        """
        print("not implemented yet")

    def do_exit(self, line):
        """usage: exit {no parameters}

        exits application, saving open changes to database
        """
        return True

    #def postcmd(self, stop, line):
    #def precmd(self, line):

    def _complete_log_names(self, text, line, start, end, command):
        if text:
            q= len(command)+1
            text = line[q:]
            suggestions = [name[(start-q):] for name in self.log_names if name.startswith(text)]
            if len(suggestions) > 10:
                suggestions = suggestions[9:]
                suggestions.append("...")
            return suggestions
        else:
            return self.log_names

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
            elif modus == "bool":  # checks if text might be some variance of tRuE
                if find := re.search(fr"\b{exstr}:\s*(\w+)\b", text):
                    if str(find[1]).strip() != "":
                        if str(find[1]).lower() == "true":
                            params[exstr] = True
                        else:
                            params[exstr] = False
            else:
                logger.warning(f"SHELL>extract_argument: called with unknown method '{modus}'")
        return params

    def close(self):
        self.backend.close()

