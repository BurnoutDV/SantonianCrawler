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

# ? own stuff / local points
import santonian_crawler.shell as shell

# ! base logger configuration
logging.basicConfig(filename='dreyfus.log', format='[%(asctime)s] %(levelname)s:%(message)s', level=logging.INFO)

COMMANDS = ["update", "fulldownload", "read", "tag", "search", "list", "exit"]


def line_completer(text, state):
    for cmd in COMMANDS:
        if cmd.startswith(text):
            if not state:
                return cmd
            else:
                state -= 1


if __name__ == "__main__":
    #bla = santonian.check_folders("santonian.db")
    #console = pprint.PrettyPrinter(indent=3, compact=True)
    #console.pprint(bla)
    #readline.set_completer_delims(" \t\n")
    #readline.parse_and_bind("tab: complete")
    #readline.set_completer(line_completer)
    #readline.insert_text("Welcome to Santonian Console 0.19")
    shell.SantonianShell().cmdloop()



