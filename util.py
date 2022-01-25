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

import json
import logging

logger = logging.getLogger(__name__)
_santonian_fields = ['endpoint', 'hdd', 'hdd_details', 'file', 'readfile']


def load_config(file="./config.json"):
    global _santonian_fields
    try:
        with open(file, "r") as config_file:
            data = json.load(config_file)
            if all(key in data for key in _santonian_fields):
                return data
    except FileNotFoundError:
        logger.warning("cannot find 'config.json', remember to copy 'config.example.json'")
    except json.JSONDecodeError:
        logger.critical("Config file is malformed")
    return False

