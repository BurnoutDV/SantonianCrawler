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

import unittest
from datetime import date

from santonian_crawler.util import find_date


class TestSantonian(unittest.TestCase):

    def test_find_date(self):
        list_of_dates = [
            ("9/25/43", date(2043, 9, 25)),
            ("25/9/43", None),
            ("May 2049", date(2049, 5, 1)),
            ("Mai 2049", None),
            ("March 18th, 2053", date(2053, 3, 18)),
            ("March 18th 2053", date(2053, 3, 18)),
            ("March 18ts, 2053", None),
            ("January 25 2028", date(2028, 1, 25)),
            ("531008 092419", date(2053, 10, 8)),
            ("531008.092419", date(2053, 10, 8)),
            ("datetime.type3(531008.092419)", date(2053, 10, 8)),
            ("531008 090000", date(2053, 10, 8)),
            ("531008 000100", date(2053, 10, 8)),
            ("531008 000001", date(2053, 10, 8)),
            ("531008 235959", date(2053, 10, 8)),
            ("531308 092419", None),
            ("531232 092419", None),
            ("531208 250000", None),  # this does currently work
            ("531208 096119", None),
            ("531208 612419", None),
            ("031008 092419", date(2003, 10, 8)),
            ("July 3rd, 2047", date(2047, 7, 3)),
            ("Mar 3 2049", date(2049, 3, 3)),
            ("March 3 2049", date(2049, 3, 3)),
            ("March 3rd 2049", date(2049, 3, 3)),
            ("Marsh 3rd 2049", None),
            ("Mar 3rd 2049", date(2049, 3, 3)),
            ("Mars 3rd 2049", None),
        ]
        for each in list_of_dates:
            with self.subTest(each[0]):
                self.assertEqual(find_date(each[0]), each[1])