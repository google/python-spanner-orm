# Lint as: python3
# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Tests for spanner_orm.condition."""

import logging
import unittest

import spanner_orm


class ConditionTest(unittest.TestCase):

  def test_contains(self):
    contains = spanner_orm.contains('some_column', r'a%b_c\d')
    self.assertEqual('some_column', contains.column)
    self.assertEqual('LIKE', contains.operator)
    self.assertEqual(r'%a\%b\_c\\d%', contains.value)


if __name__ == '__main__':
  logging.basicConfig()
  unittest.main()
