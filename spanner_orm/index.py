# python3
# Copyright 2019 Google LLC
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
"""Helps to deal with indices on Models."""


class Index(object):
  PRIMARY_INDEX = 'PRIMARY_KEY'

  def __init__(self,
               columns,
               parent=None,
               null_filtered=False,
               unique=False,
               storing_columns=None):
    assert columns, 'An index must have at least one column'
    self.columns = columns
    self.name = None
    self.parent = parent
    self.null_filtered = null_filtered
    self.unique = unique
    self.storing_columns = storing_columns or []

  @property
  def primary(self):
    return self.name == self.PRIMARY_INDEX
