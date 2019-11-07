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
"""Represents an index on a Model."""

from typing import List, Optional

from spanner_orm import error


class Index(object):
  """Represents an index on a Model."""
  PRIMARY_INDEX = 'PRIMARY_KEY'

  def __init__(self,
               columns: List[str],
               parent: Optional[str] = None,
               null_filtered: bool = False,
               unique: bool = False,
               storing_columns: Optional[List[str]] = None):
    if not columns:
      raise error.ValidationError('An index must have at least one column')
    self.columns = columns
    self.name = None
    self.parent = parent
    self.null_filtered = null_filtered
    self.unique = unique
    self.storing_columns = storing_columns or []

  @property
  def primary(self) -> bool:
    return self.name == self.PRIMARY_INDEX
