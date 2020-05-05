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
               storing_columns: Optional[List[str]] = None,
               name: Optional[str] = None):
    if not columns:
      raise error.ValidationError('An index must have at least one column')
    self._columns = columns
    self._name = name
    self._parent = parent
    self._null_filtered = null_filtered
    self._unique = unique
    self._storing_columns = storing_columns or []

  @property
  def columns(self) -> List[str]:
    return self._columns

  @property
  def name(self) -> Optional[str]:
    return self._name

  @name.setter
  def name(self, value: str) -> None:
    if not self._name:
      self._name = value

  @property
  def parent(self) -> Optional[str]:
    return self._parent

  @property
  def null_filtered(self) -> bool:
    return self._null_filtered

  @property
  def unique(self) -> bool:
    return self._unique

  @property
  def storing_columns(self) -> List[str]:
    return self._storing_columns

  @property
  def primary(self) -> bool:
    return self.name == self.PRIMARY_INDEX
