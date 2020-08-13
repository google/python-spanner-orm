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
"""Helps define a foreign key relationship between two models."""

from typing import List, Mapping

import dataclasses
from spanner_orm import registry


@dataclasses.dataclass
class ForeignKeyRelationshipConstraint:
  referencing_column: str
  referenced_column: str
  referenced_table_name: str


class ForeignKeyRelationship(object):
  """Helps define a foreign key relationship between two models."""

  def __init__(self,
               referenced_table_name: str,
               constraints: Mapping[str, str]):
    """Creates a ForeignKeyRelationship.

    Args:
      referenced_table_name: Name of the table which the foreign key references.
      constraints: Dictionary where the keys are names of columns from the
        referencing table and the values are the names of the columns in the
        referenced table.
      # TODO(dgorelik): Allow constraints to have custom names.
    """
    self.origin = None
    self.name = None
    self._referenced_table_name = referenced_table_name
    self._constraints = constraints

  @property
  def constraints(self) -> List[ForeignKeyRelationshipConstraint]:
    return self._parse_constraints()

  def _parse_constraints(self) -> List[ForeignKeyRelationshipConstraint]:
    """Returns a list of Constraints for the relationship."""
    constraints = []
    referenced_table = registry.model_registry().get(
      self._referenced_table_name)
    for referencing_column, referenced_column in self._constraints.items():
      constraints.append(
        ForeignKeyRelationshipConstraint(
          referencing_column,
          referenced_column,
          referenced_table.table,
        )
      )

    return constraints
