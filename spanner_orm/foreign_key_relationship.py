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
"""Helps define a foreign key relationship between two models."""

from typing import Any, Mapping, Type

import dataclasses
from spanner_orm import registry


@dataclasses.dataclass
class ForeignKeyRelationshipConstraint:
  columns: Mapping[str, str]
  referenced_table_name: str
  referenced_table: Type[Any]


class ForeignKeyRelationship:
  """Helps define a foreign key relationship between two models."""

  def __init__(self, referenced_table_name: str, columns: Mapping[str, str]):
    """Creates a ForeignKeyRelationship.

    Args:
      referenced_table_name: Name of the table which the foreign key references.
      columns: Dictionary where the keys are names of columns from the
        referencing table and the values are the names of the columns in the
        referenced table.
    """
    self.origin = None
    self.name = None
    self._referenced_table_name = referenced_table_name
    self._columns = columns

  @property
  def constraint(self) -> ForeignKeyRelationshipConstraint:
    return self._parse_constraint()

  @property
  def ddl(self) -> str:
    referencing_columns_ddl = ', '.join(self.constraint.columns.keys())
    referenced_columns_ddl = ', '.join(self.constraint.columns.values())
    return (
        'CONSTRAINT {fk_name} FOREIGN KEY ({referencing_columns}) REFERENCES'
        ' {referenced_table} ({referenced_columns})').format(
            fk_name=self.name,
            referencing_columns=referencing_columns_ddl,
            referenced_table=self.constraint.referenced_table_name,
            referenced_columns=referenced_columns_ddl,
        )

  def _parse_constraint(self) -> ForeignKeyRelationshipConstraint:
    """Return the relationship constraint."""
    referenced_table = registry.model_registry().get(
        self._referenced_table_name)
    return ForeignKeyRelationshipConstraint(
        self._columns,
        referenced_table.table,
        referenced_table,
    )

  @property
  def single(self) -> bool:
    # Spanner enforces uniqueness for values of fields referenced by
    # foreign keys, because it creates a unique index on the referenced
    # key.
    return True
