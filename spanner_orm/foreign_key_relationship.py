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

from typing import Any, List, Mapping, Type, Union

import dataclasses
from spanner_orm import error
from spanner_orm import registry


@dataclasses.dataclass
class RelationshipConstraint:
  destination_class: Type[Any]
  destination_column: str
  origin_class: Type[Any]
  origin_column: str


class ForeignKeyRelationship(object):
  """Helps define a foreign key relationship between two models."""

  def __init__(self,
               referenced_table_name: str,
               constraints: Mapping[str, str]):
    """Creates a ForeignKeyRelationship.

    Args:
      referenced_table_name: Destination model class or fully qualified class
        name of the destination model.
      constraints: Dictionary where the keys are names of columns from the
        referencing table and the values are the names of the columns in the
        referenced table.
    """
    self.origin = None
    self.name = None
    self._referenced_table_name = referenced_table_name
    self._constraints = constraints

  @property
  def constraints(self) -> List[RelationshipConstraint]:
    return self._constraints

  @property
  def destination(self) -> Type[Any]:
    return self._referenced_table_name
    if not self._destination:
      self._destination = registry.model_registry().get(
          self._referenced_table_name)
    return self._destination

  @property
  def single(self) -> bool:
    return self._single

  def _parse_constraints(self) -> List[RelationshipConstraint]:
    """Validates the dictionary of constraints and turns it into Conditions."""
    constraints = []
    for origin_column, destination_column in self._constraints.items():
      if origin_column not in self.origin.fields:
        raise error.ValidationError(
            'Origin column must be present in origin model')

      if destination_column not in self.destination.fields:
        raise error.ValidationError(
           'Destination column must be present in destination model')

      # TODO(dbrandao): remove when pytype #234 is fixed
      constraints.append(
          RelationshipConstraint(self.destination, destination_column,
                                 self.origin, origin_column))  # type: ignore

    return constraints
