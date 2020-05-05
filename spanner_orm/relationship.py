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

from typing import Any, Dict, List, Type, Union, Optional

import dataclasses
from spanner_orm import error
from spanner_orm import registry


@dataclasses.dataclass
class RelationshipConstraint:
  destination_class: Type[Any]
  destination_column: str
  origin_class: Type[Any]
  origin_column: str


class Relationship(object):
  """Helps define a foreign key relationship between two models."""

  def __init__(self,
               destination_handle: Union[Type[Any], str],
               constraints: Dict[str, str],
               is_parent: bool = False,
               single: bool = False):
    """Creates a ModelRelationship.

    Args:
      destination_handle: Destination model class or fully qualified class name
        of the destination model
      constraints: Dictionary where the keys are names of columns from the
        origin model and the value for a key is the name of the column in the
        destination model that the key should be equal to in order for there to
        be a relationship from an origin model to the destination
      is_parent: True if the destination is a parent table of the origin
      single: True if the destination should be treated as a single object
        instead of a list of objects
    """
    self.origin = None
    self.name = None
    self._destination_handle = destination_handle
    self._destination = None
    self._constraints = constraints
    self._is_parent = is_parent
    self._single = single

  @property
  def constraints(self) -> List[RelationshipConstraint]:
    if not self.origin:
      raise error.ValidationError(
          'Origin must be set before constraints is called')
    return self._parse_constraints()

  @property
  def destination(self) -> Type[Any]:
    if not self._destination:
      self._destination = registry.model_registry().get(
          self._destination_handle)
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
