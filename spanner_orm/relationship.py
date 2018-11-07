# python3
# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Helps define a foreign key relationship between two models."""

import importlib

from spanner_orm import condition
from spanner_orm import error
from spanner_orm import model


class Relationship(object):
  """Helps define a foreign key relationship between two models."""

  def __init__(self,
               destination_name,
               constraints,
               is_parent=False,
               single=False):
    """Creates a ModelRelationship.

    Args:
      destination_name: Fully qualified class name of the destination model
      constraints: Dictionary where the keys are names of columns from the
        origin model and the value for a key is the name of the column in the
        destination model that the key should be equal to in order for there to
        be a relationship from an origin model to the destination
      is_parent: True if the destination is a parent table of the origin
      single: True if the destination should be treated as a single object
        instead of a list of objects
    """
    self._destination_name = destination_name
    self._destination = None
    self._constraints = constraints
    self._is_parent = is_parent
    self._single = single
    self.origin = None

  @property
  def conditions(self):
    assert self.origin, 'Origin must be set before conditions is called'
    return self._parse_constraints()

  @property
  def destination(self):
    if not self._destination:
      self._destination = self._load_model(self._destination_name)
    return self._destination

  @property
  def single(self):
    return self._single

  def _parse_constraints(self):
    """Validates the dictionary of constraints and turns it into Conditions."""
    conditions = []
    for origin_column, destination_column in self._constraints.items():
      if origin_column not in self.origin.schema():
        raise error.SpannerError(
            'Origin column must be present in origin model')

      if destination_column not in self.destination.schema():
        raise error.SpannerError(
            'Destination column must be present in destination model')
      # This is backward from what you might imagine because the condition will
      # be processed from the context of the destination model
      conditions.append(
          condition.ColumnsEqualCondition(destination_column, self.origin,
                                          origin_column))

    return conditions

  def _load_model(self, model_name):
    parts = model_name.split('.')
    path = '.'.join(parts[:-1])
    module = importlib.import_module(path)
    klass = getattr(module, parts[-1])
    if not issubclass(klass, model.Model):
      raise error.SpannerError(
          '{model} is not a Model'.format(model=model_name))
    return klass
