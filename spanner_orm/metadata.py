# python3
# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
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
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Hold information about a Model extracted from the class attributes."""

from typing import Any, Dict, Type, Optional

from spanner_orm import error
from spanner_orm import field
from spanner_orm import index
from spanner_orm import registry
from spanner_orm import relationship


class ModelMetadata(object):
  """Hold information about a Model extracted from the class attributes."""

  def __init__(self,
               table: Optional[str] = None,
               fields: Optional[Dict[str, field.Field]] = None,
               relations: Optional[Dict[str, relationship.Relationship]] = None,
               indexes: Optional[Dict[str, index.Index]] = None,
               interleaved: Optional[str] = None,
               model_class: Optional[Type[Any]] = None):
    self.columns = []
    self.fields = dict(fields or {})
    self._finalized = False
    self.indexes = dict(indexes or {})
    self.interleaved = interleaved
    self.model_class = model_class
    self.primary_keys = []
    self.relations = dict(relations or {})
    self.table = table or ''

  def finalize(self) -> None:
    """Finish generating metadata state.

    Some metadata depends on having all configuration data set before it can
    be calculated--the primary index, for example, needs all fields to be added
    before it can be calculated. This method is called to indicate that all
    relevant state has been added and the calculation of the final data should
    now happen.
    """
    if self._finalized:
      raise error.SpannerError('Metadata was already finalized')
    sorted_fields = list(sorted(self.fields.values(), key=lambda f: f.position))

    if index.Index.PRIMARY_INDEX not in self.indexes:
      primary_keys = [f.name for f in sorted_fields if f.primary_key]
      primary_index = index.Index(primary_keys)
      primary_index.name = index.Index.PRIMARY_INDEX
      self.indexes[index.Index.PRIMARY_INDEX] = primary_index
    self.primary_keys = self.indexes[index.Index.PRIMARY_INDEX].columns

    self.columns = [f.name for f in sorted_fields]

    for _, relation in self.relations.items():
      relation.origin = self.model_class
    registry.model_registry().register(self.model_class)
    self._finalized = True

  def add_metadata(self, metadata: 'ModelMetadata') -> None:
    self.table = metadata.table or self.table
    self.fields.update(metadata.fields)
    self.relations.update(metadata.relations)
    self.indexes.update(metadata.indexes)
    self.interleaved = metadata.interleaved or self.interleaved

  def add_field(self, name: str, new_field: field.Field) -> None:
    new_field.name = name
    new_field.position = len(self.fields)
    if new_field.name in self.fields:
      raise error.SpannerError('Already contains a field named "{}"'.format(new_field.name))
    self.fields[name] = new_field

  def add_relation(self, name: str,
                   new_relation: relationship.Relationship) -> None:
    new_relation.name = name
    self.relations[name] = new_relation

  def add_index(self, name: str, new_index: index.Index) -> None:
    new_index.name = name
    if new_index.name in self.indexes:
      raise error.SpannerError('Already contains an index named "{}"'.format(new_index.name))
    self.indexes[name] = new_index
