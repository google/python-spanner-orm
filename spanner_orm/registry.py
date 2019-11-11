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
"""Registers Model classes so they can be referenced elsewhere."""

from typing import Any, Dict, List, Type, Union

import dataclasses
from spanner_orm import error


@dataclasses.dataclass
class RegistryComponent:
  references: List[Type[Any]] = dataclasses.field(default_factory=list)

  def add(self, reference: Type[Any]) -> None:
    self.references.append(reference)


class Registry(object):

  def __init__(self):
    self._registered = {}  # type: Dict[str, RegistryComponent]

  def _name_from_class(self, klass: Type[Any]) -> str:
    return '{}.{}'.format(klass.__module__, klass.__name__)

  def register(self, to_register: Type[Any]) -> None:
    name_components = reversed(self._name_from_class(to_register).split('.'))
    name = None
    for component in name_components:
      name = name = '{}.{}'.format(component, name) if name else component
      if name not in self._registered:
        self._registered[name] = RegistryComponent()
      self._registered[name].add(to_register)

  def get(self, name: Union[Type[Any], str]) -> Type[Any]:
    if isinstance(name, type):
      name = self._name_from_class(name)

    if name not in self._registered:
      raise error.SpannerError(
          '{} was not found, verify it has been imported'.format(name))
    if len(self._registered[name].references) > 1:
      raise error.SpannerError(
          'Multiple classes match {}, add more specificity'.format(name))
    return self._registered[name].references[0]


_registry = Registry()


def model_registry():
  return _registry
