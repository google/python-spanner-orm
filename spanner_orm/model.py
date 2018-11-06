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
"""Holds table-specific information to make querying spanner eaiser."""

import abc
import copy

from spanner_orm import api
from spanner_orm import condition
from spanner_orm import error
from spanner_orm import query

from google.cloud import spanner


class Model(abc.ABC):
  """Maps to a table in spanner and has basic functions for querying tables."""

  @classmethod
  def column_prefix(cls):
    return cls.table().split('.')[-1]

  # Table schema class methods
  @classmethod
  def columns(cls):
    return set(cls.schema())

  @staticmethod
  @abc.abstractmethod
  def primary_index_keys():
    raise NotImplementedError

  @classmethod
  def relations(cls):
    return {}

  @classmethod
  @abc.abstractmethod
  def schema(cls):
    raise NotImplementedError

  @classmethod
  @abc.abstractmethod
  def table(cls):
    raise NotImplementedError

  @classmethod
  def create_table_ddl(cls):
    fields = [
        '{} {}'.format(name, field.full_ddl())
        for name, field in cls.schema().items()
    ]
    field_ddl = '({})'.format(', '.join(fields))
    index_ddl = 'PRIMARY KEY ({})'.format(', '.join(cls.primary_index_keys()))
    return 'CREATE TABLE {} {} {}'.format(cls.table(), field_ddl, index_ddl)

  @classmethod
  def _validate(cls, name, value):
    cls.schema()[name].validate(value)

  # Instance methods
  def __init__(self, values, persisted=False):
    # Ensure that we have the primary index keys (unique id) set for all objects
    missing_keys = set(self.primary_index_keys()) - set(values.keys())
    if missing_keys:
      raise error.SpannerError(
          'All primary keys must be specified. Missing: {keys}'.format(
              keys=missing_keys))

    self.start_values = {
        key: copy.copy(value)
        for key, value in values.items()
        if key in self.columns() and value is not None
    }
    for name, value in self.start_values.items():
      try:
        self._validate(name, value)
      except AssertionError as ex:
        raise error.SpannerError(*ex.args)
    self.values = copy.deepcopy(self.start_values)
    self._persisted = persisted

  def __getattr__(self, name):
    if name in self.schema():
      return self.values.get(name)
    else:
      raise AttributeError(name)

  def __getitem__(self, name):
    if name in self.schema():
      return self.values.get(name)
    else:
      raise KeyError(name)

  def __setattr__(self, name, value):
    if name in self.primary_index_keys():
      raise AttributeError(name)
    elif name in self.schema():
      try:
        self._validate(name, value)
      except AssertionError as ex:
        raise error.SpannerError(ex.args)
      self.values[name] = copy.copy(value)
    else:
      super().__setattr__(name, value)

  def __setitem__(self, name, value):
    if name in self.primary_index_keys():
      raise KeyError(name)
    elif name in self.schema():
      try:
        self._validate(name, value)
      except AssertionError as ex:
        raise error.SpannerError(ex.args)
      self.values[name] = copy.copy(value)
    else:
      raise KeyError(name)

  def changes(self):
    return {
        key: self.values[key]
        for key in self.columns()
        if self.values.get(key) != self.start_values.get(key)
    }

  def id(self):
    return {key: self.values[key] for key in self.primary_index_keys()}

  def reload(self, transaction=None):
    updated_object = self.find(transaction, **self.id())
    if updated_object is None:
      return None
    self.values = updated_object.values
    self._persisted = True
    return self

  def save(self, transaction=None):
    if self._persisted:
      changed_values = self.changes()
      if changed_values:
        changed_values.update(self.id())
        self.update(transaction, **changed_values)
    else:
      self.create(transaction, **self.values)
      self._persisted = True
    return self

  # Table read methods
  @classmethod
  def all(cls, transaction=None):
    args = [cls.table(), cls.columns(), spanner.KeySet(all_=True)]
    results = cls._execute_read(api.SpannerApi.find, transaction, args)
    return cls._results_to_models(results)

  @classmethod
  def count(cls, transaction, *conditions):
    """Implementation of the SELECT COUNT query. Requires SqlConditions."""
    builder = query.CountQuery(cls, conditions)
    args = [builder.sql(), builder.parameters(), builder.types()]
    results = cls._execute_read(api.SpannerApi.sql_query, transaction, args)
    return builder.parse_results(results)

  @classmethod
  def count_equal(cls, transaction=None, **kwargs):
    """Creates and executes a SELECT COUNT query from kwargs."""
    conditions = []
    for k, v in kwargs.items():
      if isinstance(v, list):
        conditions.append(condition.InListCondition(k, v))
      else:
        conditions.append(condition.EqualityCondition(k, v))
    return cls.count(transaction, *conditions)

  @classmethod
  def find(cls, transaction=None, **kwargs):
    """Grabs the row with the given primary_key."""
    # Make sure that all primary keys were included (sometimes multiple
    # primary keys for a table)
    index_keys = list(cls.primary_index_keys())
    if set(kwargs.keys()) != set(index_keys):
      raise error.SpannerError(
          'All primary index keys must be specified')

    # Keys need to be in specfic order
    ordered_values = [kwargs[column] for column in index_keys]

    # Get all columns for row
    keyset = spanner.KeySet(keys=[ordered_values])

    args = [cls.table(), cls.columns(), keyset]
    results = cls._execute_read(api.SpannerApi.find, transaction, args)
    resources = cls._results_to_models(results)

    return resources[0] if resources else None

  @classmethod
  def where(cls, transaction, *conditions):
    """Implementation of the SELECT query. Requires list of SqlConditions."""
    builder = query.SelectQuery(cls, conditions)
    args = [builder.sql(), builder.parameters(), builder.types()]
    results = cls._execute_read(api.SpannerApi.sql_query, transaction, args)
    return builder.parse_results(results)

  @classmethod
  def where_equal(cls, transaction=None, **kwargs):
    """Creates and executes a SELECT query from kwargs."""
    conditions = []
    for k, v in kwargs.items():
      if isinstance(v, list):
        conditions.append(condition.InListCondition(k, v))
      else:
        conditions.append(condition.EqualityCondition(k, v))
    return cls.where(transaction, *conditions)

  @classmethod
  def _results_to_models(cls, results):
    items = [dict(zip(cls.columns(), result)) for result in results]
    return [cls(item, persisted=True) for item in items]

  @staticmethod
  def _execute_read(db_api, transaction, args):
    if transaction is not None:
      return db_api(transaction, *args)
    else:
      return api.SpannerApi.run_read_only(db_api, *args)

  # Table write methods
  @classmethod
  def create(cls, transaction=None, **kwargs):
    cls._execute_write(api.SpannerApi.insert, transaction, [kwargs])

  @classmethod
  def create_or_update(cls, transaction=None, **kwargs):
    cls._execute_write(api.SpannerApi.upsert, transaction, [kwargs])

  @classmethod
  def save_batch(cls, transaction, models):
    """Persist all model changes in list of models to Spanner."""
    to_create, to_update = [], []
    columns = cls.columns()
    for model in models:
      value = {column: model[column] for column in columns}
      if model._persisted:  # pylint: disable=protected-access
        if model.changes():
          to_update.append(value)
      else:
        model._persisted = True  # pylint: disable=protected-access
        to_create.append(value)
    if to_create:
      cls._execute_write(api.SpannerApi.insert, transaction, to_create)
    if to_update:
      cls._execute_write(api.SpannerApi.update, transaction, to_update)

  @classmethod
  def update(cls, transaction=None, **kwargs):
    cls._execute_write(api.SpannerApi.update, transaction, [kwargs])

  @classmethod
  def _execute_write(cls, db_api, transaction, dictionaries):
    """Validates all write value types and commits write to Spanner."""
    columns, values = None, []
    # Assert that we're only setting columns that belong to this table
    for dictionary in dictionaries:
      assert not set(dictionary.keys()) - cls.columns()
      if columns is None:
        columns = dictionary.keys()
      # All dictionaries should have the same set of fields specified
      assert columns == dictionary.keys()
      for key, value in dictionary.items():
        cls._validate(key, value)
      values.append([dictionary[column] for column in columns])

    args = [cls.table(), columns, values]
    if transaction is not None:
      return db_api(transaction, *args)
    else:
      return api.SpannerApi.run_write(db_api, *args)
