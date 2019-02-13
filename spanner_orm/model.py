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

import collections
import copy

from spanner_orm import api
from spanner_orm import condition
from spanner_orm import error
from spanner_orm import field
from spanner_orm import query
from spanner_orm import relationship

from google.cloud import spanner


class Metadata(object):
  """Holds Spanner table metadata corresponding to a Model."""

  def __init__(self, table=None, fields=None, relations=None):
    self.table = table or ''
    self.fields = fields or {}
    self.relations = relations or {}
    self.process_fields()

  def add_metadata(self, metadata):
    self.table = metadata.table or self.table
    self.fields.update(metadata.fields)
    self.relations.update(metadata.relations)
    self.process_fields()

  def process_fields(self):
    sorted_fields = list(sorted(self.fields.values(), key=lambda f: f.index))
    self.columns = [f.name for f in sorted_fields]
    self.primary_keys = [f.name for f in sorted_fields if f.primary_key()]


class ModelBase(type):
  """Populates Model metadata based on class attributes."""

  def __new__(mcs, name, bases, attrs, **kwargs):
    parents = [base for base in bases if isinstance(base, ModelBase)]
    if not parents:
      return super().__new__(mcs, name, bases, attrs, **kwargs)

    metadata = Metadata()
    for parent in parents:
      if 'meta' in vars(parent):
        metadata.add_metadata(parent.meta)

    non_model_attrs = {}
    for key, value in attrs.items():
      if key == '__table__':
        metadata.table = value
      if isinstance(value, field.Field):
        value.name = key
        value.index = len(metadata.fields)
        metadata.fields[key] = value
      elif isinstance(value, relationship.Relationship):
        value.name = key
        metadata.relations[key] = value
      else:
        non_model_attrs[key] = value
    metadata.process_fields()

    cls = super().__new__(mcs, name, bases, non_model_attrs, **kwargs)
    for _, relation in metadata.relations.items():
      relation.origin = cls
    cls.meta = metadata
    return cls

  def __getattr__(cls, name):
    if name in cls.schema:
      return cls.schema[name]
    elif name in cls.relations:
      return cls.relations[name]
    raise AttributeError(name)

  @property
  def creation_ddl(cls):
    fields = [
        '{} {}'.format(name, field.ddl()) for name, field in cls.schema.items()
    ]
    field_ddl = '({})'.format(', '.join(fields))
    index_ddl = 'PRIMARY KEY ({})'.format(', '.join(cls.primary_keys))
    return 'CREATE TABLE {} {} {}'.format(cls.table, field_ddl, index_ddl)

  @property
  def column_prefix(cls):
    return cls.table.split('.')[-1]

  # Table schema class methods
  @property
  def columns(cls):
    return cls.meta.columns

  @property
  def primary_keys(cls):
    return cls.meta.primary_keys

  @property
  def relations(cls):
    return cls.meta.relations

  @property
  def schema(cls):
    return cls.meta.fields

  @property
  def table(cls):
    return cls.meta.table

  def validate_value(cls, field_name, value, error_type=error.SpannerError):
    try:
      cls.schema[field_name].validate(value)
    except AssertionError as ex:
      raise error_type(*ex.args)


class ModelMeta(ModelBase):
  """Implements Spanner queries on top of ModelBase."""

  # Table read methods
  def all(cls, transaction=None):
    args = [cls.table, cls.columns, spanner.KeySet(all_=True)]
    results = cls._execute_read(api.SpannerApi.find, transaction, args)
    return cls._results_to_models(results)

  def count(cls, transaction, *conditions):
    """Implementation of the SELECT COUNT query."""
    builder = query.CountQuery(cls, conditions)
    args = [builder.sql(), builder.parameters(), builder.types()]
    results = cls._execute_read(api.SpannerApi.sql_query, transaction, args)
    return builder.process_results(results)

  def count_equal(cls, transaction=None, **constraints):
    """Creates and executes a SELECT COUNT query from constraints."""
    conditions = []
    for column, value in constraints.items():
      if isinstance(value, list):
        conditions.append(condition.in_list(column, value))
      else:
        conditions.append(condition.equal_to(column, value))
    return cls.count(transaction, *conditions)

  def find(cls, transaction=None, **keys):
    """Grabs the row with the given primary key."""
    resources = cls.find_multi(transaction, [keys])
    return resources[0] if resources else None

  def find_multi(cls, transaction, keys):
    key_values = []
    for key in keys:
      key_values.append([key[column] for column in cls.primary_keys])
    keyset = spanner.KeySet(keys=key_values)

    args = [cls.table, cls.columns, keyset]
    results = cls._execute_read(api.SpannerApi.find, transaction, args)
    resources = cls._results_to_models(results)
    return resources

  def where(cls, transaction, *conditions):
    """Implementation of the SELECT query."""
    builder = query.SelectQuery(cls, conditions)
    args = [builder.sql(), builder.parameters(), builder.types()]
    results = cls._execute_read(api.SpannerApi.sql_query, transaction, args)
    return builder.process_results(results)

  def where_equal(cls, transaction=None, **constraints):
    """Creates and executes a SELECT query from constraints."""
    conditions = []
    for column, value in constraints.items():
      if isinstance(value, list):
        conditions.append(condition.in_list(column, value))
      else:
        conditions.append(condition.equal_to(column, value))
    return cls.where(transaction, *conditions)

  def _results_to_models(cls, results):
    items = [dict(zip(cls.columns, result)) for result in results]
    return [cls(item, persisted=True) for item in items]

  def _execute_read(cls, db_api, transaction, args):
    if transaction is not None:
      return db_api(transaction, *args)
    else:
      return api.SpannerApi.run_read_only(db_api, *args)

  # Table write methods
  def create(cls, transaction=None, **kwargs):
    cls._execute_write(api.SpannerApi.insert, transaction, [kwargs])

  def create_or_update(cls, transaction=None, **kwargs):
    cls._execute_write(api.SpannerApi.upsert, transaction, [kwargs])

  def delete_batch(cls, transaction, models):
    key_list = []
    for model in models:
      key_list.append([getattr(model, column) for column in cls.primary_keys])
    keyset = spanner.KeySet(keys=key_list)

    db_api = api.SpannerApi.delete
    args = [cls.table, keyset]
    if transaction is not None:
      return db_api(transaction, *args)
    else:
      return api.SpannerApi.run_write(db_api, *args)

  def save_batch(cls, transaction, models, force_write=False):
    """Persist all model changes in list of models to Spanner.

    Note that if the transaction provided is None, multiple transactions may
    be created when calling this method.
    """
    work = collections.defaultdict(list)
    to_create, to_update = [], []
    for model in models:
      value = {column: getattr(model, column) for column in cls.columns}
      if force_write:
        api_method = api.SpannerApi.upsert
      elif model._persisted:  # pylint: disable=protected-access
        api_method = api.SpannerApi.update
      else:
        api_method = api.SpannerApi.insert
      work[api_method].append(value)
      model._persisted = True  # pylint: disable=protected-access
    for api_method, values in work.items():
      cls._execute_write(api_method, transaction, values)

  def update(cls, transaction=None, **kwargs):
    cls._execute_write(api.SpannerApi.update, transaction, [kwargs])

  def _execute_write(cls, db_api, transaction, dictionaries):
    """Validates all write value types and commits write to Spanner."""
    columns, values = None, []
    for dictionary in dictionaries:
      invalid_keys = set(dictionary.keys()) - set(cls.columns)
      if invalid_keys:
        raise error.SpannerError('Invalid keys set on {model}: {keys}'.format(
            model=cls.__name__, keys=invalid_keys))

      if columns is None:
        columns = dictionary.keys()
      if columns != dictionary.keys():
        raise error.SpannerError(
            'Attempted to update rows with different sets of keys')

      for key, value in dictionary.items():
        cls.validate_value(key, value, error.SpannerError)
      values.append([dictionary[column] for column in columns])

    args = [cls.table, columns, values]
    if transaction is not None:
      return db_api(transaction, *args)
    else:
      return api.SpannerApi.run_write(db_api, *args)


class Model(object, metaclass=ModelMeta):
  """Maps to a table in spanner and has basic functions for querying tables."""

  def __init__(self, values, persisted=False):
    start_values = {}
    self.__dict__['start_values'] = start_values
    self.__dict__['_persisted'] = persisted

    # If the values came from Spanner, trust them and skip validation
    if not persisted:
      # An object is invalid if primary key values are missing
      missing_keys = set(self._primary_keys) - set(values.keys())
      if missing_keys:
        raise error.SpannerError(
            'All primary keys must be specified. Missing: {keys}'.format(
                keys=missing_keys))

      for column in self._columns:
        self._metaclass.validate_value(column, values.get(column), ValueError)

    for column in self._columns:
      value = values.get(column)
      start_values[column] = copy.copy(value)
      self.__dict__[column] = value

    for relation in self._relations:
      if relation in values:
        self.__dict__[relation] = values[relation]

  def __setattr__(self, name, value):
    if name in self._relations:
      raise AttributeError(name)
    elif name in self._fields:
      if name in self._primary_keys:
        raise AttributeError(name)
      self._metaclass.validate_value(name, value, AttributeError)
    super().__setattr__(name, value)

  @property
  def _metaclass(self):
    return type(self)

  @property
  def _columns(self):
    return self._metaclass.columns

  @property
  def _fields(self):
    return self._metaclass.schema

  @property
  def _primary_keys(self):
    return self._metaclass.primary_keys

  @property
  def _relations(self):
    return self._metaclass.relations

  @property
  def _table(self):
    return self._metaclass.table

  @property
  def values(self):
    return {key: getattr(self, key) for key in self._columns}

  def changes(self):
    values = self.values
    return {
        key: values[key]
        for key in self._columns
        if values[key] != self.start_values.get(key)
    }

  def delete(self, transaction=None):
    key = [getattr(self, column) for column in self._primary_keys]
    keyset = spanner.KeySet([key])

    db_api = api.SpannerApi.delete
    args = [self._table, keyset]
    if transaction is not None:
      return db_api(transaction, *args)
    else:
      return api.SpannerApi.run_write(db_api, *args)

  def id(self):
    return {key: self.values[key] for key in self._primary_keys}

  def reload(self, transaction=None):
    updated_object = self._metaclass.find(transaction, **self.id())
    if updated_object is None:
      return None
    for column in self._columns:
      if column not in self._primary_keys:
        setattr(self, column, getattr(updated_object, column))
    self._persisted = True
    return self

  def save(self, transaction=None):
    if self._persisted:
      changed_values = self.changes()
      if changed_values:
        changed_values.update(self.id())
        self._metaclass.update(transaction, **changed_values)
    else:
      self._metaclass.create(transaction, **self.values)
      self._persisted = True
    return self
