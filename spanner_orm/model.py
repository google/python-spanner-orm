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
    self.primary_keys = [
        f.name for f in sorted_fields if f.primary_key()
    ]


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
    if set(kwargs.keys()) != set(cls.primary_keys):
      raise error.SpannerError('All primary index keys must be specified')

    key_values = [keys[column] for column in cls.primary_keys]
    keyset = spanner.KeySet(keys=[ordered_values])

    args = [cls.table, cls.columns, keyset]
    results = cls._execute_read(api.SpannerApi.find, transaction, args)
    resources = cls._results_to_models(results)

    return resources[0] if resources else None

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

  def save_batch(cls, transaction, models):
    """Persist all model changes in list of models to Spanner."""
    to_create, to_update = [], []
    columns = cls.columns
    for model in models:
      value = {column: getattr(model, column) for column in columns}
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

  def update(cls, transaction=None, **kwargs):
    cls._execute_write(api.SpannerApi.update, transaction, [kwargs])

  def _execute_write(cls, db_api, transaction, dictionaries):
    """Validates all write value types and commits write to Spanner."""
    columns, values = None, []
    for dictionary in dictionaries:
      invalid_keys = set(dictionary.keys()) - cls.columns
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

  # Instance methods
  def __init__(self, values, persisted=False):
    metaclass = type(self)
    self.related = {}
    self.start_values = {}
    # Ensure that we have the primary index keys (unique id) set for all objects
    missing_keys = set(metaclass.primary_keys) - set(values.keys())
    if missing_keys:
      raise error.SpannerError(
          'All primary keys must be specified. Missing: {keys}'.format(
              keys=missing_keys))

    for key, value in values.items():
      if key in metaclass.relations:
        self.related[key] = value
      elif key in metaclass.columns:
        if value is not None:
          metaclass.validate_value(key, value, ValueError)
          self.start_values[key] = copy.copy(value)
      else:
        raise ValueError('{name} is not part of {klass}'.format(
            name=key, klass=metaclass.__name__))

    self.values = copy.deepcopy(self.start_values)
    self._persisted = persisted

  def __getattr__(self, name):
    metaclass = type(self)
    if name in metaclass.schema:
      return self.values.get(name)
    elif name in metaclass.relations:
      if name in self.related:
        return self.related[name]
      raise AttributeError('{name} was not included in query'.format(name=name))
    raise AttributeError(name)

  def __setattr__(self, name, value):
    metaclass = type(self)
    if name in metaclass.primary_keys:
      raise AttributeError(name)
    elif name in metaclass.schema:
      metaclass.validate_value(name, value, AttributeError)
      self.values[name] = copy.copy(value)
    elif name in metaclass.relations:
      raise AttributeError('{name} is not settable'.format(name=name))
    else:
      super().__setattr__(name, value)

  def changes(self):
    metaclass = type(self)
    return {
        key: self.values[key]
        for key in metaclass.columns
        if self.values.get(key) != self.start_values.get(key)
    }

  def id(self):
    metaclass = type(self)
    return {key: self.values[key] for key in metaclass.primary_keys}

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
