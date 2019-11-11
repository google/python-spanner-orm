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
"""Holds table-specific information to make querying spanner eaiser."""

import collections
import copy
from typing import Any, Callable, Dict, Iterable, List, Optional, Type, TypeVar, Union

from spanner_orm import api
from spanner_orm import condition
from spanner_orm import error
from spanner_orm import field
from spanner_orm import index
from spanner_orm import metadata
from spanner_orm import query
from spanner_orm import registry
from spanner_orm import relationship
from spanner_orm import table_apis

from google.cloud import spanner
from google.cloud.spanner_v1 import transaction as spanner_transaction


class ModelMetaclass(type):
  """Populates ModelMetadata based on class attributes."""

  def __new__(mcs, name: str, bases: Any, attrs: Dict[str, Any], **kwargs: Any):
    parents = [base for base in bases if isinstance(base, ModelMetaclass)]
    if not parents:
      return super().__new__(mcs, name, bases, attrs, **kwargs)

    model_metadata = metadata.ModelMetadata()
    for parent in parents:
      if 'meta' in vars(parent):
        model_metadata.add_metadata(parent.meta)

    non_model_attrs = {}
    for key, value in attrs.items():
      if key == '__table__':
        model_metadata.table = value
      elif key == '__interleaved__':
        model_metadata.interleaved = value
      if isinstance(value, field.Field):
        model_metadata.add_field(key, value)
      elif isinstance(value, index.Index):
        model_metadata.add_index(key, value)
      elif isinstance(value, relationship.Relationship):
        model_metadata.add_relation(key, value)
      else:
        non_model_attrs[key] = value

    cls = super().__new__(mcs, name, bases, non_model_attrs, **kwargs)

    # If a table is set, this class represents a complete model, so finalize
    # the metadata
    if model_metadata.table:
      model_metadata.model_class = cls
      model_metadata.finalize()
    cls.meta = model_metadata
    return cls

  def __getattr__(
      cls,
      name: str) -> Union[field.Field, relationship.Relationship, index.Index]:
    # Unclear why pylint doesn't like this
    # pylint: disable=unsupported-membership-test
    if name in cls.fields:
      return cls.fields[name]
    elif name in cls.relations:
      return cls.relations[name]
    elif name in cls.indexes:
      return cls.indexes[name]
    # pylint: enable=unsupported-membership-test
    raise AttributeError(name)

  @property
  def column_prefix(cls) -> str:
    return cls.table.split('.')[-1]

  # Table fields class methods
  @property
  def columns(cls) -> List[str]:
    return cls.meta.columns

  @property
  def indexes(cls) -> Dict[str, index.Index]:
    return cls.meta.indexes

  @property
  def interleaved(cls) -> Optional[Type['Model']]:
    if cls.meta.interleaved:
      return registry.model_registry().get(cls.meta.interleaved)
    return None

  @property
  def primary_keys(cls) -> List[str]:
    return cls.meta.primary_keys

  @property
  def relations(cls) -> Dict[str, relationship.Relationship]:
    return cls.meta.relations

  @property
  def fields(cls) -> Dict[str, field.Field]:
    return cls.meta.fields

  @property
  def table(cls):
    return cls.meta.table

  def validate_value(cls, field_name, value, error_type=error.SpannerError):
    try:
      cls.fields[field_name].validate(value)
    except error.ValidationError as ex:
      raise error_type(*ex.args)


CallableReturn = TypeVar('CallableReturn')


class ModelApi(metaclass=ModelMetaclass):
  """Implements class-level Spanner queries on top of ModelMetaclass.

  Note: all methods in this class should only be called on subclasses of Model
  that have associated tables. Violating this will cause an exception to be
  raised.
  """

  @classmethod
  def spanner_api(cls) -> api.SpannerApi:
    if not cls.table:
      raise error.SpannerError('Class must define a table for API calls')
    return api.spanner_api()

  # Table read methods
  @classmethod
  def all(
      cls,
      transaction: Optional[spanner_transaction.Transaction] = None
  ) -> List['ModelObject']:
    """Returns all objects of this type stored in Spanner.

    Note: this method should only be called on subclasses of Model that have
    a table associated with it. Violating this will cause an exception to be
    raised.

    Args:
      transaction: The existing transaction to use, or None to start a new
        transaction

    Returns:
      A list of models, one per row in the associated Spanner table
    """
    args = [cls.table, cls.columns, spanner.KeySet(all_=True)]
    results = cls._execute_read(table_apis.find, transaction, args)
    return cls._results_to_models(results)

  @classmethod
  def count(cls, transaction: Optional[spanner_transaction.Transaction],
            *conditions: condition.Condition) -> int:
    """Returns the number of objects in Spanner that match the given conditions.

    Args:
      transaction: The existing transaction to use, or None to start a new
        transaction
      *conditions: Instances of subclasses of Condition that help specify which
        rows should be included in the count. The includes condition is not
        allowed here

    Returns:
      The integer result of the COUNT query
    """
    builder = query.CountQuery(cls, conditions)
    args = [builder.sql(), builder.parameters(), builder.types()]
    results = cls._execute_read(table_apis.sql_query, transaction, args)
    return builder.process_results(results)

  @classmethod
  def count_equal(cls,
                  transaction: Optional[spanner_transaction.Transaction] = None,
                  **constraints: Any) -> int:
    """Returns the number of objects in Spanner that match the given conditions.

    Convenience method that generates equality conditions based on the keyword
    arguments.

    Args:
      transaction: The existing transaction to use, or None to start a new
        transaction
      **constraints: Each key/value pair is turned into an equality condition:
        the key is used as the column in the condition and the value is used as
        the value to compare the column against in the query.

    Returns:
      The integer result of the COUNT query
    """
    conditions = []
    for column, value in constraints.items():
      if isinstance(value, list):
        conditions.append(condition.in_list(column, value))
      else:
        conditions.append(condition.equal_to(column, value))
    return cls.count(transaction, *conditions)

  @classmethod
  def find(cls,
           transaction: Optional[spanner_transaction.Transaction] = None,
           **keys: Any) -> Optional['ModelObject']:
    """Retrieves an object from Spanner based on the provided key.

    Args:
      transaction: The existing transaction to use, or None to start a new
        transaction
      **keys: The keys provided are the complete set of primary keys for this
        table and the corresponding values make up the unique identifier of the
        object being retrieved

    Returns:
      The requested object or None if no such object exists
    """
    resources = cls.find_multi(transaction, [keys])
    return resources[0] if resources else None

  @classmethod
  def find_multi(cls, transaction: Optional[spanner_transaction.Transaction],
                 keys: Iterable[Dict[str, Any]]) -> List['ModelObject']:
    """Retrieves objects from Spanner based on the provided keys.

    Args:
      transaction: The existing transaction to use, or None to start a new
        transaction
      keys: An iterable of dictionaries, each dictionary representing the set of
        primary key values necessary to uniquely identify an object in this
        table.

    Returns:
      A list containing all requested objects that exist in the table (can be
      an empty list)
    """
    key_values = []
    for key in keys:
      key_values.append([key[column] for column in cls.primary_keys])
    keyset = spanner.KeySet(keys=key_values)

    args = [cls.table, cls.columns, keyset]
    results = cls._execute_read(table_apis.find, transaction, args)
    return cls._results_to_models(results)

  @classmethod
  def where(cls, transaction: Optional[spanner_transaction.Transaction],
            *conditions: condition.Condition) -> List['ModelObject']:
    """Retrieves objects from Spanner based on the provided conditions.

    Args:
      transaction: The existing transaction to use, or None to start a new
        transaction
      *conditions: Instances of subclasses of Condition that help specify which
        objects should be retrieved

    Returns:
      A list containing all requested objects that exist in the table (can be
      an empty list)
    """
    builder = query.SelectQuery(cls, conditions)
    args = [builder.sql(), builder.parameters(), builder.types()]
    results = cls._execute_read(table_apis.sql_query, transaction, args)
    return builder.process_results(results)

  @classmethod
  def where_equal(cls,
                  transaction: Optional[spanner_transaction.Transaction] = None,
                  **constraints: Any) -> List['ModelObject']:
    """Retrieves objects from Spanner based on the provided constraints.

    Args:
      transaction: The existing transaction to use, or None to start a new
        transaction
      **constraints: Each key/value pair is turned into an equality condition:
        the key is used as the column in the condition and the value is used as
        the value to compare the column against in the query.

    Returns:
      A list containing all requested objects that exist in the table (can be
      an empty list)
    """
    conditions = []
    for column, value in constraints.items():
      if isinstance(value, list):
        conditions.append(condition.in_list(column, value))
      else:
        conditions.append(condition.equal_to(column, value))
    return cls.where(transaction, *conditions)

  @classmethod
  def _results_to_models(cls,
                         results: Iterable[Iterable[Any]]) -> List['ModelObject']:
    items = [dict(zip(cls.columns, result)) for result in results]
    return [cls(item, persisted=True) for item in items]

  @classmethod
  def _execute_read(cls, db_api: Callable[..., CallableReturn],
                    transaction: Optional[spanner_transaction.Transaction],
                    args: List[Any]) -> CallableReturn:
    if transaction is not None:
      return db_api(transaction, *args)
    else:
      return cls.spanner_api().run_read_only(db_api, *args)

  # Table write methods
  @classmethod
  def create(cls,
             transaction: Optional[spanner_transaction.Transaction] = None,
             **kwargs: Any) -> None:
    """Creates a row in Spanner based on the provided data.

    Note: may throw an exception if bad values are provided.

    Args:
      transaction: The existing transaction to use, or None to start a new
        transaction
      **kwargs: The keys are columns on the table and the values are the values
        each column in the table should be set to. None and keys not present
        indicate the corresponding column value should be NULL.
    """
    cls._execute_write(table_apis.insert, transaction, [kwargs])

  @classmethod
  def create_or_update(cls,
                       transaction: Optional[
                           spanner_transaction.Transaction] = None,
                       **kwargs: Any) -> None:
    cls._execute_write(table_apis.upsert, transaction, [kwargs])

  @classmethod
  def delete_batch(cls, transaction: Optional[spanner_transaction.Transaction],
                   models: List['ModelObject']) -> None:
    """Deletes rows from Spanner based on the provided models' primary keys.

    Args:
      transaction: The existing transaction to use, or None to start a new
        transaction
      models: A list of models to be deleted from Spanner.
    """
    key_list = []
    for model in models:
      key_list.append([getattr(model, column) for column in cls.primary_keys])
    keyset = spanner.KeySet(keys=key_list)

    db_api = table_apis.delete
    args = [cls.table, keyset]
    if transaction is not None:
      db_api(transaction, *args)
    else:
      cls.spanner_api().run_write(db_api, *args)

  @classmethod
  def save_batch(cls,
                 transaction: Optional[spanner_transaction.Transaction],
                 models: List['ModelObject'],
                 force_write: bool = False) -> None:
    """Writes rows to Spanner based on the provided model data.

    Args:
      transaction: The existing transaction to use, or None to start a new
        transaction
      models: A list of models to be written to Spanner. If the _persisted flag
        is set, by default we try to issue an UPDATE with values set for all
        columns in the table. Otherwise, we try to issue an INSERT for all
        columns in the table. If we try to INSERTa row that already exists (or
        update one that is missing), an exception will be thrown.
      force_write: If true, we use UPSERT instead of UPDATE/INSERT, so no
        exceptions are thrown based on the presence or absence of data in
        Spanner
    """
    work = collections.defaultdict(list)
    for model in models:
      value = {column: getattr(model, column) for column in cls.columns}
      if force_write:
        api_method = table_apis.upsert
      elif model._persisted:  # pylint: disable=protected-access
        api_method = table_apis.update
      else:
        api_method = table_apis.insert
      work[api_method].append(value)
      model._persisted = True  # pylint: disable=protected-access
    for api_method, values in work.items():
      cls._execute_write(api_method, transaction, values)

  @classmethod
  def update(cls,
             transaction: Optional[spanner_transaction.Transaction] = None,
             **kwargs: Any) -> None:
    """Updates a row in Spanner based on the provided data.

    Args:
      transaction: The existing transaction to use, or None to start a new
        transaction
      **kwargs: The keys are columns on the table and the values are the values
        each column in the table should be set to. None indicates indicate the
        corresponding column value should be NULL and not present keys indicate
        the corresponding column value should not be changed.
    """
    cls._execute_write(table_apis.update, transaction, [kwargs])

  @classmethod
  def _execute_write(cls, db_api: Callable[..., Any],
                     transaction: Optional[spanner_transaction.Transaction],
                     dictionaries: Iterable[Dict[str, Any]]) -> None:
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
      return cls.spanner_api().run_write(db_api, *args)


class Model(ModelApi):
  """Maps to a table in spanner and has basic functions for querying tables."""

  def __init__(self, values: Dict[str, Any], persisted: bool = False):
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

  def __setattr__(self, name: str, value: Any) -> None:
    if name in self._relations:
      raise AttributeError(name)
    elif name in self._fields:
      if name in self._primary_keys:
        raise AttributeError(name)
      self._metaclass.validate_value(name, value, AttributeError)
    super().__setattr__(name, value)

  @property
  def _metaclass(self) -> Type['Model']:
    return type(self)

  @property
  def _columns(self) -> List[str]:
    return self._metaclass.columns

  @property
  def _fields(self) -> Dict[str, field.Field]:
    return self._metaclass.fields

  @property
  def _primary_keys(self) -> List[str]:
    return self._metaclass.primary_keys

  @property
  def _relations(self) -> Dict[str, relationship.Relationship]:
    return self._metaclass.relations

  @property
  def _table(self) -> str:
    return self._metaclass.table

  @property
  def values(self) -> Dict[str, Any]:
    """Gets all attributes.

    Returns:
      Dictionary mapping from attribute name to value.
    """
    return {key: getattr(self, key) for key in self._columns}

  def changes(self) -> Dict[str, Any]:
    """Gets all attributes that have been updated since object creation.

    Returns:
      Dictionary mapping from changed attribute name to new value.
    """
    values = self.values
    return {
        key: values[key]
        for key in self._columns
        if values[key] != self.start_values.get(key)
    }

  def delete(self, transaction: spanner_transaction.Transaction = None) -> None:
    """Deletes this object from the Spanner database.

    Args:
      transaction: The existing transaction to use, or None to start a new
        transaction
    """
    key = [getattr(self, column) for column in self._primary_keys]
    keyset = spanner.KeySet([key])

    db_api = table_apis.delete
    args = [self._table, keyset]
    if transaction is not None:
      db_api(transaction, *args)
    else:
      self.spanner_api().run_write(db_api, *args)

  def id(self) -> Dict[str, Any]:
    """Gets the identifier of this object.

    Returns:
      Dictionary mapping from primary key attribute name to values. Note: this
      dictionary can be used with Model.find to return the updated version of
      this object from Spanner.
    """
    return {key: self.values[key] for key in self._primary_keys}

  def reload(
      self,
      transaction: spanner_transaction.Transaction = None) -> Optional['Model']:
    """Refreshes this object with information from Spanner.

    Args:
      transaction: The existing transaction to use, or None to start a new
        transaction

    Returns:
      This object updated with the appropriate values if information was found
      in Spanner, or None if no information was found (object was deleted or
      never was persisted)
    """
    updated_object = self._metaclass.find(transaction, **self.id())
    if updated_object is None:
      return None
    start_values = {}

    for column in self._columns:
      value = getattr(updated_object, column)
      start_values[column] = copy.copy(value)
      if column not in self._primary_keys:
        setattr(self, column, value)

    self.start_values = start_values
    self._persisted = True
    return self

  def save(self,
           transaction: spanner_transaction.Transaction = None) -> 'Model':
    """Persists this object to Spanner.

    Note: if the _persisted flag doesn't match whether this object is actually
    stored in Spanner, an exception may be thrown due to using the wrong
    API.

    Args:
      transaction: The existing transaction to use, or None to start a new
        transaction

    Returns:
      The current object
    """
    if self._persisted:
      changed_values = self.changes()
      if changed_values:
        changed_values.update(self.id())
        self._metaclass.update(transaction, **changed_values)
    else:
      self._metaclass.create(transaction, **self.values)
      self._persisted = True
    return self


ModelObject = TypeVar('ModelObject', bound=Model)
