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

import datetime
import logging
import unittest
from unittest import mock

from spanner_orm import condition
from spanner_orm import error
from spanner_orm import field
from spanner_orm import query
from spanner_orm.tests import models

from google.cloud.spanner_v1.proto import type_pb2


def now():
  return datetime.datetime.now(tz=datetime.timezone.utc)


class SqlBodyTest(unittest.TestCase):

  @mock.patch('spanner_orm.api.SpannerApi')
  def test_where(self, mock_db):
    models.UnittestModel.where_equal(True, int_=3)
    ((_, sql, parameters, types), _) = mock_db.sql_query.call_args
    expected_sql = 'SELECT .* FROM table WHERE table.int_ = @int_0'
    self.assertRegex(sql, expected_sql)
    self.assertEqual({'int_0': 3}, parameters)
    self.assertEqual(types, {'int_0': field.Integer.grpc_type()})

  @mock.patch('spanner_orm.api.SpannerApi')
  def test_count(self, mock_db):
    column, value = ('int_', 3)
    models.UnittestModel.count_equal(True, **{column: value})
    ((_, sql, parameters, types), _) = mock_db.sql_query.call_args

    column_key = '{}0'.format(column)
    expected_sql = r'SELECT COUNT\(\*\) FROM table WHERE table.{} = @{}'.format(
        column, column_key)
    self.assertRegex(sql, expected_sql)
    self.assertEqual({column_key: value}, parameters)
    self.assertEqual(types, {column_key: field.Integer.grpc_type()})

  def test_query_limit(self):
    key, value = ('limit0', 2)
    query_ = query.SelectQuery(models.UnittestModel, [condition.limit(value)])

    self.assertTrue(query_.sql().endswith(' LIMIT @{}'.format(key)))
    self.assertEqual(query_.parameters(), {key: value})
    self.assertEqual(query_.types(), {key: field.Integer.grpc_type()})

    query_ = query.SelectQuery(models.UnittestModel, [])
    self.assertEqual(query_._limit(), ('', {}, {}))

  def test_query_limit_offset(self):
    limit_key, limit = 'limit0', 2
    offset_key, offset = 'offset0', 5
    query_ = query.SelectQuery(models.UnittestModel,
                               [condition.limit(limit, offset=offset)])

    self.assertTrue(query_.sql().endswith(' LIMIT @{} OFFSET @{}'.format(
        limit_key, offset_key)))
    self.assertEqual(query_.parameters(), {
        limit_key: limit,
        offset_key: offset
    })
    self.assertEqual(query_.types(), {
        limit_key: field.Integer.grpc_type(),
        offset_key: field.Integer.grpc_type()
    })

    query_ = query.SelectQuery(models.UnittestModel, [])
    self.assertEqual(query_._limit(), ('', {}, {}))

  def test_query__order_by(self):
    order = ('int_', condition.OrderType.DESC)
    query_ = query.SelectQuery(models.UnittestModel,
                               [condition.order_by(order)])

    self.assertTrue(query_.sql().endswith(' ORDER BY table.int_ DESC'))
    self.assertEqual(query_.parameters(), {})
    self.assertEqual(query_.types(), {})

    query_ = query.SelectQuery(models.UnittestModel, [])
    self.assertEqual(query_._order(), ('', {}, {}))

  def test_query__where_comparison(self):
    tuples = [('int_', 5, field.Integer.grpc_type()),
              ('string', 'foo', field.String.grpc_type()),
              ('timestamp', now(), field.Timestamp.grpc_type())]
    conditions = [
        condition.greater_than, condition.not_less_than, condition.less_than,
        condition.not_greater_than, condition.equal_to, condition.not_equal_to
    ]
    for column, value, type_ in tuples:
      for condition_generator in conditions:
        current_condition = condition_generator(column, value)
        query_ = query.SelectQuery(models.UnittestModel, [current_condition])

        column_key = '{}0'.format(column)
        expected_where = ' WHERE table.{} {} @{}'.format(
            column, current_condition.operator(), column_key)
        self.assertTrue(query_.sql().endswith(expected_where))
        self.assertEqual(query_.parameters(), {column_key: value})
        self.assertEqual(query_.types(), {column_key: type_})

  def test_query__where_list_comparison(self):
    tuples = [('int_', [1, 2, 3], field.Integer.grpc_type()),
              ('string', ['a', 'b', 'c'], field.String.grpc_type()),
              ('timestamp', [now()], field.Timestamp.grpc_type())]

    conditions = [condition.in_list, condition.not_in_list]
    for column, values, type_ in tuples:
      for condition_generator in conditions:
        current_condition = condition_generator(column, values)
        query_ = query.SelectQuery(models.UnittestModel, [current_condition])

        column_key = '{}0'.format(column)
        expected_sql = ' WHERE table.{} {} UNNEST(@{})'.format(
            column, current_condition.operator(), column_key)
        list_type = type_pb2.Type(code=type_pb2.ARRAY, array_element_type=type_)
        self.assertTrue(query_.sql().endswith(expected_sql))
        self.assertEqual(query_.parameters(), {column_key: values})
        self.assertEqual(query_.types(), {column_key: list_type})

  def test_query__combines_properly(self):
    query_ = query.SelectQuery(models.UnittestModel, [
        condition.equal_to('int_', 5),
        condition.not_equal_to('string_array', ['foo', 'bar']),
        condition.limit(2),
        condition.order_by(('string', condition.OrderType.DESC))
    ])
    expected_sql = ('WHERE table.int_ = @int_0 AND table.string_array != '
                    '@string_array1 ORDER BY table.string DESC LIMIT @limit2')
    self.assertTrue(query_.sql().endswith(expected_sql))

  def test_only_one_limit_allowed(self):
    with self.assertRaises(error.SpannerError):
      query.SelectQuery(
          models.UnittestModel,
          [condition.limit(2), condition.limit(2)])

  def test_includes(self):
    query_ = query.SelectQuery(models.ChildTestModel,
                               [condition.includes('parent')])
    # The column order varies between test runs
    expected_sql = (
        r'SELECT ChildTestModel\S* ChildTestModel\S* ARRAY\(SELECT AS '
        r'STRUCT SmallTestModel\S* SmallTestModel\S* SmallTestModel\S* FROM '
        r'SmallTestModel WHERE SmallTestModel.key = '
        r'ChildTestModel.parent_key\)')
    (sql, _, _) = query_._select()
    self.assertRegex(sql, expected_sql)

  def test_includes_subconditions_query(self):
    query_ = query.SelectQuery(
        models.ChildTestModel,
        [condition.includes('parents', [condition.equal_to('key', 'value')])])
    expected_sql = ('WHERE SmallTestModel.key = ChildTestModel.parent_key '
                    'AND SmallTestModel.key = @key0')
    self.assertRegex(query_.sql(), expected_sql)

  def includes_result(self, related=1):
    child = {'parent_key': 'parent_key', 'child_key': 'child'}
    result = [child[name] for name in models.ChildTestModel.columns]
    parent = {'key': 'key', 'value_1': 'value_1', 'value_2': None}
    parents = []
    for _ in range(related):
      parents.append([parent[name] for name in models.SmallTestModel.columns])
    result.append(parents)
    return child, parent, [result]

  def test_includes_single_related_object_result(self):
    query_ = query.SelectQuery(models.ChildTestModel,
                               [condition.includes('parent')])
    child_values, parent_values, rows = self.includes_result(related=1)
    result = query_.process_results(rows)[0]

    self.assertIsInstance(result.parent, models.SmallTestModel)
    for name, value in child_values.items():
      self.assertEqual(getattr(result, name), value)

    for name, value in parent_values.items():
      self.assertEqual(getattr(result.parent, name), value)

  def test_includes_single_no_related_object_result(self):
    query_ = query.SelectQuery(models.ChildTestModel,
                               [condition.includes('parent')])
    child_values, _, rows = self.includes_result(related=0)
    result = query_.process_results(rows)[0]

    self.assertIsNone(result.parent)
    for name, value in child_values.items():
      self.assertEqual(getattr(result, name), value)

  def test_includes_subcondition_result(self):
    query_ = query.SelectQuery(
        models.ChildTestModel,
        [condition.includes('parents', [condition.equal_to('key', 'value')])])

    child_values, parent_values, rows = self.includes_result(related=2)
    result = query_.process_results(rows)[0]

    self.assertEqual(len(result.parents), 2)
    for name, value in child_values.items():
      self.assertEqual(getattr(result, name), value)

    for name, value in parent_values.items():
      self.assertEqual(getattr(result.parents[0], name), value)

  def test_includes_error_on_multiple_results_for_single(self):
    query_ = query.SelectQuery(models.ChildTestModel,
                               [condition.includes('parent')])
    _, _, rows = self.includes_result(related=2)
    with self.assertRaises(error.SpannerError):
      _ = query_.process_results(rows)

  def test_includes_error_on_invalid_relation(self):
    with self.assertRaises(AssertionError):
      query.SelectQuery(models.ChildTestModel,
                        [condition.includes('bad_relation')])

  def test_includes_error_on_invalid_subconditions(self):
    with self.assertRaises(AssertionError):
      query.SelectQuery(
          models.ChildTestModel,
          [condition.includes('parent', [condition.equal_to('bad_column', 0)])])

    with self.assertRaises(AssertionError):
      query.SelectQuery(
          models.ChildTestModel,
          [condition.includes('parent', [condition.equal_to('child_key', 0)])])

    with self.assertRaises(AssertionError):
      query.SelectQuery(
          models.ChildTestModel,
          [condition.includes('parent', [condition.equal_to('key', 0)])])


if __name__ == '__main__':
  logging.basicConfig()
  unittest.main()
