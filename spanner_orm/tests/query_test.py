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
import datetime
import logging
import unittest
from unittest import mock

from absl.testing import parameterized
from spanner_orm import condition
from spanner_orm import error
from spanner_orm import field
from spanner_orm import query
from spanner_orm.tests import models

from google.cloud.spanner_v1.proto import type_pb2


def now():
  return datetime.datetime.now(tz=datetime.timezone.utc)


class QueryTest(parameterized.TestCase):

  @mock.patch('spanner_orm.api.SpannerApi')
  def test_where(self, spanner_api):
    models.UnittestModel.where_equal(True, int_=3)
    (_, sql, parameters, types), _ = spanner_api.sql_query.call_args

    expected_sql = 'SELECT .* FROM table WHERE table.int_ = @int_0'
    self.assertRegex(sql, expected_sql)
    self.assertEqual(parameters, {'int_0': 3})
    self.assertEqual(types, {'int_0': field.Integer.grpc_type()})

  @mock.patch('spanner_orm.api.SpannerApi')
  def test_count(self, spanner_api):
    column, value = 'int_', 3
    models.UnittestModel.count_equal(True, int_=3)
    (_, sql, parameters, types), _ = spanner_api.sql_query.call_args

    column_key = '{}0'.format(column)
    expected_sql = r'SELECT COUNT\(\*\) FROM table WHERE table.{} = @{}'.format(
        column, column_key)
    self.assertRegex(sql, expected_sql)
    self.assertEqual({column_key: value}, parameters)
    self.assertEqual(types, {column_key: field.Integer.grpc_type()})

  def select(self, *conditions):
    return query.SelectQuery(models.UnittestModel, list(conditions))

  def test_query_limit(self):
    key, value = 'limit0', 2
    select_query = self.select(condition.limit(value))

    self.assertEndsWith(select_query.sql(), ' LIMIT @{}'.format(key))
    self.assertEqual(select_query.parameters(), {key: value})
    self.assertEqual(select_query.types(), {key: field.Integer.grpc_type()})

    select_query = self.select()
    self.assertNotRegex(select_query.sql(), 'LIMIT')

  def test_query_limit_offset(self):
    limit_key, limit = 'limit0', 2
    offset_key, offset = 'offset0', 5
    select_query = self.select(condition.limit(limit, offset=offset))

    self.assertEndsWith(select_query.sql(), ' LIMIT @{} OFFSET @{}'.format(
        limit_key, offset_key))
    self.assertEqual(select_query.parameters(), {
        limit_key: limit,
        offset_key: offset
    })
    self.assertEqual(select_query.types(), {
        limit_key: field.Integer.grpc_type(),
        offset_key: field.Integer.grpc_type()
    })

  def test_query_order_by(self):
    order = ('int_', condition.OrderType.DESC)
    select_query = self.select(condition.order_by(order))

    self.assertEndsWith(select_query.sql(), ' ORDER BY table.int_ DESC')
    self.assertEmpty(select_query.parameters())
    self.assertEmpty(select_query.types())

    select_query = self.select()
    self.assertNotRegex(select_query.sql(), 'ORDER BY')

  def test_query_order_by_with_object(self):
    order = (models.UnittestModel.int_, condition.OrderType.DESC)
    select_query = self.select(condition.order_by(order))

    self.assertEndsWith(select_query.sql(), ' ORDER BY table.int_ DESC')
    self.assertEmpty(select_query.parameters())
    self.assertEmpty(select_query.types())

    select_query = self.select()
    self.assertNotRegex(select_query.sql(), 'ORDER BY')

  @parameterized.parameters(('int_', 5, field.Integer.grpc_type()),
                            ('string', 'foo', field.String.grpc_type()),
                            ('timestamp', now(), field.Timestamp.grpc_type()))
  def test_query_where_comparison(self, column, value, grpc_type):
    condition_generators = [
        condition.greater_than, condition.not_less_than, condition.less_than,
        condition.not_greater_than, condition.equal_to, condition.not_equal_to
    ]
    for condition_generator in condition_generators:
      current_condition = condition_generator(column, value)
      select_query = self.select(current_condition)

      column_key = '{}0'.format(column)
      expected_where = ' WHERE table.{} {} @{}'.format(
          column, current_condition.operator, column_key)
      self.assertEndsWith(select_query.sql(), expected_where)
      self.assertEqual(select_query.parameters(), {column_key: value})
      self.assertEqual(select_query.types(), {column_key: grpc_type})

  @parameterized.parameters(
      (models.UnittestModel.int_, 5, field.Integer.grpc_type()),
      (models.UnittestModel.string, 'foo', field.String.grpc_type()),
      (models.UnittestModel.timestamp, now(), field.Timestamp.grpc_type()))
  def test_query_where_comparison_with_object(self, column, value, grpc_type):
    condition_generators = [
        condition.greater_than, condition.not_less_than, condition.less_than,
        condition.not_greater_than, condition.equal_to, condition.not_equal_to
    ]
    for condition_generator in condition_generators:
      current_condition = condition_generator(column, value)
      select_query = self.select(current_condition)

      column_key = '{}0'.format(column.name)
      expected_where = ' WHERE table.{} {} @{}'.format(
          column.name, current_condition.operator, column_key)
      self.assertEndsWith(select_query.sql(), expected_where)
      self.assertEqual(select_query.parameters(), {column_key: value})
      self.assertEqual(select_query.types(), {column_key: grpc_type})

  @parameterized.parameters(
      ('int_', [1, 2, 3], field.Integer.grpc_type()),
      ('string', ['a', 'b', 'c'], field.String.grpc_type()),
      ('timestamp', [now()], field.Timestamp.grpc_type()))
  def test_query_where_list_comparison(self, column, values, grpc_type):
    condition_generators = [condition.in_list, condition.not_in_list]
    for condition_generator in condition_generators:
      current_condition = condition_generator(column, values)
      select_query = self.select(current_condition)

      column_key = '{}0'.format(column)
      expected_sql = ' WHERE table.{} {} UNNEST(@{})'.format(
          column, current_condition.operator, column_key)
      list_type = type_pb2.Type(
          code=type_pb2.ARRAY, array_element_type=grpc_type)
      self.assertEndsWith(select_query.sql(), expected_sql)
      self.assertEqual(select_query.parameters(), {column_key: values})
      self.assertEqual(select_query.types(), {column_key: list_type})

  def test_query_combines_properly(self):
    select_query = self.select(
        condition.equal_to('int_', 5),
        condition.not_equal_to('string_array', ['foo', 'bar']),
        condition.limit(2),
        condition.order_by(('string', condition.OrderType.DESC)))
    expected_sql = ('WHERE table.int_ = @int_0 AND table.string_array != '
                    '@string_array1 ORDER BY table.string DESC LIMIT @limit2')
    self.assertEndsWith(select_query.sql(), expected_sql)

  def test_only_one_limit_allowed(self):
    with self.assertRaises(error.SpannerError):
      self.select(condition.limit(2), condition.limit(2))

  def test_force_index(self):
    select_query = self.select(condition.force_index('test_index'))
    expected_sql = 'FROM table@{FORCE_INDEX=test_index}'
    self.assertEndsWith(select_query.sql(), expected_sql)

  def test_force_index_with_object(self):
    select_query = self.select(
        condition.force_index(models.UnittestModel.test_index))
    expected_sql = 'FROM table@{FORCE_INDEX=test_index}'
    self.assertEndsWith(select_query.sql(), expected_sql)

  def includes(self, relation, *conditions):
    include_condition = condition.includes(relation, list(conditions))
    return query.SelectQuery(models.RelationshipTestModel, [include_condition])

  def test_includes(self):
    select_query = self.includes('parent')

    # The column order varies between test runs
    expected_sql = (
        r'SELECT RelationshipTestModel\S* RelationshipTestModel\S* '
        r'ARRAY\(SELECT AS STRUCT SmallTestModel\S* SmallTestModel\S* '
        r'SmallTestModel\S* FROM SmallTestModel WHERE SmallTestModel.key = '
        r'RelationshipTestModel.parent_key\)')
    self.assertRegex(select_query.sql(), expected_sql)
    self.assertEmpty(select_query.parameters())
    self.assertEmpty(select_query.types())

  def test_includes_with_object(self):
    select_query = self.includes(models.ChildTestModel.parent)

    # The column order varies between test runs
    expected_sql = (
        r'SELECT ChildTestModel\S* ChildTestModel\S* ARRAY\(SELECT AS '
        r'STRUCT SmallTestModel\S* SmallTestModel\S* SmallTestModel\S* FROM '
        r'SmallTestModel WHERE SmallTestModel.key = '
        r'ChildTestModel.parent_key\)')
    self.assertRegex(select_query.sql(), expected_sql)
    self.assertEmpty(select_query.parameters())
    self.assertEmpty(select_query.types())

  def test_includes_subconditions_query(self):
    select_query = self.includes('parents', condition.equal_to('key', 'value'))
    expected_sql = (
        'WHERE SmallTestModel.key = RelationshipTestModel.parent_key '
        'AND SmallTestModel.key = @key0')
    self.assertRegex(select_query.sql(), expected_sql)

  def includes_result(self, related=1):
    child = {'parent_key': 'parent_key', 'child_key': 'child'}
    result = [child[name] for name in models.RelationshipTestModel.columns]
    parent = {'key': 'key', 'value_1': 'value_1', 'value_2': None}
    parents = []
    for _ in range(related):
      parents.append([parent[name] for name in models.SmallTestModel.columns])
    result.append(parents)
    return child, parent, [result]

  def test_includes_single_related_object_result(self):
    select_query = self.includes('parent')
    child_values, parent_values, rows = self.includes_result(related=1)
    result = select_query.process_results(rows)[0]

    self.assertIsInstance(result.parent, models.SmallTestModel)
    for name, value in child_values.items():
      self.assertEqual(getattr(result, name), value)

    for name, value in parent_values.items():
      self.assertEqual(getattr(result.parent, name), value)

  def test_includes_single_no_related_object_result(self):
    select_query = self.includes('parent')
    child_values, _, rows = self.includes_result(related=0)
    result = select_query.process_results(rows)[0]

    self.assertIsNone(result.parent)
    for name, value in child_values.items():
      self.assertEqual(getattr(result, name), value)

  def test_includes_subcondition_result(self):
    select_query = self.includes('parents', condition.equal_to('key', 'value'))

    child_values, parent_values, rows = self.includes_result(related=2)
    result = select_query.process_results(rows)[0]

    self.assertLen(result.parents, 2)
    for name, value in child_values.items():
      self.assertEqual(getattr(result, name), value)

    for name, value in parent_values.items():
      self.assertEqual(getattr(result.parents[0], name), value)

  def test_includes_error_on_multiple_results_for_single(self):
    select_query = self.includes('parent')
    _, _, rows = self.includes_result(related=2)
    with self.assertRaises(error.SpannerError):
      _ = select_query.process_results(rows)

  def test_includes_error_on_invalid_relation(self):
    with self.assertRaises(AssertionError):
      self.includes('bad_relation')

  @parameterized.parameters(('bad_column', 0), ('child_key', 'good value'),
                            ('key', ['bad value']))
  def test_includes_error_on_invalid_subconditions(self, column, value):
    with self.assertRaises(AssertionError):
      self.includes('parent', condition.equal_to(column, value))

  def test_or(self):
    condition_1 = condition.equal_to('int_', 1)
    condition_2 = condition.equal_to('int_', 2)
    select_query = self.select(condition.or_([condition_1], [condition_2]))

    expected_sql = '((table.int_ = @int_0) OR (table.int_ = @int_1))'
    self.assertEndsWith(select_query.sql(), expected_sql)
    self.assertEqual(select_query.parameters(), {'int_0': 1, 'int_1': 2})
    self.assertEqual(select_query.types(), {
        'int_0': field.Integer.grpc_type(),
        'int_1': field.Integer.grpc_type()
    })


if __name__ == '__main__':
  logging.basicConfig()
  unittest.main()
