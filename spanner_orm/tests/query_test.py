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
import unittest
from unittest import mock

from spanner_orm import condition
from spanner_orm import error
from spanner_orm import field
from spanner_orm import query
from spanner_orm.tests import models


def now():
  return datetime.datetime.now(tz=datetime.timezone.utc)


class SqlBodyTest(unittest.TestCase):

  @mock.patch('spanner_orm.api.SpannerApi')
  def test_where(self, mock_db):
    models.UnittestModel.where_equal(True, int_=3)
    ((_, sql, parameters, types), _) = mock_db.sql_query.call_args
    expected_sql = 'SELECT .* FROM table WHERE table.int_ = @int_'
    self.assertRegex(sql, expected_sql)
    self.assertEqual({'int_': 3}, parameters)
    self.assertEqual(types, {'int_': field.Integer.grpc_type()})

  @mock.patch('spanner_orm.api.SpannerApi')
  def test_count(self, mock_db):
    column, value = ('int_', 3)
    models.UnittestModel.count_equal(True, **{column: value})
    ((_, sql, parameters, types), _) = mock_db.sql_query.call_args
    expected_sql = r'SELECT COUNT\(\*\) FROM table WHERE table.{} = @{}'.format(
        column, column)
    self.assertRegex(sql, expected_sql)
    self.assertEqual({column: value}, parameters)
    self.assertEqual(types, {column: field.Integer.grpc_type()})

  def test_query__limit(self):
    key, value = (condition.LimitCondition.KEY, 2)
    query_ = query.SelectQuery(models.UnittestModel, [condition.limit(value)])

    sql, params, types = query_._limit()
    self.assertEqual(sql, ' LIMIT @limit')
    self.assertEqual(params, {key: value})
    self.assertEqual(types, {key: field.Integer.grpc_type()})

    query_ = query.SelectQuery(models.UnittestModel, [])
    self.assertEqual(query_._limit(), ('', {}, {}))

  def test_query__order_by(self):
    order = ('int_', condition.OrderType.DESC)
    query_ = query.SelectQuery(models.UnittestModel,
                               [condition.order_by(order)])

    sql, params, types = query_._order()
    self.assertEqual(sql, ' ORDER BY table.int_ DESC')
    self.assertEqual(params, {})
    self.assertEqual(types, {})

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
        expected_where = ' WHERE table.{} {} @{}'.format(
            column, current_condition.operator(), column)
        sql, params, types = query_._where()
        self.assertEqual(sql, expected_where)
        self.assertEqual(params, {column: value})
        self.assertEqual(types, {column: type_})

  def test_query__where_list_comparison(self):
    tuples = [('int_', [1, 2, 3], field.Integer.grpc_list_type()),
              ('string', ['a', 'b', 'c'], field.String.grpc_list_type()),
              ('timestamp', [now()], field.Timestamp.grpc_list_type())]

    conditions = [condition.in_list, condition.not_in_list]
    for column, values, type_ in tuples:
      for condition_generator in conditions:
        current_condition = condition_generator(column, values)
        query_ = query.SelectQuery(models.UnittestModel, [current_condition])
        expected_sql = ' WHERE table.{} {} UNNEST(@{})'.format(
            column, current_condition.operator(), column)
        sql, params, types = query_._where()
        self.assertEqual(sql, expected_sql)
        self.assertEqual(params, {column: values})
        self.assertEqual(types, {column: type_})

  def test_query__combines_properly(self):
    query_ = query.SelectQuery(models.UnittestModel, [
        condition.equal_to('int_', 5),
        condition.not_equal_to('string_array', ['foo', 'bar']),
        condition.limit(2),
        condition.order_by(('string', condition.OrderType.DESC))
    ])
    expected_sql = ('WHERE table.int_ = @int_ AND table.string_array != '
                    '@string_array ORDER BY table.string DESC LIMIT @limit')
    self.assertTrue(query_.sql().endswith(expected_sql))

  def test_only_one_limit_allowed(self):
    with self.assertRaises(error.SpannerError):
      query.SelectQuery(
          models.UnittestModel,
          [condition.limit(2), condition.limit(2)])

  def test_only_one_condition_per_column_allowed(self):
    with self.assertRaises(error.SpannerError):
      query.SelectQuery(
          models.UnittestModel,
          [condition.equal_to('int_', 5),
           condition.equal_to('int_', 2)])

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

  def test_includes_subconditions(self):
    query_ = query.SelectQuery(
        models.ChildTestModel,
        [condition.includes('parent', [condition.equal_to('key', 'value')])])
    # The column order varies between test runs
    expected_sql = ('WHERE SmallTestModel.key = ChildTestModel.parent_key '
                    'AND SmallTestModel.key = @key')
    (sql, _, _) = query_._select()
    self.assertRegex(sql, expected_sql)

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
  unittest.main()
