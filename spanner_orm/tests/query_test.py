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
from unittest.mock import patch

from spanner_orm.condition import equal_to
from spanner_orm.condition import greater_than
from spanner_orm.condition import in_list
from spanner_orm.condition import includes
from spanner_orm.condition import less_than
from spanner_orm.condition import limit
from spanner_orm.condition import LimitCondition
from spanner_orm.condition import not_equal_to
from spanner_orm.condition import not_greater_than
from spanner_orm.condition import not_in_list
from spanner_orm.condition import not_less_than
from spanner_orm.condition import order_by
from spanner_orm.condition import OrderType
from spanner_orm.query import SelectQuery
from spanner_orm.tests import models
from spanner_orm.type import Integer
from spanner_orm.type import String
from spanner_orm.type import Timestamp


def now():
  return datetime.datetime.now(tz=datetime.timezone.utc)


class SqlBodyTest(unittest.TestCase):

  @patch('spanner_orm.api.SpannerApi')
  def test_where(self, mock_db):
    models.UnittestModel.where_equal(True, int_=3)
    ((_, sql, parameters, types), _) = mock_db.sql_query.call_args
    expected_sql = 'SELECT .* FROM table WHERE table.int_ = @int_'
    self.assertRegex(sql, expected_sql)
    self.assertEqual({'int_': 3}, parameters)
    self.assertEqual(types, {'int_': Integer.grpc_type()})

  @patch('spanner_orm.api.SpannerApi')
  def test_count(self, mock_db):
    column, value = ('int_', 3)
    models.UnittestModel.count_equal(True, **{column: value})
    ((_, sql, parameters, types), _) = mock_db.sql_query.call_args
    expected_sql = r'SELECT COUNT\(\*\) FROM table WHERE table.{} = @{}'.format(
        column, column)
    self.assertRegex(sql, expected_sql)
    self.assertEqual({column: value}, parameters)
    self.assertEqual(types, {column: Integer.grpc_type()})

  def test_query_limit(self):
    key, value = (LimitCondition.KEY, 2)
    query = SelectQuery(models.UnittestModel, [limit(value)])

    sql, params, types = query._limit()
    self.assertEqual(sql, ' LIMIT @limit')
    self.assertEqual(params, {key: value})
    self.assertEqual(types, {key: Integer.grpc_type()})

    query = SelectQuery(models.UnittestModel, [])
    self.assertEqual(query._limit(), ('', {}, {}))

  def test_query_order_by(self):
    order = ('int_', OrderType.DESC)
    query = SelectQuery(models.UnittestModel, [order_by(order)])

    sql, params, types = query._order()
    self.assertEqual(sql, ' ORDER BY table.int_ DESC')
    self.assertEqual(params, {})
    self.assertEqual(types, {})

    query = SelectQuery(models.UnittestModel, [])
    self.assertEqual(query._order(), ('', {}, {}))

  def test_query_where_comparison(self):
    tuples = [('int_', 5, Integer.grpc_type()),
              ('string', 'foo', String.grpc_type()),
              ('timestamp', now(), Timestamp.grpc_type())]
    conditions = [
        greater_than, not_less_than, less_than, not_greater_than, equal_to,
        not_equal_to
    ]
    for column, value, type_ in tuples:
      for condition_generator in conditions:
        condition = condition_generator(column, value)
        query = SelectQuery(models.UnittestModel, [condition])
        expected_where = ' WHERE table.{} {} @{}'.format(
            column, condition.operator(), column)
        sql, params, types = query._where()
        self.assertEqual(sql, expected_where)
        self.assertEqual(params, {column: value})
        self.assertEqual(types, {column: type_})

  def test_query_where_list_comparison(self):
    tuples = [('int_', [1, 2, 3], Integer.grpc_list_type()),
              ('string', ['a', 'b', 'c'], String.grpc_list_type()),
              ('timestamp', [now()], Timestamp.grpc_list_type())]

    conditions = [in_list, not_in_list]
    for column, values, type_ in tuples:
      for condition_generator in conditions:
        condition = condition_generator(column, values)
        query = SelectQuery(models.UnittestModel, [condition])
        expected_sql = ' WHERE table.{} {} UNNEST(@{})'.format(
            column, condition.operator(), column)
        sql, params, types = query._where()
        self.assertEqual(sql, expected_sql)
        self.assertEqual(params, {column: values})
        self.assertEqual(types, {column: type_})

  def test_query_combines_properly(self):
    query = SelectQuery(models.UnittestModel, [
        equal_to('int_', 5),
        not_equal_to('string_array', ['foo', 'bar']),
        limit(2),
        order_by(('string', OrderType.DESC))
    ])
    expected_sql = ('WHERE table.int_ = @int_ AND table.string_array != '
                    '@string_array ORDER BY table.string DESC LIMIT @limit')
    self.assertTrue(query.sql().endswith(expected_sql))

  def test_only_one_limit_allowed(self):
    with self.assertRaises(AssertionError):
      SelectQuery(models.UnittestModel, [limit(2), limit(2)])

  def test_only_one_condition_per_column_allowed(self):
    with self.assertRaises(AssertionError):
      SelectQuery(
          models.UnittestModel,
          [equal_to('int_', 5), equal_to('int_', 2)])

  def test_includes(self):
    query = SelectQuery(models.ChildTestModel, [includes('parent')])
    # The column order varies between test runs
    expected_sql = (
        r'SELECT ChildTestModel\S* ChildTestModel\S* ARRAY\(SELECT AS '
        r'STRUCT SmallTestModel\S* SmallTestModel\S* SmallTestModel\S* FROM '
        r'SmallTestModel WHERE SmallTestModel.key = '
        r'ChildTestModel.parent_key\)')
    (sql, _, _) = query._select()
    self.assertRegex(sql, expected_sql)

  def test_includes_subconditions(self):
    query = SelectQuery(models.ChildTestModel,
                        [includes('parent', [equal_to('key', 'value')])])
    # The column order varies between test runs
    expected_sql = ('WHERE SmallTestModel.key = ChildTestModel.parent_key '
                    'AND SmallTestModel.key = @key')
    (sql, _, _) = query._select()
    self.assertRegex(sql, expected_sql)

  def test_includes_error_on_invalid_relation(self):
    with self.assertRaises(AssertionError):
      SelectQuery(models.ChildTestModel, [includes('bad_relation')])

  def test_includes_error_on_invalid_subconditions(self):
    with self.assertRaises(AssertionError):
      SelectQuery(models.ChildTestModel,
                  [includes('parent', [equal_to('bad_column', 0)])])

    with self.assertRaises(AssertionError):
      SelectQuery(models.ChildTestModel,
                  [includes('parent', [equal_to('child_key', 0)])])

    with self.assertRaises(AssertionError):
      SelectQuery(models.ChildTestModel,
                  [includes('parent', [equal_to('key', 0)])])


if __name__ == '__main__':
  unittest.main()
