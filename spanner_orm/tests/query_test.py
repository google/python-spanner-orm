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

from google.cloud import spanner_v1


def now():
  return datetime.datetime.now(tz=datetime.timezone.utc)


class QueryTest(parameterized.TestCase):

  @mock.patch('spanner_orm.table_apis.sql_query')
  def test_where(self, sql_query):
    sql_query.return_value = []

    models.UnittestModel.where_equal(int_=3, transaction=True)
    (_, sql, parameters, types), _ = sql_query.call_args

    expected_sql = 'SELECT .* FROM table WHERE table.int_ = @int_0'
    self.assertRegex(sql, expected_sql)
    self.assertEqual(parameters, {'int_0': 3})
    self.assertEqual(types, {'int_0': field.Integer().grpc_type()})

  @mock.patch('spanner_orm.table_apis.sql_query')
  def test_count(self, sql_query):
    sql_query.return_value = [[0]]
    column, value = 'int_', 3
    models.UnittestModel.count_equal(int_=3, transaction=True)
    (_, sql, parameters, types), _ = sql_query.call_args

    column_key = '{}0'.format(column)
    expected_sql = r'SELECT COUNT\(\*\) FROM table WHERE table.{} = @{}'.format(
        column, column_key)
    self.assertRegex(sql, expected_sql)
    self.assertEqual({column_key: value}, parameters)
    self.assertEqual(types, {column_key: field.Integer().grpc_type()})

  def test_count_allows_force_index(self):
    force_index = condition.force_index('test_index')
    count_query = query.CountQuery(models.UnittestModel, [force_index])
    sql = count_query.sql()
    expected_sql = 'SELECT COUNT(*) FROM table@{FORCE_INDEX=test_index}'
    self.assertEqual(expected_sql, sql)

  @parameterized.parameters(
      condition.limit(1), condition.order_by(
          ('int_', condition.OrderType.DESC)))
  def test_count_only_allows_where_and_from_segment_conditions(self, condition):
    with self.assertRaises(error.SpannerError):
      query.CountQuery(models.UnittestModel, [condition])

  def select(self, *conditions):
    return query.SelectQuery(models.UnittestModel, list(conditions))

  def test_query_limit(self):
    key, value = 'limit0', 2
    select_query = self.select(condition.limit(value))

    self.assertEndsWith(select_query.sql(), ' LIMIT @{}'.format(key))
    self.assertEqual(select_query.parameters(), {key: value})
    self.assertEqual(select_query.types(), {key: field.Integer().grpc_type()})

    select_query = self.select()
    self.assertNotRegex(select_query.sql(), 'LIMIT')

  def test_query_limit_offset(self):
    limit_key, limit = 'limit0', 2
    offset_key, offset = 'offset0', 5
    select_query = self.select(condition.limit(limit, offset=offset))

    self.assertEndsWith(select_query.sql(),
                        ' LIMIT @{} OFFSET @{}'.format(limit_key, offset_key))
    self.assertEqual(select_query.parameters(), {
        limit_key: limit,
        offset_key: offset
    })
    self.assertEqual(
        select_query.types(), {
            limit_key: field.Integer().grpc_type(),
            offset_key: field.Integer().grpc_type()
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

  @parameterized.parameters(('int_', 5, field.Integer().grpc_type()),
                            ('string', 'foo', field.String().grpc_type()),
                            ('timestamp', now(), field.Timestamp().grpc_type()))
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
      (models.UnittestModel.int_, 5, field.Integer().grpc_type()),
      (models.UnittestModel.string, 'foo', field.String().grpc_type()),
      (models.UnittestModel.timestamp, now(), field.Timestamp().grpc_type()))
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
      ('int_', [1, 2, 3], field.Integer().grpc_type()),
      ('int_', (4, 5, 6), field.Integer().grpc_type()),
      ('string', ['a', 'b', 'c'], field.String().grpc_type()),
      ('timestamp', [now()], field.Timestamp().grpc_type()))
  def test_query_where_list_comparison(self, column, values, grpc_type):
    condition_generators = [condition.in_list, condition.not_in_list]
    for condition_generator in condition_generators:
      current_condition = condition_generator(column, values)
      select_query = self.select(current_condition)

      column_key = '{}0'.format(column)
      expected_sql = ' WHERE table.{} {} UNNEST(@{})'.format(
          column, current_condition.operator, column_key)
      list_type = spanner_v1.Type(
          code=spanner_v1.TypeCode.ARRAY, array_element_type=grpc_type)
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

  def includes(self, relation, *conditions, foreign_key_relation=False):
    include_condition = condition.includes(relation, list(conditions),
                                           foreign_key_relation)
    return query.SelectQuery(
        models.ForeignKeyTestModel
        if foreign_key_relation else models.RelationshipTestModel,
        [include_condition],
    )

  @parameterized.parameters((models.RelationshipTestModel.parent, True),
                            (models.ForeignKeyTestModel.foreign_key_1, False))
  def test_bad_includes_args(self, relation_key, foreign_key_relation):
    with self.assertRaisesRegex(ValueError, 'Must pass'):
      self.includes(
          relation_key,
          foreign_key_relation=foreign_key_relation,
      )

  @parameterized.named_parameters(
      (
          'legacy_relationship',
          {
              'relation': 'parent'
          },
          r'SELECT RelationshipTestModel\S* RelationshipTestModel\S* '
          r'ARRAY\(SELECT AS STRUCT SmallTestModel\S* SmallTestModel\S* '
          r'SmallTestModel\S* FROM SmallTestModel WHERE SmallTestModel.key = '
          r'RelationshipTestModel.parent_key\)',
      ),
      (
          'legacy_relationship_with_object_arg',
          {
              'relation': models.RelationshipTestModel.parent
          },
          r'SELECT RelationshipTestModel\S* RelationshipTestModel\S* '
          r'ARRAY\(SELECT AS STRUCT SmallTestModel\S* SmallTestModel\S* '
          r'SmallTestModel\S* FROM SmallTestModel WHERE SmallTestModel.key = '
          r'RelationshipTestModel.parent_key\)',
      ),
      (
          'foreign_key_relationship',
          {
              'relation': 'foreign_key_1',
              'foreign_key_relation': True
          },
          r'SELECT ForeignKeyTestModel\S* ForeignKeyTestModel\S* ForeignKeyTestModel\S* ForeignKeyTestModel\S* '
          r'ARRAY\(SELECT AS STRUCT SmallTestModel\S* SmallTestModel\S* '
          r'SmallTestModel\S* FROM SmallTestModel WHERE SmallTestModel.key = '
          r'ForeignKeyTestModel.referencing_key_1\)',
      ),
      (
          'foreign_key_relationship_with_object_arg',
          {
              'relation': models.ForeignKeyTestModel.foreign_key_1,
              'foreign_key_relation': True
          },
          r'SELECT ForeignKeyTestModel\S* ForeignKeyTestModel\S* ForeignKeyTestModel\S* ForeignKeyTestModel\S* '
          r'ARRAY\(SELECT AS STRUCT SmallTestModel\S* SmallTestModel\S* '
          r'SmallTestModel\S* FROM SmallTestModel WHERE SmallTestModel.key = '
          r'ForeignKeyTestModel.referencing_key_1\)',
      ),
  )
  def test_includes(self, includes_kwargs, expected_sql):
    select_query = self.includes(**includes_kwargs)

    # The column order varies between test runs
    self.assertRegex(select_query.sql(), expected_sql)
    self.assertEmpty(select_query.parameters())
    self.assertEmpty(select_query.types())

  @parameterized.parameters(({
      'relation': models.RelationshipTestModel.parent,
      'foreign_key_relation': True
  },), ({
      'relation': models.ForeignKeyTestModel.foreign_key_1,
      'foreign_key_relation': False
  },))
  def test_error_mismatched_params(self, includes_kwargs):
    with self.assertRaisesRegex(ValueError, 'Must pass'):
      self.includes(**includes_kwargs)

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

  def fk_includes_result(self, related=1):
    child = {
        'referencing_key_1': 'parent_key',
        'referencing_key_2': 'child',
        'referencing_key_3': 'child',
        'self_referencing_key': 'child'
    }
    result = [child[name] for name in models.ForeignKeyTestModel.columns]
    parent = {'key': 'key', 'value_1': 'value_1', 'value_2': None}
    parents = []
    for _ in range(related):
      parents.append([parent[name] for name in models.SmallTestModel.columns])
    result.append(parents)
    return child, parent, [result]

  @parameterized.named_parameters(
      (
          'legacy_relationship',
          {
              'relation': 'parent'
          },
          lambda x: x.parent,
          lambda x: x.includes_result(related=1),
      ),
      (
          'foreign_key_relationship',
          {
              'relation': 'foreign_key_1',
              'foreign_key_relation': True
          },
          lambda x: x.foreign_key_1,
          lambda x: x.fk_includes_result(related=1),
      ),
  )
  def test_includes_single_related_object_result(
      self,
      includes_kwargs,
      referenced_table_fn,
      includes_result_fn,
  ):
    select_query = self.includes(**includes_kwargs)
    child_values, parent_values, rows = includes_result_fn(self)
    result = select_query.process_results(rows)[0]

    self.assertIsInstance(
        referenced_table_fn(result),
        models.SmallTestModel,
    )
    for name, value in child_values.items():
      self.assertEqual(getattr(result, name), value)

    for name, value in parent_values.items():
      self.assertEqual(getattr(referenced_table_fn(result), name), value)

  @parameterized.named_parameters(
      (
          'legacy_relationship',
          {
              'relation': 'parent'
          },
          lambda x: x.parent,
          lambda x: x.includes_result(related=0),
      ),
      (
          'foreign_key_relationship',
          {
              'relation': 'foreign_key_1',
              'foreign_key_relation': True
          },
          lambda x: x.foreign_key_1,
          lambda x: x.fk_includes_result(related=0),
      ),
  )
  def test_includes_single_no_related_object_result(self, includes_kwargs,
                                                    referenced_table_fn,
                                                    includes_result_fn):
    select_query = self.includes(**includes_kwargs)
    child_values, _, rows = includes_result_fn(self)
    result = select_query.process_results(rows)[0]

    self.assertIsNone(referenced_table_fn(result))
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

  @parameterized.named_parameters(
      (
          'legacy_relationship',
          {
              'relation': 'parent'
          },
          lambda x: x.includes_result(related=2),
      ),
      (
          'foreign_key_relationship',
          {
              'relation': 'foreign_key_1',
              'foreign_key_relation': True
          },
          lambda x: x.fk_includes_result(related=2),
      ),
  )
  def test_includes_error_on_multiple_results_for_single(
      self, includes_kwargs, includes_result_fn):
    select_query = self.includes(**includes_kwargs)
    _, _, rows = includes_result_fn(self)
    with self.assertRaises(error.SpannerError):
      _ = select_query.process_results(rows)

  @parameterized.parameters(True, False)
  def test_includes_error_on_invalid_relation(self, foreign_key_relation):
    with self.assertRaises(error.ValidationError):
      self.includes('bad_relation', foreign_key_relation=foreign_key_relation)

  @parameterized.parameters(
      ('bad_column', 0, 'parent', False),
      ('bad_column', 0, 'foreign_key_1', True),
      ('child_key', 'good value', 'parent', False),
      ('child_key', 'good value', 'foreign_key_1', False),
      ('key', ['bad value'], 'parent', False),
      ('key', ['bad value'], 'foreign_key_1', False),
  )
  def test_includes_error_on_invalid_subconditions(self, column, value,
                                                   relation,
                                                   foreign_key_relation):
    with self.assertRaises(error.ValidationError):
      self.includes(
          relation,
          condition.equal_to(column, value),
          foreign_key_relation,
      )

  def test_or(self):
    condition_1 = condition.equal_to('int_', 1)
    condition_2 = condition.equal_to('int_', 2)
    select_query = self.select(condition.or_([condition_1], [condition_2]))

    expected_sql = '((table.int_ = @int_0) OR (table.int_ = @int_1))'
    self.assertEndsWith(select_query.sql(), expected_sql)
    self.assertEqual(select_query.parameters(), {'int_0': 1, 'int_1': 2})
    self.assertEqual(select_query.types(), {
        'int_0': field.Integer().grpc_type(),
        'int_1': field.Integer().grpc_type()
    })


if __name__ == '__main__':
  logging.basicConfig()
  unittest.main()
