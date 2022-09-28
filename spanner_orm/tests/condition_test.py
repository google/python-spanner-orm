# Copyright 2020 Google LLC
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
"""Tests for spanner_orm.condition."""

import datetime
import decimal
import logging
import os
import unittest

from absl.testing import parameterized
from google.api_core import datetime_helpers
from google.cloud import spanner
from google.cloud import spanner_v1

import spanner_orm
from spanner_orm import condition
from spanner_orm import error
from spanner_orm.testlib.spanner_emulator import testlib as spanner_emulator_testlib
from spanner_orm.tests import models


class ConditionTest(
    spanner_emulator_testlib.TestCase,
    parameterized.TestCase,
):

  def setUp(self):
    super().setUp()
    self.run_orm_migrations(
        os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'migrations_for_emulator_test',
        ))

  @parameterized.parameters(
      (True, spanner_v1.param_types.BOOL),
      (0, spanner_v1.param_types.INT64),
      (0.0, spanner_v1.param_types.FLOAT64),
      (
          datetime_helpers.DatetimeWithNanoseconds(2021, 1, 5),
          spanner_v1.param_types.TIMESTAMP,
      ),
      (datetime.datetime(2021, 1, 5), spanner_v1.param_types.TIMESTAMP),
      (datetime.date(2021, 1, 5), spanner_v1.param_types.DATE),
      (b'\x01', spanner_v1.param_types.BYTES),
      ('foo', spanner_v1.param_types.STRING),
      (decimal.Decimal('1.23'), spanner_v1.param_types.NUMERIC),
      (
          (0, 1),
          spanner_v1.Type(
              code=spanner_v1.TypeCode.ARRAY,
              array_element_type=spanner_v1.param_types.INT64,
          ),
      ),
      (
          ['a', None, 'b'],
          spanner_v1.Type(
              code=spanner_v1.TypeCode.ARRAY,
              array_element_type=spanner_v1.param_types.STRING,
          ),
      ),
  )
  def test_param_from_value(self, value, expected_type):
    param = condition.Param.from_value(value)
    self.assertEqual(expected_type, param.type)
    # Test that the value and inferred type are compatible. This will raise an
    # exception if they're not.
    self.assertEmpty(
        models.SmallTestModel.where(
            condition.ArbitraryCondition(
                '$param IS NULL',
                dict(param=param),
                segment=condition.Segment.WHERE,
            )))

  @parameterized.parameters(
      (None, 'Cannot infer type of None'),
      ((0, 'some-string'), 'elements of exactly one type'),
      ((0, 'some-string', None), 'elements of exactly one type'),
      (object(), 'Unknown type'),
  )
  def test_param_from_value_error(self, value, error_regex):
    with self.assertRaisesRegex(TypeError, error_regex):
      condition.Param.from_value(value)

  @parameterized.named_parameters(
      (
          'bytes',
          condition.ArbitraryCondition(
              '$param = b"\x01\x02"',
              dict(param=condition.Param.from_value(b'\x01\x02')),
              segment=condition.Segment.WHERE,
          ),
      ),
      (
          'array_of_bytes',
          condition.ArbitraryCondition(
              '${param}[OFFSET(0)] = b"\x01\x02"',
              dict(param=condition.Param.from_value([b'\x01\x02'])),
              segment=condition.Segment.WHERE,
          ),
      ),
      (
          'array_of_bytes_and_null',
          condition.ArbitraryCondition(
              '${param}[OFFSET(0)] IS NULL',
              dict(param=condition.Param.from_value((None, b'\x01\x02'))),
              segment=condition.Segment.WHERE,
          ),
      ),
  )
  def test_param_from_value_correctly_encodes(self, tautology):
    test_model = models.SmallTestModel(
        dict(
            key='some-key',
            value_1='some-value',
            value_2='other-value',
        ))
    test_model.save()
    self.assertCountEqual((test_model,), models.SmallTestModel.where(tautology))

  @parameterized.named_parameters(
      (
          'minimal',
          condition.ArbitraryCondition(
              'FALSE',
              segment=condition.Segment.WHERE,
          ),
          {},
          {},
          'FALSE',
          (),
      ),
      (
          'full',
          condition.ArbitraryCondition(
              '$key = IF($true_param, ${key_param}, $value_1)',
              dict(
                  key=models.SmallTestModel.key,
                  true_param=condition.Param.from_value(True),
                  key_param=condition.Param.from_value('some-key'),
                  value_1=condition.Column('value_1'),
              ),
              segment=condition.Segment.WHERE,
          ),
          dict(
              true_param0=True,
              key_param0='some-key',
          ),
          dict(
              true_param0=spanner_v1.param_types.BOOL,
              key_param0=spanner_v1.param_types.STRING,
          ),
          ('SmallTestModel.key = '
           'IF(@true_param0, @key_param0, SmallTestModel.value_1)'),
          ('some-key',),
      ),
  )
  def test_arbitrary_condition(
      self,
      condition_,
      expected_params,
      expected_types,
      expected_sql,
      expected_row_keys,
  ):
    models.SmallTestModel(
        dict(
            key='some-key',
            value_1='some-value',
            value_2='other-value',
        )).save()
    rows = models.SmallTestModel.where(condition_)
    self.assertEqual(expected_params, condition_.params())
    self.assertEqual(expected_types, condition_.types())
    self.assertEqual(expected_sql, condition_.sql())
    self.assertCountEqual(expected_row_keys, tuple(row.key for row in rows))

  @parameterized.named_parameters(
      ('key_not_found', '$not_found', KeyError, 'not_found'),
      ('invalid_template', '$', ValueError, 'Invalid placeholder'),
  )
  def test_arbitrary_condition_template_error(
      self,
      template,
      error_class,
      error_regex,
  ):
    with self.assertRaisesRegex(error_class, error_regex):
      condition.ArbitraryCondition(template, segment=condition.Segment.WHERE)

  @parameterized.named_parameters(
      (
          'field_from_wrong_model',
          models.ChildTestModel.key,
          'does not belong to the Model',
      ),
      (
          'column_not_found',
          condition.Column('not_found'),
          'does not exist in the Model',
      ),
  )
  def test_arbitrary_condition_validation_error(
      self,
      substitution,
      error_regex,
  ):
    condition_ = condition.ArbitraryCondition(
        '$substitution',
        dict(substitution=substitution),
        segment=condition.Segment.WHERE,
    )
    with self.assertRaisesRegex(error.ValidationError, error_regex):
      models.SmallTestModel.where(condition_)

  @parameterized.named_parameters(
      (
          'empty_or',
          condition.OrCondition(),
          {},
          {},
          'FALSE',
          '',
      ),
      (
          'empty_and',
          condition.OrCondition([]),
          {},
          {},
          '(TRUE)',
          'ab',
      ),
      (
          'single',
          condition.OrCondition(
              [condition.equal_to(models.SmallTestModel.key, 'a')]),
          dict(key0='a'),
          dict(key0=spanner_v1.param_types.STRING),
          '((SmallTestModel.key = @key0))',
          'a',
      ),
      (
          'multiple',
          condition.OrCondition(
              [
                  condition.equal_to(models.SmallTestModel.key, 'a'),
                  condition.equal_to(models.SmallTestModel.value_1, 'a'),
              ],
              [
                  condition.equal_to(models.SmallTestModel.key, 'b'),
                  condition.equal_to(models.SmallTestModel.value_1, 'b'),
              ],
          ),
          dict(
              key0='a',
              value_11='a',
              key2='b',
              value_13='b',
          ),
          dict(
              key0=spanner_v1.param_types.STRING,
              value_11=spanner_v1.param_types.STRING,
              key2=spanner_v1.param_types.STRING,
              value_13=spanner_v1.param_types.STRING,
          ),
          ('('
           '(SmallTestModel.key = @key0 AND SmallTestModel.value_1 = @value_11)'
           ' OR '
           '(SmallTestModel.key = @key2 AND SmallTestModel.value_1 = @value_13)'
           ')'),
          'ab',
      ),
  )
  def test_or_condition(
      self,
      condition_,
      expected_params,
      expected_types,
      expected_sql,
      expected_row_keys,
  ):
    models.SmallTestModel(dict(key='a', value_1='a', value_2='a')).save()
    models.SmallTestModel(dict(key='b', value_1='b', value_2='b')).save()
    rows = models.SmallTestModel.where(condition_)
    self.assertEqual(expected_params, condition_.params())
    self.assertEqual(expected_types, condition_.types())
    self.assertEqual(expected_sql, condition_.sql())
    self.assertCountEqual(expected_row_keys, tuple(row.key for row in rows))

  @parameterized.parameters(
      ('ABCD', 'BC', True),
      ('ABCD', 'bc', False),
      ('ABCD', 'CB', False),
      (b'ABCD', b'BC', True),
      (b'ABCD', b'bc', False),
      (b'ABCD', b'CB', False),
      ('ABCD', 'BC', True, dict(case_sensitive=False)),
      ('ABCD', 'bc', True, dict(case_sensitive=False)),
      ('ABCD', 'CB', False, dict(case_sensitive=False)),
      (b'ABCD', b'BC', True, dict(case_sensitive=False)),
      (b'ABCD', b'bc', True, dict(case_sensitive=False)),
      (b'ABCD', b'CB', False, dict(case_sensitive=False)),
  )
  def test_contains(
      self,
      haystack,
      needle,
      expect_results,
      kwargs={},
  ):
    test_model = models.SmallTestModel(dict(key='a', value_1='a', value_2='a'))
    test_model.save()
    self.assertCountEqual(
        ((test_model,) if expect_results else ()),
        models.SmallTestModel.where(
            spanner_orm.contains(
                condition.Param.from_value(haystack),
                condition.Param.from_value(needle),
                **kwargs,
            )),
    )

  def test_force_null_filtered_index(self):
    non_null_model = models.NullFilteredIndexModel(
        dict(key='a', value_1='a', value_2=1))
    non_null_model.save()
    models.NullFilteredIndexModel(dict(key='b', value_1=None, value_2=2)).save()
    self.assertCountEqual((non_null_model,),
                          models.NullFilteredIndexModel.where(
                              *spanner_orm.force_null_filtered_index(
                                  models.NullFilteredIndexModel.value_index)))


if __name__ == '__main__':
  logging.basicConfig()
  unittest.main()
