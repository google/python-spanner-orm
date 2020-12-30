# Lint as: python3
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

import logging
import os
import unittest

from absl.testing import parameterized
from google.cloud.spanner_v1.proto import type_pb2

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
            os.path.dirname(os.path.realpath(__file__)),
            'migrations_for_emulator_test',
        ))

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
                  true_param=condition.Param(
                      True,
                      type=type_pb2.Type(code=type_pb2.BOOL),
                  ),
                  key_param=condition.Param(
                      'some-key',
                      type=type_pb2.Type(code=type_pb2.STRING),
                  ),
                  value_1=condition.Column('value_1'),
              ),
              segment=condition.Segment.WHERE,
          ),
          dict(
              true_param0=True,
              key_param0='some-key',
          ),
          dict(
              true_param0=type_pb2.Type(code=type_pb2.BOOL),
              key_param0=type_pb2.Type(code=type_pb2.STRING),
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

  def test_contains(self):
    contains = spanner_orm.contains('some_column', r'a%b_c\d')
    self.assertEqual('some_column', contains.column)
    self.assertEqual('LIKE', contains.operator)
    self.assertEqual(r'%a\%b\_c\\d%', contains.value)


if __name__ == '__main__':
  logging.basicConfig()
  unittest.main()
