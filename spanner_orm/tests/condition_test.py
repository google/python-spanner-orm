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

from absl.testing import parameterized
from spanner_orm import condition
from spanner_orm.tests import models


class IncludesConditionTest(parameterized.TestCase):

  @parameterized.parameters(True, False)
  def test_equality(self, bound_equality):
    condition_1 = condition.IncludesCondition(
        'parent', [condition.equal_to('key', 'value')])
    condition_2 = condition.IncludesCondition(
        'parent', [condition.equal_to('key', 'value')])
    if bound_equality:
      condition_1.bind(models.ChildTestModel)
      condition_2.bind(models.ChildTestModel)
    self.assertEqual(condition_1, condition_2)

  def test_bind_inequality(self):
    condition_1 = condition.IncludesCondition(
        'parent', [condition.equal_to('key', 'value')])
    condition_2 = condition.IncludesCondition(
        'parent', [condition.equal_to('key', 'value')])
    condition_1.bind(models.ChildTestModel)
    condition_2.bind(models.IdenticalChildTestModel)
    self.assertNotEqual(condition_1, condition_2)

  def test_name_inequality(self):
    condition_1 = condition.IncludesCondition(
        'parent', [condition.equal_to('key', 'value')])
    condition_2 = condition.IncludesCondition(
        'grandparent', [condition.equal_to('key', 'value')])
    self.assertNotEqual(condition_1, condition_2)

  def test_condition_inequality(self):
    condition_1 = condition.IncludesCondition(
        'parent', [condition.equal_to('key', 'other')])
    condition_2 = condition.IncludesCondition(
        'parent', [condition.equal_to('key', 'value')])
    self.assertNotEqual(condition_1, condition_2)

  def test_extra_condition_inequality(self):
    condition_1 = condition.IncludesCondition(
        'parent', [condition.equal_to('key', 'value')])
    condition_2 = condition.IncludesCondition('parent', [
        condition.equal_to('key', 'value'),
        condition.equal_to('key', 'other')
    ])
    self.assertNotEqual(condition_1, condition_2)


class LimitConditionTest(parameterized.TestCase):

  @parameterized.parameters(True, False)
  def test_equality(self, bound_equality):
    condition_1 = condition.LimitCondition(100, 50)
    condition_2 = condition.LimitCondition(100, 50)
    if bound_equality:
      condition_1.bind(models.SmallTestModel)
      condition_2.bind(models.SmallTestModel)
    self.assertEqual(condition_1, condition_2)

  def test_bind_inequality(self):
    condition_1 = condition.LimitCondition(100, 50)
    condition_2 = condition.LimitCondition(100, 50)
    condition_1.bind(models.SmallTestModel)
    condition_2.bind(models.IdenticalSmallTestModel)
    self.assertNotEqual(condition_1, condition_2)

  def test_value_inequality(self):
    condition_1 = condition.LimitCondition(100, 50)
    condition_2 = condition.LimitCondition(99, 50)
    self.assertNotEqual(condition_1, condition_2)

  def test_offset_inequality(self):
    condition_1 = condition.LimitCondition(100, 50)
    condition_2 = condition.LimitCondition(100, 51)
    self.assertNotEqual(condition_1, condition_2)


class OrConditionTest(parameterized.TestCase):

  def condition_a(self):
    return condition.LimitCondition(100, 50)

  def condition_b(self):
    return condition.LimitCondition(99, 50)

  def condition_c(self):
    return condition.ComparisonCondition('op', 'value_1', 'value')

  def condition_d(self):
    return condition.ComparisonCondition('oops', 'value_1', 'value')

  @parameterized.parameters(True, False)
  def test_equality(self, bound_equality):
    condition_1 = condition.OrCondition(
        [self.condition_a(), self.condition_b()],
        [self.condition_c(), self.condition_d()])
    condition_2 = condition.OrCondition(
        [self.condition_a(), self.condition_b()],
        [self.condition_c(), self.condition_d()])
    if bound_equality:
      condition_1.bind(models.SmallTestModel)
      condition_2.bind(models.SmallTestModel)
    self.assertEqual(condition_1, condition_2)

  def test_bind_inequality(self):
    condition_1 = condition.OrCondition(
        [self.condition_a(), self.condition_b()],
        [self.condition_c(), self.condition_d()])
    condition_2 = condition.OrCondition(
        [self.condition_a(), self.condition_b()],
        [self.condition_c(), self.condition_d()])
    condition_1.bind(models.SmallTestModel)
    condition_2.bind(models.IdenticalSmallTestModel)
    self.assertNotEqual(condition_1, condition_2)

  def test_condition_order_inequality(self):
    condition_1 = condition.OrCondition(
        [self.condition_a(), self.condition_b()],
        [self.condition_c(), self.condition_d()])
    condition_2 = condition.OrCondition(
        [self.condition_b(), self.condition_a()],
        [self.condition_c(), self.condition_d()])
    self.assertNotEqual(condition_1, condition_2)

  def test_list_order_inequality(self):
    condition_1 = condition.OrCondition(
        [self.condition_a(), self.condition_b()],
        [self.condition_c(), self.condition_d()])
    condition_2 = condition.OrCondition(
        [self.condition_c(), self.condition_d()],
        [self.condition_a(), self.condition_b()])
    self.assertNotEqual(condition_1, condition_2)

  def test_extra_condition_inequality(self):
    condition_1 = condition.OrCondition(
        [self.condition_a(), self.condition_b()],
        [self.condition_c(), self.condition_d()])
    condition_2 = condition.OrCondition(
        [self.condition_a(), self.condition_b()], [self.condition_c()])
    self.assertNotEqual(condition_1, condition_2)

  def test_extra_list_inequality(self):
    condition_1 = condition.OrCondition([self.condition_a()],
                                        [self.condition_b()])
    condition_2 = condition.OrCondition(
        [self.condition_a()], [self.condition_b()], [self.condition_c()])
    self.assertNotEqual(condition_1, condition_2)


class ColumnsEqualConditionTest(parameterized.TestCase):

  @parameterized.parameters(True, False)
  def test_equality(self, bound_equality):
    condition_1 = condition.ColumnsEqualCondition(
        'value_1', models.IdenticalSmallTestModel, 'value_1')
    condition_2 = condition.ColumnsEqualCondition(
        'value_1', models.IdenticalSmallTestModel, 'value_1')
    if bound_equality:
      condition_1.bind(models.SmallTestModel)
      condition_2.bind(models.SmallTestModel)
    self.assertEqual(condition_1, condition_2)

  def test_bind_inequality(self):
    condition_1 = condition.ColumnsEqualCondition(
        'value_1', models.UnittestModel, 'string')
    condition_2 = condition.ColumnsEqualCondition(
        'value_1', models.UnittestModel, 'string')
    condition_1.bind(models.SmallTestModel)
    condition_2.bind(models.IdenticalSmallTestModel)
    self.assertNotEqual(condition_1, condition_2)

  def test_origin_inequality(self):
    condition_1 = condition.ColumnsEqualCondition('origin', 'dest_model',
                                                  'dest_column')
    condition_2 = condition.ColumnsEqualCondition('original', 'dest_model',
                                                  'dest_column')
    self.assertNotEqual(condition_1, condition_2)

  def test_dest_model_inequality(self):
    condition_1 = condition.ColumnsEqualCondition('origin', 'dest_model',
                                                  'dest_column')
    condition_2 = condition.ColumnsEqualCondition('origin', 'destination',
                                                  'dest_column')
    self.assertNotEqual(condition_1, condition_2)

  def test_dest_column_inequality(self):
    condition_1 = condition.ColumnsEqualCondition('origin', 'dest_model',
                                                  'dest_column')
    condition_2 = condition.ColumnsEqualCondition('origin', 'dest_model',
                                                  'destination')
    self.assertNotEqual(condition_1, condition_2)


class OrderByConditionTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    self._asc = condition.OrderType.ASC
    self._desc = condition.OrderType.DESC

  @parameterized.parameters(True, False)
  def test_equality(self, bound_equality):
    condition_1 = condition.OrderByCondition(('value_1', self._asc),
                                             ('value_2', self._desc))
    condition_2 = condition.OrderByCondition(('value_1', self._asc),
                                             ('value_2', self._desc))
    if bound_equality:
      condition_1.bind(models.SmallTestModel)
      condition_2.bind(models.SmallTestModel)
    self.assertEqual(condition_1, condition_2)

  def test_bind_inequality(self):
    condition_1 = condition.OrderByCondition(('value_1', self._asc),
                                             ('value_2', self._desc))
    condition_2 = condition.OrderByCondition(('value_1', self._asc),
                                             ('value_2', self._desc))
    condition_1.bind(models.SmallTestModel)
    condition_2.bind(models.IdenticalSmallTestModel)
    self.assertNotEqual(condition_1, condition_2)

  def test_column_name_inequality(self):
    condition_1 = condition.OrderByCondition(('value_1', self._asc),
                                             ('value_2', self._desc))
    condition_2 = condition.OrderByCondition(('value_1', self._asc),
                                             ('key', self._desc))
    self.assertNotEqual(condition_1, condition_2)

  def test_order_type_inequality(self):
    condition_1 = condition.OrderByCondition(('value_1', self._asc),
                                             ('value_2', self._desc))
    condition_2 = condition.OrderByCondition(('value_1', self._asc),
                                             ('value_2', self._asc))
    self.assertNotEqual(condition_1, condition_2)

  def test_extra_order_inequality(self):
    condition_1 = condition.OrderByCondition(('value_1', self._asc),
                                             ('value_2', self._desc))
    condition_2 = condition.OrderByCondition(
        ('value_1', self._asc), ('value_2', self._desc), ('key', self._desc))
    self.assertNotEqual(condition_1, condition_2)


class ComparisonConditionTest(parameterized.TestCase):

  @parameterized.parameters(True, False)
  def test_equality(self, bound_equality):
    condition_1 = condition.ComparisonCondition('op', 'value_1', 'value')
    condition_2 = condition.ComparisonCondition('op', 'value_1', 'value')
    if bound_equality:
      condition_1.bind(models.SmallTestModel)
      condition_2.bind(models.SmallTestModel)
    self.assertEqual(condition_1, condition_2)

  def test_bind_inequality(self):
    condition_1 = condition.ComparisonCondition('op', 'value_1', 'value')
    condition_2 = condition.ComparisonCondition('op', 'value_1', 'value')
    condition_1.bind(models.SmallTestModel)
    condition_2.bind(models.IdenticalSmallTestModel)
    self.assertNotEqual(condition_1, condition_2)

  def test_operator_inequality(self):
    condition_1 = condition.ComparisonCondition('op', 'value_1', 'value')
    condition_2 = condition.ComparisonCondition('oops', 'value_1', 'value')
    self.assertNotEqual(condition_1, condition_2)

  def test_column_inequality(self):
    condition_1 = condition.ComparisonCondition('op', 'value_1', 'value')
    condition_2 = condition.ComparisonCondition('op', 'value_2', 'value')
    self.assertNotEqual(condition_1, condition_2)

  def test_value_inequality(self):
    condition_1 = condition.ComparisonCondition('op', 'value_1', 'value')
    condition_2 = condition.ComparisonCondition('op', 'value_1', 'valyou')
    self.assertNotEqual(condition_1, condition_2)


class NullableComparisonConditionTest(parameterized.TestCase):

  @parameterized.parameters(True, False)
  def test_equality(self, bound_equality):
    condition_1 = condition.NullableComparisonCondition('op', 'null_op',
                                                        'value_1', 'value')
    condition_2 = condition.NullableComparisonCondition('op', 'null_op',
                                                        'value_1', 'value')
    if bound_equality:
      condition_1.bind(models.SmallTestModel)
      condition_2.bind(models.SmallTestModel)
    self.assertEqual(condition_1, condition_2)

  def test_bind_inequality(self):
    condition_1 = condition.NullableComparisonCondition('op', 'null_op',
                                                        'value_1', 'value')
    condition_2 = condition.NullableComparisonCondition('op', 'null_op',
                                                        'value_1', 'value')
    condition_1.bind(models.SmallTestModel)
    condition_2.bind(models.IdenticalSmallTestModel)
    self.assertNotEqual(condition_1, condition_2)

  def test_operator_inequality(self):
    condition_1 = condition.NullableComparisonCondition('op', 'null_op',
                                                        'value_1', 'value')
    condition_2 = condition.NullableComparisonCondition('oops', 'null_op',
                                                        'value_1', 'value')
    self.assertNotEqual(condition_1, condition_2)

  def test_column_inequality(self):
    condition_1 = condition.NullableComparisonCondition('op', 'null_op',
                                                        'value_1', 'value')
    condition_2 = condition.NullableComparisonCondition('op', 'null_op',
                                                        'value_2', 'value')
    self.assertNotEqual(condition_1, condition_2)

  def test_value_inequality(self):
    condition_1 = condition.NullableComparisonCondition('op', 'null_op',
                                                        'value_1', 'value')
    condition_2 = condition.NullableComparisonCondition('op', 'null_op',
                                                        'value_1', 'valyou')
    self.assertNotEqual(condition_1, condition_2)

  def test_null_operator_inequality(self):
    condition_1 = condition.NullableComparisonCondition('op', 'null_op',
                                                        'value_1', 'value')
    condition_2 = condition.NullableComparisonCondition('op', 'null_oops',
                                                        'value_1', 'value')
    self.assertNotEqual(condition_1, condition_2)
