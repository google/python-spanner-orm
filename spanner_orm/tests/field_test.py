# Copyright 2022 Google LLC
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
"""Tests for field."""

import warnings

from spanner_orm import error
from spanner_orm import field
from absl.testing import absltest
from absl.testing import parameterized


class FieldTest(parameterized.TestCase):

  @parameterized.parameters(
      (field.Boolean(), field.Boolean(), True),
      (field.Boolean(), field.String(), False),
  )
  def test_field_type_comparable_with(
      self,
      field_type_1: field.FieldType,
      field_type_2: field.FieldType,
      expected_comparable: bool,
  ):
    self.assertEqual(
        field_type_1.comparable_with(field_type_2), expected_comparable)
    self.assertEqual(
        field_type_2.comparable_with(field_type_1), expected_comparable)

  def test_field_field_type_is_class(self):
    with warnings.catch_warnings(record=True) as actual_warnings:
      self.assertIsInstance(
          field.Field(field.String).field_type(), field.String)
    self.assertLen(actual_warnings, 1)
    self.assertIn('instance of FieldType', str(actual_warnings[0].message))
    self.assertIs(actual_warnings[0].category, DeprecationWarning)

  @parameterized.parameters(
      'BOOL',
      'INT64',
      'FLOAT64',
      'STRING(MAX)',
      'ARRAY<STRING(MAX)>',
      'TIMESTAMP',
      'BYTES(MAX)',
  )
  def test_ddl_to_field_type_to_ddl(self, ddl: str):
    self.assertEqual(field.field_type_from_ddl(ddl).ddl(), ddl)

  def test_field_type_from_ddl_invalid(self):
    with self.assertRaisesRegex(error.SpannerError, 'DDL type'):
      field.field_type_from_ddl('UNICORN(MAX)')


if __name__ == '__main__':
  absltest.main()
