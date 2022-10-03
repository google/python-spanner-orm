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

import base64
import datetime
from typing import Any
import warnings

from absl.testing import absltest
from absl.testing import parameterized
from google.cloud import spanner
from google.cloud import spanner_v1
from spanner_orm import error
from spanner_orm import field


class FieldTest(parameterized.TestCase):

  @parameterized.parameters(
      (field.Boolean(), 'BOOL'),
      (field.Integer(), 'INT64'),
      (field.Float(), 'FLOAT64'),
      (field.String(), 'STRING(MAX)'),
      (field.String(10), 'STRING(10)'),
      (field.Timestamp(), 'TIMESTAMP'),
      (field.BytesBase64(), 'BYTES(MAX)'),
      (field.BytesBase64(10), 'BYTES(10)'),
      (field.Array(field.Boolean()), 'ARRAY<BOOL>'),
      (field.Array(field.String()), 'ARRAY<STRING(MAX)>'),
      (field.Array(field.String(10)), 'ARRAY<STRING(10)>'),
      (field.Array(field.BytesBase64()), 'ARRAY<BYTES(MAX)>'),
      (field.Array(field.BytesBase64(10)), 'ARRAY<BYTES(10)>'),
  )
  def test_field_type_ddl(
      self,
      field_type: field.FieldType,
      ddl: str,
  ):
    self.assertEqual(field_type.ddl(), ddl)

  @parameterized.parameters(
      (field.Boolean(), spanner.param_types.BOOL),
      (field.Integer(), spanner.param_types.INT64),
      (field.Float(), spanner.param_types.FLOAT64),
      (field.String(), spanner.param_types.STRING),
      (field.String(10), spanner.param_types.STRING),
      (field.Timestamp(), spanner.param_types.TIMESTAMP),
      (field.BytesBase64(), spanner.param_types.BYTES),
      (field.BytesBase64(10), spanner.param_types.BYTES),
      (field.Array(field.Boolean()),
       spanner.param_types.Array(spanner.param_types.BOOL)),
      (field.Array(field.String()),
       spanner.param_types.Array(spanner.param_types.STRING)),
      (field.Array(field.String(10)),
       spanner.param_types.Array(spanner.param_types.STRING)),
  )
  def test_field_type_grpc_type(
      self,
      field_type: field.FieldType,
      grpc_type: spanner_v1.Type,
  ):
    self.assertEqual(field_type.grpc_type(), grpc_type)

  @parameterized.parameters(
      (field.Boolean(), True),
      (field.Integer(), 1),
      (field.Float(), 1),
      (field.Float(), 1.0),
      (field.String(), 'foo'),
      (field.String(10), 'foo'),
      (field.Timestamp(), datetime.datetime(2022, 9, 21)),
      (field.BytesBase64(), base64.b64encode(b'\x00')),
      (field.BytesBase64(10), base64.b64encode(b'\x00')),
      (field.Array(field.Boolean()), [True]),
  )
  def test_field_type_validate_type_ok(
      self,
      field_type: field.FieldType,
      value: Any,
  ):
    field_type.validate_type(value)

  @parameterized.parameters(
      (field.Boolean(), 1),
      (field.Integer(), 1.0),
      (field.Float(), '1.0'),
      (field.String(), b'foo'),
      (field.String(10), b'foo'),
      (field.Timestamp(), datetime.date(2022, 9, 21)),
      (field.BytesBase64(), base64.b64encode(b'\x00').decode('utf-8')),
      (field.BytesBase64(), b'!'),
      (field.BytesBase64(10), b'!'),
      (field.Array(field.Boolean()), {True}),
      (field.Array(field.Boolean()), [1]),
  )
  def test_field_type_validate_type_error(
      self,
      field_type: field.FieldType,
      value: Any,
  ):
    with self.assertRaises(error.ValidationError):
      field_type.validate_type(value)

  @parameterized.parameters(
      (field.Boolean(), field.Boolean(), True),
      (field.Boolean(), field.String(), False),
      (field.String(10), field.String(20), True),
      (field.String(), field.String(10), True),
      (field.Array(field.Integer()), field.Array(field.Integer()), False),
      (field.Array(field.Integer()), field.Integer(), False),
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

  def test_array_of_array_is_invalid(self):
    with self.assertRaisesRegex(error.SpannerError, 'arrays of arrays'):
      field.Array(field.Array(field.String()))

  def test_string_array_is_deprecated_and_equivalent_to_array_of_string(self):
    with warnings.catch_warnings(record=True) as actual_warnings:
      string_array = field.StringArray()
    array_of_string = field.Array(field.String())
    self.assertLen(actual_warnings, 1)
    self.assertIn('Use Array(String()) instead',
                  str(actual_warnings[0].message))
    self.assertIs(actual_warnings[0].category, DeprecationWarning)
    self.assertEqual(string_array.ddl(), array_of_string.ddl())
    self.assertEqual(string_array.grpc_type(), array_of_string.grpc_type())

  @parameterized.parameters(
      'BOOL',
      'INT64',
      'FLOAT64',
      'STRING(MAX)',
      'STRING(10)',
      'TIMESTAMP',
      'BYTES(MAX)',
      'BYTES(10)',
      'ARRAY<INT64>',
      'ARRAY<STRING(MAX)>',
      'ARRAY<STRING(10)>',
  )
  def test_ddl_to_field_type_to_ddl(self, ddl: str):
    self.assertEqual(field.field_type_from_ddl(ddl).ddl(), ddl)

  @parameterized.parameters('UNICORN(MAX)', 'STRING(MAX1)', 'STRING(MIN)',
                            'ARRAY<STRING(MAX1)>', 'BYTES(MAX1)', 'BYTES(MIN)')
  def test_field_type_from_ddl_invalid(self, ddl: str):
    with self.assertRaisesRegex(error.SpannerError, 'DDL type'):
      field.field_type_from_ddl(ddl)


if __name__ == '__main__':
  absltest.main()
