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
"""Model for interacting with Spanner column schema table."""

import typing
from typing import Type

from spanner_orm import error
from spanner_orm import field
from spanner_orm.admin import schema

allow_commit_timestamp_option = ' OPTIONS (allow_commit_timestamp=true)'


class ColumnSchema(schema.InformationSchema):
  """Model for interacting with Spanner column schema table."""

  __table__ = 'information_schema.columns'
  table_catalog = typing.cast(str, field.Field(field.String, primary_key=True))
  table_schema = typing.cast(str, field.Field(field.String, primary_key=True))
  table_name = typing.cast(str, field.Field(field.String, primary_key=True))
  column_name = typing.cast(str, field.Field(field.String, primary_key=True))
  ordinal_position = typing.cast(int, field.Field(field.Integer))
  is_nullable = typing.cast(str, field.Field(field.String))
  spanner_type = typing.cast(str, field.Field(field.String))

  def nullable(self) -> bool:
    return self.is_nullable == 'YES'

  def field_type(self) -> Type[field.FieldType]:
    for field_type in field.ALL_TYPES:
      len = _get_type_len(self.spanner_type)
      contain_commit_timestamp_option = _get_commit_timestamp_option(
          self.spanner_type)
      actual_type = field_type.ddl()
      if len:
        actual_type = actual_type.replace('(MAX)', f'({len})')
      if contain_commit_timestamp_option:
        actual_type = f'{actual_type}{allow_commit_timestamp_option}'
      if self.spanner_type == actual_type:
        return field_type

    raise error.SpannerError('No corresponding Type for {}'.format(
        self.spanner_type))


def _get_type_len(spanner_type: str) -> int:
  """Retrieve length for existing STRING, BYTES or ARRAY<STRING> field."""
  bytes_len = spanner_type[6:-1]
  str_len = spanner_type[7:-1]
  array_str_len = spanner_type[13:-2]
  if bytes_len.isnumeric():
    return int(bytes_len)
  elif str_len.isnumeric():
    return int(str_len)
  elif array_str_len.isnumeric():
    return int(array_str_len)
  return 0


def _get_commit_timestamp_option(spanner_type: str) -> bool:
  """Retrive commit timestamp option if any."""
  return spanner_type.startswith('TIMESTAMP') and spanner_type.endswith(
      allow_commit_timestamp_option)
