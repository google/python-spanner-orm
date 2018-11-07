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
"""Model for interacting with Spanner column schema table."""

from spanner_orm import error
from spanner_orm import field
from spanner_orm.schemas import schema


class ColumnSchema(schema.Schema):
  """Model for interacting with Spanner column schema table."""

  @staticmethod
  def primary_index_keys():
    return ['table_catalog', 'table_schema', 'table_name', 'column_name']

  @classmethod
  def schema(cls):
    return {
        'table_catalog': field.String,
        'table_schema': field.String,
        'table_name': field.String,
        'column_name': field.String,
        'ordinal_position': field.Integer,
        'is_nullable': field.String,
        'spanner_type': field.String
    }

  @classmethod
  def table(cls):
    return 'information_schema.columns'

  def nullable(self):
    return self.is_nullable == 'YES'

  def type(self):
    for db_type in field.ALL_TYPES:
      db_nullable = issubclass(db_type, field.NullableType)
      if self.spanner_type == db_type.ddl() and self.nullable() == db_nullable:
        return db_type

    raise error.SpannerError('No corresponding Type for {}'.format(
        self.spanner_type))
