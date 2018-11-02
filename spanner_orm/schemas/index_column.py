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
"""Model for interacting with Spanner index column schema table."""

from spanner_orm.schemas.schema import Schema
from spanner_orm.type import NullableInteger
from spanner_orm.type import NullableString
from spanner_orm.type import String


class IndexColumnSchema(Schema):
  """Model for interacting with Spanner index column schema table."""

  @staticmethod
  def primary_index_keys():
    return [
        'table_catalog', 'table_schema', 'table_name', 'index_name',
        'column_name'
    ]

  @classmethod
  def schema(cls):
    return {
        'table_catalog': String,
        'table_schema': String,
        'table_name': String,
        'index_name': String,
        'column_name': String,
        'ordinal_position': NullableInteger,
        'column_ordering': NullableString,
        'is_nullable': String,
        'spanner_type': String
    }

  @classmethod
  def table(cls):
    return 'information_schema.index_columns'
