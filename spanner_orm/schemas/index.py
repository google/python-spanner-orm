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
"""Model for interacting with Spanner index schema table."""

from spanner_orm import field
from spanner_orm.schemas import schema


class IndexSchema(schema.Schema):
  """Model for interacting with Spanner index schema table."""

  @staticmethod
  def primary_index_keys():
    return ['table_catalog', 'table_schema', 'table_name', 'index_name']

  @classmethod
  def schema(cls):
    return {
        'table_catalog': field.String,
        'table_schema': field.String,
        'table_name': field.String,
        'index_name': field.String,
        'index_type': field.String,
        'parent_table_name': field.NullableString,
        'is_unique': field.Boolean,
        'is_null_filtered': field.Boolean,
        'index_state': field.String
    }

  @classmethod
  def table(cls):
    return 'information_schema.indexes'
