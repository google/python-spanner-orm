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

from spanner_orm import field
from spanner_orm.admin import schema


class TableSchema(schema.InformationSchema):
  """Model for interacting with Spanner column schema table."""

  __table__ = 'information_schema.tables'
  table_catalog = field.Field(field.String, primary_key=True)
  table_schema = field.Field(field.String, primary_key=True)
  table_name = field.Field(field.String, primary_key=True)
  parent_table_name = field.Field(field.String, nullable=True)
  on_delete_action = field.Field(field.String, nullable=True)
