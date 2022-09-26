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
"""Creates table with UnittestModel.

Migration ID: 'f735d6b706d3'
Created: 2020-07-10 16:24
"""

import spanner_orm
from spanner_orm import field

migration_id = 'f735d6b706d3'
prev_migration_id = 'f735d6b706d2'


class OriginalUnittestModelTable(spanner_orm.model.Model):
  """ORM Model with the original schema for the UnittestModel table."""

  __table__ = 'table'
  int_ = field.Field(field.Integer, primary_key=True)
  int_2 = field.Field(field.Integer, nullable=True)
  float_ = field.Field(field.Float, primary_key=True)
  float_2 = field.Field(field.Float, nullable=True)
  string = field.Field(field.String, primary_key=True)
  string_2 = field.Field(field.String, nullable=True)
  string_3 = field.Field(field.String(20), nullable=True)
  bytes_ = field.Field(field.BytesBase64, primary_key=True)
  bytes_2 = field.Field(field.BytesBase64, nullable=True)
  bytes_3 = field.Field(field.BytesBase64(20), nullable=True)
  timestamp = field.Field(field.Timestamp)
  string_array = field.Field(field.StringArray, nullable=True)
  string_array_2 = field.Field(field.Array(field.String(20)), nullable=True)


def upgrade() -> spanner_orm.CreateTable:
  """See ORM migrations interface."""
  return spanner_orm.CreateTable(OriginalUnittestModelTable)


def downgrade() -> spanner_orm.DropTable:
  """See ORM migrations interface."""
  return spanner_orm.DropTable(OriginalUnittestModelTable.__table__)
