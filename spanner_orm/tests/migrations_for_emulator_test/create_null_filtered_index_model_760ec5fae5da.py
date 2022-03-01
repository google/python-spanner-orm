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
"""Spanner ORM migration: create_null_filtered_index_model.

Migration ID: '760ec5fae5da'
Created: 2022-03-01 16:50:32-05:00
"""

import spanner_orm

migration_id = '760ec5fae5da'
prev_migration_id = 'f735d6b706d4'


class _NullFilteredIndexModel(spanner_orm.Model):
  __table__ = 'NullFilteredIndexModel'
  key = spanner_orm.Field(spanner_orm.String, primary_key=True)
  value_1 = spanner_orm.Field(spanner_orm.String, nullable=True)
  value_2 = spanner_orm.Field(spanner_orm.Integer)


def upgrade() -> spanner_orm.MigrationUpdate:
  """See spanner_orm migrations interface."""
  return spanner_orm.CreateTable(_NullFilteredIndexModel)


def downgrade() -> spanner_orm.MigrationUpdate:
  """See spanner_orm migrations interface."""
  return spanner_orm.DropTable(_NullFilteredIndexModel.__table__)
