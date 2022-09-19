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
"""Creates table with ForeignKeyTestModel.

Migration ID: 'f735d6b706d3'
Created: 2020-07-10 16:24
"""

import spanner_orm
from spanner_orm import field
from spanner_orm import foreign_key_relationship

migration_id = 'f735d6b706d4'
prev_migration_id = 'f735d6b706d3'


class OriginalForeignKeyTestModelTable(spanner_orm.model.Model):
  """ORM Model with the original schema for the ForeignKeyTestModel table."""

  __table__ = 'ForeignKeyTestModel'
  referencing_key_1 = field.Field(field.String, primary_key=True)
  referencing_key_2 = field.Field(field.String, primary_key=True)
  referencing_key_3 = field.Field(field.Integer, primary_key=True)
  self_referencing_key = field.Field(field.String, nullable=True)
  foreign_key_1 = foreign_key_relationship.ForeignKeyRelationship(
      'SmallTestModel', {'referencing_key_1': 'key'})
  foreign_key_2 = foreign_key_relationship.ForeignKeyRelationship(
      'UnittestModel',
      {
          'referencing_key_2': 'string',
          'referencing_key_3': 'int_'
      },
  )
  foreign_key_3 = foreign_key_relationship.ForeignKeyRelationship(
      'ForeignKeyTestModel', {'self_referencing_key': 'referencing_key_1'})


def upgrade() -> spanner_orm.CreateTable:
  """See ORM migrations interface."""
  return spanner_orm.CreateTable(OriginalForeignKeyTestModelTable)


def downgrade() -> spanner_orm.DropTable:
  """See ORM migrations interface."""
  return spanner_orm.DropTable(OriginalForeignKeyTestModelTable.__table__)
