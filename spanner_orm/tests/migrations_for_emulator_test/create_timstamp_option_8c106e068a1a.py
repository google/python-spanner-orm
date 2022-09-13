"""Spanner ORM migration: create_timstamp_option.

Migration ID: '8c106e068a1a'
Created: 2022-09-12 19:38:24-07:00
"""

import spanner_orm

migration_id = '8c106e068a1a'
prev_migration_id = '69a8f072dacf'


class OriginalTeeTable(spanner_orm.model.Model):
  """ORM Model with the original schema for the Commands table.

  Don't update this model, create new migrations instead.
  """

  __table__ = 'Tee'
  id = spanner_orm.Field(spanner_orm.String, primary_key=True)
  timestamp = spanner_orm.Field(
      spanner_orm.Timestamp, nullable=False, allow_commit_timestamp=True)
  cus_str = spanner_orm.Field(spanner_orm.String, length=555)
  cus_bytes = spanner_orm.Field(spanner_orm.BytesBase64, length=12)
  cus_strarr = spanner_orm.Field(spanner_orm.StringArray, length=24)


def upgrade() -> spanner_orm.CreateTable:
  """Creates the original Commands table."""
  return spanner_orm.CreateTable(OriginalTeeTable)


def downgrade() -> spanner_orm.DropTable:
  """Drops the original Commands table."""
  return spanner_orm.DropTable(OriginalTeeTable.__table__)
