"""Spanner ORM migration: create_custom_length_field.

Migration ID: 'f959b767457d'
Created: 2022-09-13 13:28:34-07:00
"""

import spanner_orm

migration_id = 'f959b767457d'
prev_migration_id = '69a8f072dacf'


class OriginalTeeTable(spanner_orm.model.Model):
  """ORM Model with the original schema for the Commands table.
  Don't update this model, create new migrations instead.
  """

  __table__ = 'Tee'
  id = spanner_orm.Field(spanner_orm.String, primary_key=True)
  custom_string_length = spanner_orm.Field(spanner_orm.String(20))
  custom_array_string_length = spanner_orm.Field(
      spanner_orm.Array(spanner_orm.String(4)))
  custom_bytes_length = spanner_orm.Field(spanner_orm.BytesBase64(20))
  custom_array_bytes_length = spanner_orm.Field(
      spanner_orm.Array(spanner_orm.BytesBase64(4)))


def upgrade() -> spanner_orm.CreateTable:
  """Creates the original Commands table."""
  return spanner_orm.CreateTable(OriginalTeeTable)


def downgrade() -> spanner_orm.DropTable:
  """Drops the original Commands table."""
  return spanner_orm.DropTable(OriginalTeeTable.__table__)
