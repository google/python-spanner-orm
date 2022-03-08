"""Spanner ORM migration: create_null_filtered_index_model_value_index.

Migration ID: '69a8f072dacf'
Created: 2022-03-01 16:53:59-05:00
"""

import spanner_orm

migration_id = '69a8f072dacf'
prev_migration_id = '760ec5fae5da'


def upgrade() -> spanner_orm.MigrationUpdate:
  """See spanner_orm migrations interface."""
  return spanner_orm.CreateIndex(
      table_name='NullFilteredIndexModel',
      index_name='value_index',
      columns=['value_1', 'value_2'],
      null_filtered=True,
  )


def downgrade() -> spanner_orm.MigrationUpdate:
  """See spanner_orm migrations interface."""
  return spanner_orm.DropIndex(
      table_name='NullFilteredIndexModel',
      index_name='value_index',
  )
