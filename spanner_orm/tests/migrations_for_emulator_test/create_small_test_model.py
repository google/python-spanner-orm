# Lint as: python3
"""Creates table with SmallTestModel.

Migration ID: 'f735d6b706d2'
Created: 2020-07-10 16:24
"""

import spanner_orm
from spanner_orm import field

migration_id = 'f735d6b706d2'
prev_migration_id = None


class OriginalSmallTestModelsTable(spanner_orm.model.Model):
  """ORM Model with the original schema for the DiabloVerdicts table."""

  __table__ = 'SmallTestModel'
  key = field.Field(field.String, primary_key=True)
  value_1 = field.Field(field.String)
  value_2 = field.Field(field.String, nullable=True)


def upgrade() -> spanner_orm.CreateTable:
  """See ORM migrations interface."""
  return spanner_orm.CreateTable(OriginalSmallTestModelsTable)


def downgrade() -> spanner_orm.DropTable:
  """See ORM migrations interface."""
  return spanner_orm.DropTable(OriginalSmallTestModelsTable.__table__)
