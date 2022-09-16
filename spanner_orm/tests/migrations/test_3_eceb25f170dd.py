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
"""test_3.

Migration ID: eceb25f170dd
Created: 2019-02-27 18:52
"""

import spanner_orm

migration_id = 'eceb25f170dd'
prev_migration_id = '5c078bbb4d43'


# Returns a SchemaUpdate object that tells what should be changed
def upgrade() -> spanner_orm.NoUpdate:
  return spanner_orm.NoUpdate()


# Returns a SchemaUpdate object that tells how to roll back the changes
def downgrade() -> spanner_orm.NoUpdate:
  return spanner_orm.NoUpdate()
