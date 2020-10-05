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
"""Holds information about a specific migration."""

from typing import Callable, Optional

from spanner_orm.admin import update


def no_update_callable() -> update.SchemaUpdate:
  return update.NoUpdate()


class Migration:
  """Holds information about a specific migration."""

  def __init__(self,
               migration_id: str,
               prev_migration_id: Optional[str],
               description: str,
               upgrade: Optional[Callable[[], update.SchemaUpdate]] = None,
               downgrade: Optional[Callable[[], update.SchemaUpdate]] = None):
    self._id = migration_id
    self._description = description
    self._prev = prev_migration_id
    self._upgrade = upgrade or no_update_callable
    self._downgrade = downgrade or no_update_callable

  @property
  def migration_id(self) -> str:
    return self._id

  @property
  def prev_migration_id(self) -> Optional[str]:
    return self._prev

  @property
  def description(self) -> str:
    return self._description

  @property
  def upgrade(self) -> Optional[Callable[[], update.SchemaUpdate]]:
    return self._upgrade

  @property
  def downgrade(self) -> Optional[Callable[[], update.SchemaUpdate]]:
    return self._downgrade
