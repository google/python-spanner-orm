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
"""Indicates whether a migration has been applied to the current database."""

from spanner_orm import field
from spanner_orm import model
from spanner_orm.admin import api


class MigrationStatus(model.Model):

  @classmethod
  def spanner_api(cls) -> api.SpannerAdminApi:
    return api.spanner_admin_api()

  __table__ = 'spanner_orm_migrations'
  id = field.Field(field.String, primary_key=True)
  migrated = field.Field(field.Boolean)
  update_time = field.Field(field.Timestamp)
