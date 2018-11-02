# python3
# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Base model for schemas."""

from spanner_orm.admin.api import DatabaseAdminApi
from spanner_orm.model import Model


class Schema(Model):
  """Base model for schemas. Disallows writes and uses AdminApi for reads."""

  @staticmethod
  def _execute_read(db_api, transaction, args):
    if transaction is not None:
      return db_api(transaction, *args)
    else:
      return DatabaseAdminApi.run_read_only(db_api, *args)

  @staticmethod
  def _execute_write():
    raise AssertionError('Writes not allowed for schema tables')
