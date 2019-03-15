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
"""Base model for schemas."""

from __future__ import annotations

from typing import Any, Callable, NoReturn, Optional, TypeVar

from spanner_orm import error
from spanner_orm import model
from spanner_orm.admin import api

from google.cloud.spanner_v1 import transaction as spanner_transaction

CallableReturn = TypeVar('CallableReturn')


class InformationSchema(model.Model):
  """Base model for Spanner INFORMATION_SCHEMA tables.

  Note: Writes are disallowed and AdminApi is used for reads.
  """

  @classmethod
  def _execute_read(cls, db_api: Callable[..., CallableReturn],
                    transaction: Optional[spanner_transaction.Transaction],
                    args: Any) -> CallableReturn:
    if transaction is not None:
      return db_api(transaction, *args)
    else:
      return api.SpannerAdminApi.run_read_only(db_api, *args)

  @classmethod
  def _execute_write(cls, *args) -> NoReturn:
    raise error.SpannerError('Writes not allowed for schema tables')
