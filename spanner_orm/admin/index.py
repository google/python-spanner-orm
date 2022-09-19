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
"""Model for interacting with Spanner index schema table."""

import typing
from typing import Optional

from spanner_orm import field
from spanner_orm.admin import schema


class IndexSchema(schema.InformationSchema):
  """Model for interacting with Spanner index schema table."""

  __table__ = 'information_schema.indexes'
  table_catalog = typing.cast(str, field.Field(field.String, primary_key=True))
  table_schema = typing.cast(str, field.Field(field.String, primary_key=True))
  table_name = typing.cast(str, field.Field(field.String, primary_key=True))
  index_name = typing.cast(str, field.Field(field.String, primary_key=True))
  index_type = typing.cast(str, field.Field(field.String))
  parent_table_name = typing.cast(Optional[str],
                                  field.Field(field.String, nullable=True))
  is_unique = typing.cast(bool, field.Field(field.Boolean))
  is_null_filtered = typing.cast(bool, field.Field(field.Boolean))
  index_state = typing.cast(str, field.Field(field.String))
