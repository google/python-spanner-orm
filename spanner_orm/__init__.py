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
"""Sets up shortcuts for imports from the library."""
import logging

from spanner_orm import api
from spanner_orm import condition
from spanner_orm import decorator
from spanner_orm import error
from spanner_orm import field
from spanner_orm import model
from spanner_orm import relationship
from spanner_orm.admin import api as admin_api

# add NullHandler to root-module logger so that individual modules
# won't have to.
logging.getLogger(__name__).addHandler(logging.NullHandler())

# pylint: disable=invalid-name
SpannerApi = api.SpannerApi
SpannerAdminApi = admin_api.SpannerAdminApi
SpannerError = error.SpannerError

Model = model.Model

Boolean = field.Boolean
Field = field.Field
Integer = field.Integer
Relationship = relationship.Relationship
String = field.String
StringArray = field.StringArray
Timestamp = field.Timestamp

equal_to = condition.equal_to
greater_than = condition.greater_than
greater_than_or_equal_to = condition.greater_than_or_equal_to
includes = condition.includes
in_list = condition.in_list
less_than = condition.less_than
less_than_or_equal_to = condition.less_than_or_equal_to
limit = condition.limit
not_equal_to = condition.not_equal_to
not_greater_than = condition.not_greater_than
not_in_list = condition.not_in_list
not_less_than = condition.not_less_than
order_by = condition.order_by
ORDER_ASC = condition.OrderType.ASC
ORDER_DESC = condition.OrderType.DESC

transactional_read = decorator.transactional_read
transactional_write = decorator.transactional_write
