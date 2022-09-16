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

import logging
import unittest

from spanner_orm.tests import models


class MetadataTest(unittest.TestCase):

  def test_metadata_present(self):
    columns = ['key', 'value_1', 'value_2']
    indexes = ['PRIMARY_KEY', 'index_1']
    table = 'SmallTestModel'
    self.assertCountEqual(columns, models.SmallTestModel.meta.columns)
    self.assertCountEqual(columns, models.SmallTestModel.meta.fields.keys())
    self.assertCountEqual(indexes, models.SmallTestModel.meta.indexes.keys())
    self.assertEqual(table, models.SmallTestModel.meta.table)

  def test_metadata_inheritance(self):
    self.assertEqual(models.SmallTestModel.meta.indexes,
                     models.InheritanceTestModel.meta.indexes)

    self.assertEqual(models.SmallTestModel.meta.table,
                     models.InheritanceTestModel.meta.table)

    self.assertEqual(models.SmallTestModel.meta.relations,
                     models.InheritanceTestModel.meta.relations)

    self.assertEqual(models.SmallTestModel.meta.interleaved,
                     models.InheritanceTestModel.meta.interleaved)


if __name__ == '__main__':
  logging.basicConfig()
  unittest.main()
