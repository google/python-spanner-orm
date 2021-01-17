# python3
# Copyright 2020 Google LLC
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
import datetime
import logging
import os
import unittest

import spanner_orm
from spanner_orm.tests import models
from spanner_orm.testlib.spanner_emulator import testlib as spanner_emulator_testlib

from google.api_core import exceptions as google_api_exceptions



class MigrationsEmulatorTest(spanner_emulator_testlib.TestCase):
  TEST_MIGRATIONS_DIR = os.path.join(
      os.path.dirname(os.path.abspath(__file__)),
      'migrations_for_emulator_test',
  )

  def setUp(self):
    super().setUp()
    self.run_orm_migrations(self.TEST_MIGRATIONS_DIR)

  def test_basic(self):
    models.SmallTestModel({'key': 'key', 'value_1': 'value'}).save()
    self.assertEqual(
        [x.values for x in models.SmallTestModel.all()],
        [{
            'key': 'key',
            'value_1': 'value',
            'value_2': None,
        }],
    )

  def test_error_with_missing_referencing_key(self):
    with self.assertRaisesRegex(
        google_api_exceptions.FailedPrecondition,
        'Cannot find referenced key',
    ):
      models.ForeignKeyTestModel({
        'referencing_key_1': 'key',
        'referencing_key_2': 'key',
        'referencing_key_3': 42,
        'value': 'value'
      }).save()

  def test_key(self):
    models.SmallTestModel({'key': 'key', 'value_1': 'value'}).save()
    models.UnittestModel(
      {'string': 'string',
       'int_': 42,
       'float_': 4.2,
       'timestamp': datetime.datetime.now(tz=datetime.timezone.utc),
      }).save()
    models.ForeignKeyTestModel({
        'referencing_key_1': 'key',
        'referencing_key_2': 'string',
        'referencing_key_3': 42,
        'value': 'value'
      }).save()

if __name__ == '__main__':
  logging.basicConfig()
  unittest.main()
