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
"""Python test wrapper for the cloud spanner emulator binary."""

import os
import subprocess
from typing import Mapping, Optional

import portpicker

from google.auth import credentials
from google.cloud.spanner_v1 import client

# Environment variable with path to Spanner Emulator binary.
_EMULATOR_BINARY_PATH_ENV_VAR = "SPANNER_EMULATOR_BINARY_PATH"
# Environment variable used by the client library to set the correct URL.
_CLIENT_EMULATOR_ENV_VAR = "SPANNER_EMULATOR_HOST"


class Emulator:
  """Spanner emulator python wrapper.

  Below is an example of how this wrapper can be used in a test class.

  class SpannerTest(googletest.TestCase):

    def setUp(self):
      super().setUp()
      self._spanner_emulator = emulator.Emulator()
      self.addCleanup(self._spanner_emulator.stop)

    def test_something(self):
      client = self._spanner_emulator.get_client()
      # Create tables, add data, and retrieve it
  """

  def __init__(self,
               *,
               spanner_emulator_port: Optional[int] = None,
               log_emulator_requests: bool = False) -> None:
    """Initializer.

    Args:
      spanner_emulator_port: The port to start the emulator on. A random unused
        port is picked if this value is None.
      log_emulator_requests: If true, the emulator subprocess will log each
        request and response message.
    """

    self._spanner_emulator_port = spanner_emulator_port
    self._log_emulator_requests = log_emulator_requests

    self._process = None
    self._host_port = None

    self._start()
    self._wait_for_ready()

  def get_client(
      self,
      project: str = "test-project",
      client_options: Optional[Mapping[str, str]] = None) -> client.Client:
    """Returns a spanner client for interacting with the emulator.

    Args:
      project: Name of the project that the client should point to.
      client_options: Any client options that the client should be created with.
    """
    return client.Client(
        project=project,
        credentials=credentials.AnonymousCredentials(),
        client_options=client_options)

  def _start(self) -> None:
    """Starts the emulator as a subprocess."""
    port = self._spanner_emulator_port or portpicker.pick_unused_port()
    self._host_port = f"localhost:{port}"

    # Used by the client library to point to the correct spanner endpoint.
    os.environ[_CLIENT_EMULATOR_ENV_VAR] = self._host_port

    try:
      emulator_binary_path = os.environ[_EMULATOR_BINARY_PATH_ENV_VAR]
    except KeyError as key_error:
      raise ValueError(
          f'Please set the environment variable {_EMULATOR_BINARY_PATH_ENV_VAR} '
          'to a binary with the Cloud Spanner Emulator. For more info, see '
          'https://github.com/GoogleCloudPlatform/cloud-spanner-emulator.'
      ) from key_error

    self._process = subprocess.Popen([
        emulator_binary_path,
        "--log_requests" if self._log_emulator_requests else "--nolog_requests",
        "--host_port",
        self._host_port,
    ])

  def _wait_for_ready(self) -> None:
    """Waits for the emulator to become ready."""
    emulator_client = self.get_client()

    # This will not return until the emulator is running.
    for _ in emulator_client.list_instance_configs():
      return

  def stop(self) -> None:
    """If there is an emulator process, stops it and waits for it to stop."""
    if self._process is not None:
      self._process.terminate()
      self._process.wait()
      self._process = None
      self._host_port = None
