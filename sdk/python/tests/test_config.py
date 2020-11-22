# Copyright 2020 The Feast Authors
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

import os
from tempfile import mkstemp

import pytest

from feast.config import Config


class TestConfig:
    @pytest.fixture
    def normal_config(self):
        fd, path = mkstemp()
        return Config(path=path)

    def test_init_config_file_with_path(self):
        configuration_string = "[general]\nCORE_URL = grpc://127.0.0.1:6565"

        fd, path = mkstemp()
        with open(fd, "w") as f:
            f.write(configuration_string)
        config = Config(path=path)
        assert config.get("core_url") == "grpc://127.0.0.1:6565"

    def test_load_environmental_variable(self, normal_config):
        import os

        serving_url = "http://196.25.1.1"
        os.environ["FEAST_SERVING_URL"] = serving_url
        assert normal_config.get("SERVING_URL") == serving_url
        del os.environ["FEAST_SERVING_URL"]

    def test_env_var_not_case_sensitive(self, normal_config):
        import os

        serving_url = "http://196.25.1.1"
        os.environ["FEAST_SerVING_url"] = serving_url
        assert normal_config.get("SERVING_URL") == serving_url

    def test_force_options(self):
        fd, path = mkstemp()
        options = {"feast_config_1": "one", "random_config_two": 2}
        config = Config(options, path)
        assert config.get("feast_config_1") == "one"

    def test_init_options_precedence(self):
        """
        Init options > env var > file options > default options
        """
        fd, path = mkstemp()
        os.environ["FEAST_CORE_URL"] = "env"
        options = {"core_url": "init", "serving_url": "init"}
        configuration_string = "[general]\nCORE_URL = file\n"
        with open(fd, "w") as f:
            f.write(configuration_string)
        config = Config(options, path)
        assert config.get("core_url") == "init"
        del os.environ["FEAST_CORE_URL"]

    def test_env_var_precedence(self):
        """
        Env vars > file options > default options
        """
        fd, path = mkstemp()
        os.environ["FEAST_CORE_URL"] = "env"
        configuration_string = "[general]\nCORE_URL = file\n"
        with open(fd, "w") as f:
            f.write(configuration_string)
        config = Config(path=path)
        assert config.get("CORE_URL") == "env"

        del os.environ["FEAST_CORE_URL"]

    def test_file_option_precedence(self):
        """
        file options > default options
        """
        fd, path = mkstemp()
        configuration_string = "[general]\nCORE_URL = file\n"
        with open(fd, "w") as f:
            f.write(configuration_string)
        config = Config(path=path)
        assert config.get("CORE_URL") == "file"

    def test_default_options(self):
        """
        default options
        """
        fd, path = mkstemp()
        config = Config(path=path)
        assert config.get("CORE_URL") == "localhost:6565"

    def test_defaults_are_not_written(self):
        """
        default values are not written to config file
        """
        fd, path = mkstemp()
        config = Config(path=path)
        config.set("option", "value")
        config.save()
        with open(path) as f:
            assert f.read() == "[general]\noption = value\n\n"

    def test_type_casting(self):
        """
        Test type casting of strings to other types
        """
        fd, path = mkstemp()
        os.environ["FEAST_INT_VAR"] = "1"
        os.environ["FEAST_FLOAT_VAR"] = "1.0"
        os.environ["FEAST_BOOLEAN_VAR"] = "True"
        config = Config(path=path)

        assert config.getint("INT_VAR") == 1
        assert config.getfloat("FLOAT_VAR") == 1.0
        assert config.getboolean("BOOLEAN_VAR") is True

    def test_type_casting_of_defaults(self):
        """
        default values are casted as expected
        """
        fd, path = mkstemp()
        config = Config(path=path)
        assert isinstance(config.getboolean("enable_auth"), bool)
        assert isinstance(config.getint("DATAPROC_EXECUTOR_INSTANCES"), int)
        assert isinstance(config.getfloat("DATAPROC_EXECUTOR_INSTANCES"), float)

    def test_set_value(self):
        """
        Test type casting of strings to other types
        """
        fd, path = mkstemp()
        config = Config(path=path)
        config.set("my_val", 1)

        assert config.getint("my_val") == 1

    def test_exists(self):
        """
        Test type casting of strings to other types
        """
        fd, path = mkstemp()
        config = Config(path=path)
        config.set("my_val_exist", 1)

        assert config.exists("my_val_exist") is True
        assert config.exists("my_val_not_exist") is False
