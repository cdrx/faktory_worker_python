import datetime
import os
import shlex
import subprocess
import sys
import tempfile
import uuid
from concurrent.futures import Executor, ProcessPoolExecutor, ThreadPoolExecutor
from typing import List

import pytest

import faktory
from faktory import Worker, configuration
from faktory.configuration import Config
from faktory.utilities.configuration import set_temporary_config

template = b"""
    debug = false
    [general]
    x = 1
    y = "hi"
        [general.nested]
        x = "${general.x}"
        x_interpolated = "${general.x} + 1"
        y = "${general.y} or bye"
    [interpolation]
    key = "x"
    value = "${general.nested.${interpolation.key}}"
    bad_value = "${general.bad_key}"
    [env_vars]
    interpolated_path = "$PATH"
    interpolated_from_non_string_key_bool = "${env_vars.true}"
    interpolated_from_non_string_key_string = "${env_vars.true} string"
    not_interpolated_path = "xxx$PATHxxx"
    [logging]
    format = "log-format"
    [secrets]
    password = "1234"
    very_private = "000"
    [worker]
    heartbeat_seconds = 5
    concurrency = 4
    use_threads = false
    labels = ["python", "testing"]
    queues = ["testing"]
        [worker.disconnect]
        keyboard = 20
        pool_failure = 20
        server_requested = 30
    """


@pytest.fixture
def test_config_file_path():
    with tempfile.TemporaryDirectory() as test_config_dir:
        test_config_loc = os.path.join(test_config_dir, "test_config.toml")
        with open(test_config_loc, "wb") as test_config:
            test_config.write(template)
        yield test_config_loc


@pytest.fixture
def config(test_config_file_path, monkeypatch):

    monkeypatch.setenv("FAKTORY_TEST__ENV_VARS__NEW_KEY", "TEST")
    monkeypatch.setenv("FAKTORY_TEST__ENV_VARS__TWICE__NESTED__NEW_KEY", "TEST")
    monkeypatch.setenv("FAKTORY_TEST__ENV_VARS__TRUE", "true")
    monkeypatch.setenv("FAKTORY_TEST__ENV_VARS__FALSE", "false")
    monkeypatch.setenv("FAKTORY_TEST__ENV_VARS__INT", "10")
    monkeypatch.setenv("FAKTORY_TEST__ENV_VARS__NEGATIVE_INT", "-10")
    monkeypatch.setenv("FAKTORY_TEST__ENV_VARS__FLOAT", "7.5")
    monkeypatch.setenv("FAKTORY_TEST__ENV_VARS__NEGATIVE_FLOAT", "-7.5")
    monkeypatch.setenv("PATH", "1/2/3")
    monkeypatch.setenv(
        "FAKTORY_TEST__ENV_VARS__ESCAPED_CHARACTERS", r"line 1\nline 2\rand 3\tand 4"
    )

    yield configuration.load_configuration(
        test_config_file_path, env_var_prefix="FAKTORY_TEST"
    )


def test_keys(config):
    assert "debug" in config
    assert "general" in config
    assert "nested" in config.general
    assert "x" not in config


def test_getattr_missing(config):
    with pytest.raises(AttributeError, match="object has no attribute"):
        config.hello


def test_debug(config):
    assert config.debug is False


def test_general(config):
    assert config.general.x == 1
    assert config.general.y == "hi"


def test_general_nested(config):
    assert config.general.nested.x == config.general.x == 1
    assert config.general.nested.x_interpolated == "1 + 1"
    assert config.general.nested.y == "hi or bye"


def test_interpolation(config):
    assert config.interpolation.value == config.general.nested.x == 1


def test_env_var_interpolation(config):
    assert config.env_vars.interpolated_path == os.environ.get("PATH")


def test_string_to_type_function():

    assert configuration.string_to_type("true") is True
    assert configuration.string_to_type("True") is True
    assert configuration.string_to_type("TRUE") is True
    assert configuration.string_to_type("trUe") is True
    assert configuration.string_to_type("false") is False
    assert configuration.string_to_type("False") is False
    assert configuration.string_to_type("FALSE") is False
    assert configuration.string_to_type("falSe") is False

    assert configuration.string_to_type("1") == 1
    assert configuration.string_to_type("1.5") == 1.5

    assert configuration.string_to_type("-1") == -1
    assert configuration.string_to_type("-1.5") == -1.5

    assert configuration.string_to_type("x") == "x"


def test_env_var_interpolation_with_type_assignment(config):
    assert config.env_vars.true is True
    assert config.env_vars.false is False
    assert config.env_vars.int == 10
    assert config.env_vars.negative_int == -10
    assert config.env_vars.float == 7.5
    assert config.env_vars.negative_float == -7.5


def test_env_var_interpolation_with_type_interpolation(config):
    assert config.env_vars.interpolated_from_non_string_key_bool is True
    assert config.env_vars.interpolated_from_non_string_key_string == "True string"


def test_env_var_interpolation_doesnt_match_internal_dollar_sign(config):
    assert config.env_vars.not_interpolated_path == "xxx$PATHxxx"


def test_env_var_interpolation_with_nonexistant_key(config):
    assert config.interpolation.bad_value == ""


def test_env_var_overrides_new_key(config):
    assert config.env_vars.new_key == "TEST"


def test_env_var_creates_nested_keys(config):
    assert config.env_vars.twice.nested.new_key == "TEST"


def test_env_var_escaped(config):
    assert config.env_vars.escaped_characters == "line 1\nline 2\rand 3\tand 4"


def test_copy_leaves_values_mutable(config):

    config = Config(config, default_box=True)
    config.x.y.z = [1]
    new = config.copy()
    assert new.x.y.z == [1]
    new.x.y.z.append(2)
    assert config.x.y.z == [1, 2]


def test_copy_doesnt_make_keys_mutable(config):

    new = config.copy()
    new.general.z = 1
    assert "z" not in config.general


class TestUserConfig:
    def test_load_user_config(self, test_config_file_path):

        with tempfile.TemporaryDirectory() as user_config_dir:
            user_config_loc = os.path.join(user_config_dir, "test_config.toml")
            with open(user_config_loc, "wb") as user_config:
                user_config.write(
                    b"""
                    [general]
                    x = 2
                    [user]
                    foo = "bar"
                    """
                )
            config = configuration.load_configuration(
                path=test_config_file_path, user_config_path=user_config_loc
            )

            # check that user values are loaded
            assert config.general.x == 2
            assert config.user.foo == "bar"

            # check that default values are preserved
            assert config.general.y == "hi"

            # check that interpolation takes place after user config is loaded
            assert config.general.nested.x == 2


class TestConfigValidation:
    def test_invalid_keys_raise_error(self):

        with tempfile.TemporaryDirectory() as test_config_dir:
            test_config_loc = os.path.join(test_config_dir, "test_config.toml")
            with open(test_config_loc, "wb") as test_config:
                test_config.write(
                    b"""
                    [outer]
                    x = 1
                        [outer.keys]
                        a = "b"
                    """
                )

            with pytest.raises(ValueError):
                configuration.load_configuration(test_config_loc)

    def test_invalid_env_var_raises_error(self, monkeypatch):
        monkeypatch.setenv("FAKTORY_TEST__X__Y__KEYS__Z", "TEST")

        with tempfile.TemporaryDirectory() as test_config_dir:
            test_config_loc = os.path.join(test_config_dir, "test_config.toml")
            with open(test_config_loc, "wb") as test_config:
                test_config.write(b"")
            with pytest.raises(ValueError):
                configuration.load_configuration(
                    test_config_loc, env_var_prefix="FAKTORY_TEST"
                )

    def test_mixed_case_keys_are_ok(self):
        with tempfile.TemporaryDirectory() as test_config_dir:
            test_config_loc = os.path.join(test_config_dir, "test_config.toml")
            with open(test_config_loc, "wb") as test_config:
                test_config.write(
                    b"""
                    [SeCtIoN]
                    KeY = 1
                    """
                )

            config = configuration.load_configuration(test_config_loc)

        assert "KeY" in config.SeCtIoN
        assert config.SeCtIoN.KeY == 1

    def test_env_vars_are_interpolated_as_lower_case(self, monkeypatch):

        monkeypatch.setenv("FAKTORY_TEST__SECTION__KEY", "2")

        with tempfile.TemporaryDirectory() as test_config_dir:
            test_config_loc = os.path.join(test_config_dir, "test_config.toml")
            with open(test_config_loc, "wb") as test_config:
                test_config.write(
                    b"""
                    [SeCtIoN]
                    KeY = 1
                    """
                )

            config = configuration.load_configuration(
                test_config_loc, env_var_prefix="FAKTORY_TEST"
            )

        assert "KeY" in config.SeCtIoN
        assert config.SeCtIoN.KeY == 1
        assert config.section.key == 2


class TestWorkerUsesConfig:
    @pytest.mark.parametrize(
        "flag,expected", [(False, ProcessPoolExecutor), (True, ThreadPoolExecutor)]
    )
    def test_uses_executor(self, flag: bool, expected: Executor):
        with set_temporary_config({"worker.use_threads": flag}):

            worker = Worker()
            assert worker._executor_class == expected

    @pytest.mark.parametrize(
        "labels", [["python"], ["python", "testing"], ["python", "testing", "random"]]
    )
    def test_uses_labels(self, labels: List[str]):
        with set_temporary_config({"worker.labels": labels}):
            worker = Worker()
            assert worker.labels == labels

    @pytest.mark.parametrize(
        "queues", [["default"], ["default", "testing"], ["testing", "urgent"]]
    )
    def test_uses_queues(self, queues: List[str]):
        with set_temporary_config({"worker.queues": queues}):
            worker = Worker()
            assert worker._queues == queues

    @pytest.mark.parametrize("concurrency", [1, 2, 3, 4])
    def test_uses_concurrency(self, concurrency: int):
        with set_temporary_config({"worker.concurrency": concurrency}):
            worker = Worker()
            assert worker.concurrency == concurrency

    @pytest.mark.parametrize("kwargs", [{"concurrency": 5, "queues": ["tests"]}])
    def test_uses_provided_over_config(self, kwargs):
        with set_temporary_config(
            {
                "worker.concurrency": kwargs.get("concurrency"),
                "worker.queues": kwargs.get("queues"),
            }
        ):
            worker = Worker(**kwargs)
            assert worker.concurrency == kwargs.get("concurrency")
            assert worker._queues == kwargs.get("queues")
