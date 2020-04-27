from datetime import timedelta

import pytest

import faktory
from faktory.utilities.configuration import set_temporary_config


def test_set_temporary_config_is_temporary():
    # without this setting, the tasks will error because they have max_retries but no
    # retry delay
    with set_temporary_config({"worker.concurrency": 3}):
        with set_temporary_config({"worker.concurrency": 5}):
            with set_temporary_config({"worker.concurrency": 1}):
                assert faktory.config.worker.concurrency == 1

            assert faktory.config.worker.concurrency == 5
        assert faktory.config.worker.concurrency == 3


def test_set_temporary_config_can_invent_new_settings():
    with set_temporary_config({"worker.nested.nested_again.val": "5"}):
        assert faktory.config.worker.nested.nested_again.val == "5"

    with pytest.raises(AttributeError):
        assert faktory.config.worker.nested.nested_again.val == "5"


def test_set_temporary_config_with_multiple_keys():
    with set_temporary_config({"x.y.z": 1, "a.b.c": 2}):
        assert faktory.config.x.y.z == 1
        assert faktory.config.a.b.c == 2
