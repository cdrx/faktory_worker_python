"""
Utilities for interacting with Faktory configuration. These are only intended
to be used for testing.
"""
from contextlib import contextmanager
from typing import Iterator

import faktory
from faktory.configuration import Config


@contextmanager
def set_temporary_config(temp_config: dict) -> Iterator:
    """
    Temporarily sets configuration values for the duration of the context manager.
    Args:
        - temp_config (dict): a dictionary containing (possibly nested) configuration keys and values.
            Nested configuration keys should be supplied as `.`-delimited strings.
    Example:
        ```python
        with set_temporary_config({'setting': 1, 'nested.setting': 2}):
            assert faktory.config.setting == 1
            assert faktory.config.nested.setting == 2
        ```
    """
    try:
        old_config = faktory.config.copy()

        for key, value in temp_config.items():
            # the `key` might be a dot-delimited string, so we split on "." and set the value
            cfg = faktory.config
            subkeys = key.split(".")
            for subkey in subkeys[:-1]:
                cfg = cfg.setdefault(subkey, Config())
            cfg[subkeys[-1]] = value

        yield faktory.config

    finally:
        faktory.config.clear()
        faktory.config.update(old_config)
