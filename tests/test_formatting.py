import os
import shlex
import subprocess
import sys

import pytest

pytestmark = pytest.mark.formatting


FILE_ENDING = os.path.join("tests", "test_formatting.py")
POSIX = sys.platform != "win32"


@pytest.mark.skipif(sys.version_info < (3, 6), reason="Black requires Python 3.6+")
def test_source_code_black_formatting():
    # make sure we know what working directory we're in
    assert __file__.endswith(FILE_ENDING)

    faktory_dir = os.path.dirname(os.path.dirname(__file__))
    result = subprocess.call(
        shlex.split("black --check {}".format(faktory_dir), posix=POSIX)
    )
    assert result == 0, "Faktory repo did not pass Black formatting!"
