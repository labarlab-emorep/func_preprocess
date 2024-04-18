import pytest
import os
import sys
from typing import Iterator

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import helper  # noqa: E402


class IntegTestVars:
    """Allow each fixture to add respective attrs."""

    pass


@pytest.fixture(scope="package")
def fixt_wf_setup(fixt_setup) -> Iterator[IntegTestVars]:
    pass
