"""Helper methods for unit and integration tests.

check_test_env : Check for required resources and environment

"""

import os
import platform
from typing import Union


def test_dir() -> Union[str, os.PathLike]:
    """Return path to project testing directory."""
    return f"/work/{os.environ['USER']}/EmoRep/tests"


def check_test_env():
    """Raise EnvironmentError for improper testing envs."""
    # Check for DCC
    if "dcc" not in platform.uname().node:
        raise EnvironmentError("Please execute pytest on DCC")

    # Check for scheduled resources
    msg_rsc = (
        "Please execute pytest with scheduled resources : "
        + "--cpus-per-task 5 --mem 24G"
    )
    try:
        num_cpu = os.environ["SLURM_CPUS_ON_NODE"]
        num_ram = os.environ["SLURM_MEM_PER_NODE"]
        if num_cpu != "5" or num_ram != "24576":
            raise EnvironmentError(msg_rsc)
    except KeyError:
        raise EnvironmentError(msg_rsc)

    # Check for EmoRep env
    msg_nat = "Please execute pytest in emorep conda env"
    try:
        conda_env = os.environ["CONDA_DEFAULT_ENV"]
        if "emorep" not in conda_env:
            raise EnvironmentError(msg_nat)
    except KeyError:
        raise EnvironmentError(msg_nat)

    # Check for required global vars
    os.environ["RSA_LS2"]
    os.environ["SING_FMRIPREP"]
    os.environ["SINGULARITYENV_TEMPLATEFLOW_HOME"]
    os.environ["FS_LICENSE"]
