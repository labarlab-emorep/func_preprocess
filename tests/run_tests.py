"""Conduct user-requested unit and integration tests.

Tests are conducted in "/work/$(whoami)/EmoRep/tests" which is
removed if all requested tests pass.

Examples
--------
python run_tests.py --all
python run_tests.py --preproc
python run_tests.py --no-mark
python run_tests.py --integ

"""

import sys
import subprocess as sp
from argparse import ArgumentParser, RawTextHelpFormatter
import func_preprocess._version as ver


# %%
def get_args():
    """Get and parse arguments."""
    ver_info = f"\nVersion : {ver.__version__}\n\n"
    parser = ArgumentParser(
        description=ver_info + __doc__, formatter_class=RawTextHelpFormatter
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Conduct all unit and integration tests",
    )
    parser.add_argument(
        "--integ",
        action="store_true",
        help="Conduct marked itegration tests",
    )
    parser.add_argument(
        "--preproc",
        action="store_true",
        help="Conduct marked unit tests for preprocessing workflow",
    )
    parser.add_argument(
        "--no-mark",
        action="store_true",
        help="Conduct un-marked unit tests",
    )

    if len(sys.argv) <= 1:
        parser.print_help(sys.stderr)
        sys.exit(0)

    return parser


def _submit_pytest(pytest_opts: list = None):
    """Submit subprocess pytest."""
    pytest_cmd = ["python", "-m pytest", "-vv"]
    if pytest_opts:
        pytest_cmd = pytest_cmd + pytest_opts
    h_sp = sp.Popen(" ".join(pytest_cmd), shell=True)
    job_out, job_err = h_sp.communicate()
    h_sp.wait()


# %%
def main():
    """Coordinate module resources."""
    args = get_args().parse_args()

    if args.all:
        _submit_pytest()
    if args.no_mark:
        _submit_pytest(
            pytest_opts=[
                "-m ",
                "'",
                "not preproc",
                "and not integ",
                "'",
            ]
        )
    if args.preproc:
        _submit_pytest(pytest_opts=["-m preproc"])
    if args.integ:
        _submit_pytest(pytest_opts=["-m integ"])


if __name__ == "__main__":
    main()
