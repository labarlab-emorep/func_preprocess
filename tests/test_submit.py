import pytest
import os
import sys
from func_preprocess import submit


def test_submit_subprocess_local(fixt_setup):
    job_out, job_err = submit.submit_subprocess(
        True, "echo foo local", "foo_sub_local", fixt_setup.log_dir
    )
    assert "foo local\n" == job_out.decode("utf-8")


def test_submit_subprocess_sched(fixt_setup):
    job_out, job_err = submit.submit_subprocess(
        False, "echo foo sched", "foo_sub_sched", fixt_setup.log_dir
    )
    assert os.path.exists(
        os.path.join(fixt_setup.log_dir, "err_foo_sub_sched.log")
    )
    out_file = os.path.join(fixt_setup.log_dir, "out_foo_sub_sched.log")
    assert os.path.exists(out_file)
    with open(out_file, "r") as of:
        line = of.read()
    assert "foo sched\n" == line


class Test_schedule_subj:

    @pytest.fixture(autouse=True)
    def _get_fixts(self, fixt_setup):
        """Make, set fixtures as attrs."""
        self.fixt_setup = fixt_setup

        # Write a preproc script
        submit.schedule_subj(
            fixt_setup.subj,
            [fixt_setup.sess],
            fixt_setup.group_raw,
            fixt_setup.group_deriv,
            fixt_setup.work_dir,
            os.environ["SING_FMRIPREP"],
            os.environ["SINGULARITYENV_TEMPLATEFLOW_HOME"],
            os.environ["FS_LICENSE"],
            0.5,
            True,
            os.environ["SING_AFNI"],
            fixt_setup.log_dir,
            False,
            os.environ["USER"],
            os.environ["RSA_LS2"],
            schedule_job=False,
        )
        py_script = os.path.join(
            fixt_setup.log_dir, f"run_preprocess_{fixt_setup.subj}.py"
        )

        # Get info from preproc script
        with open(py_script, "r") as pf:
            lines = pf.readlines()
        self.lines = [x.strip() for x in lines]
        self.subjid = self.fixt_setup.subj.split("-")[1]

    @property
    def _head_opts(self) -> dict:
        """Return {line num: SBATCH command}."""
        return {
            0: f"#!/bin/env {sys.executable}",
            2: f"#SBATCH --job-name=p{self.subjid}",
            3: f"#SBATCH --output={self.fixt_setup.log_dir}/"
            + f"par{self.subjid}.txt",
            4: "#SBATCH --time=60:00:00",
            5: "#SBATCH --cpus-per-task=4",
            6: "#SBATCH --mem-per-cpu=6G",
            8: "import os",
            9: "import sys",
            10: "from func_preprocess import workflows",
        }

    @property
    def _body_opts(self) -> dict:
        """Return {line num: pipeline argument}."""
        # More options could be tested, these
        # were easiest to avoid str issues.
        return {
            12: "workflows.run_preproc(",
            21: "0.5,",
            22: "True,",
            25: "False,",
            28: ")",
        }

    def test_script_head(self):
        for key, value in self._head_opts.items():
            assert self.lines[key] == value

    def test_script_body(self):
        for key, value in self._body_opts.items():
            assert self.lines[key] == value
