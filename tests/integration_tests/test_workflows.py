import pytest
import os
import subprocess
from typing import Iterator, Union
import glob
from func_preprocess import workflows


def _submit_bash(cmd: str):
    """Submit bash subprocess."""
    job_sp = subprocess.Popen(
        cmd,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    job_sp.communicate()
    job_sp.wait()


class IntegTestVars:
    """Allow each fixture to add respective attrs."""

    pass


@pytest.fixture(scope="module")
def fixt_wf_setup(
    fixt_setup, fixt_freesurfer, fixt_fmriprep
) -> Iterator[IntegTestVars]:
    """Run workflows.run_preproc."""

    # Setup work dir for integration tests, to avoid conflicts
    # with unit tests.
    work_integ_dir = os.path.join(fixt_setup.test_dir, "work_integ")
    work_integ_fp = os.path.join(
        work_integ_dir,
        "fmriprep",
        fixt_setup.sess,
    )
    subj_fp = os.path.join(work_integ_fp, fixt_setup.subj, fixt_setup.sess)
    subj_fp_func = os.path.join(subj_fp, "func")
    if not os.path.exists(subj_fp_func):
        os.makedirs(subj_fp_func)

    # Copy freesurfer and fmriprep anat, fmap, func/run-01
    _submit_bash(f"cp -r {fixt_setup.work_dir}/freesurfer {work_integ_dir}")
    _submit_bash(
        f"""\
            cp \
            {fixt_fmriprep.subj_fp}/{fixt_setup.sess}/func/*task-rest_run-01* \
            {subj_fp_func}
        """
    )
    _submit_bash(
        f"cp -r {fixt_fmriprep.subj_fp}/{fixt_setup.sess}/anat {subj_fp}"
    )
    _submit_bash(
        f"cp -r {fixt_fmriprep.subj_fp}/{fixt_setup.sess}/fmap {subj_fp}"
    )
    _submit_bash(
        f"cp {fixt_setup.work_dir}/fmriprep/{fixt_setup.sess}/"
        + f"{fixt_setup.subj}.html {work_integ_fp}"
    )

    # Run preprocessing workflow
    workflows.run_preproc(
        fixt_setup.subj,
        [fixt_setup.sess],
        fixt_setup.group_raw,
        fixt_setup.group_deriv,
        work_integ_dir,
        0.5,
        False,
        fixt_setup.log_dir,
        test_mode=True,
    )

    # Build and yield obj
    help_wf = IntegTestVars()
    help_wf.work_integ_dir = work_integ_dir
    help_wf.group_fsl_path = os.path.join(
        fixt_setup.group_deriv,
        "fsl_denoise",
    )
    help_wf.group_fs_path = os.path.join(
        fixt_setup.group_deriv,
        "freesurfer",
    )
    help_wf.group_fp_path = os.path.join(
        fixt_setup.group_deriv,
        "fmriprep",
    )
    yield help_wf


@pytest.mark.integ
def test_copy_clean(fixt_setup, fixt_wf_setup):
    # Check that work dirs were removed
    assert 0 == len(
        os.listdir(
            os.path.join(
                fixt_wf_setup.work_integ_dir,
                "fmriprep",
                fixt_setup.sess,
                fixt_setup.subj,
            )
        )
    )
    assert 0 == len(
        os.listdir(
            os.path.join(
                fixt_wf_setup.work_integ_dir, "freesurfer", fixt_setup.sess
            )
        )
    )
    assert 0 == len(
        os.listdir(os.path.join(fixt_wf_setup.work_integ_dir, "fsl_denoise"))
    )

    # Check for output in group freesurfer, fmriprep, fsl_denoise dirs
    assert 1 == len(
        os.listdir(
            os.path.join(
                fixt_wf_setup.group_fs_path,
                fixt_setup.sess,
                fixt_setup.subj,
                "mri/orig",
            )
        )
    )
    assert 40 == len(
        os.listdir(
            os.path.join(
                fixt_wf_setup.group_fp_path,
                fixt_setup.subj,
                fixt_setup.sess,
                "anat",
            )
        )
    )
    assert 2 == len(
        os.listdir(
            os.path.join(
                fixt_wf_setup.group_fsl_path,
                fixt_setup.subj,
                fixt_setup.sess,
                "func",
            )
        )
    )


@pytest.mark.integ
class Test_run_preproc:

    @pytest.fixture(autouse=True)
    def _get_fixts(self, fixt_setup, fixt_wf_setup):
        """Set fixtures as attrs."""
        self.fixt_setup = fixt_setup
        self.fixt_wf_setup = fixt_wf_setup

    @property
    def _fsl_path(self) -> Union[str, os.PathLike]:
        """Return path to FSL subj/sess/func dir."""
        return os.path.join(
            self.fixt_wf_setup.group_fsl_path,
            self.fixt_setup.subj,
            self.fixt_setup.sess,
            "func",
        )

    @property
    def _fs_path(self) -> Union[str, os.PathLike]:
        """Return path to freesurfer sess/subj/mri dir."""
        return os.path.join(
            self.fixt_wf_setup.group_fs_path,
            self.fixt_setup.sess,
            self.fixt_setup.subj,
            "mri",
        )

    def test_fsl_output(self):
        nii_list = sorted(glob.glob(f"{self._fsl_path}/*_bold.nii.gz"))
        assert "desc-scaled" in os.path.basename(nii_list[0])
        assert "desc-smoothed" in os.path.basename(nii_list[1])

    def test_fs_output(self):
        assert os.path.exists(os.path.join(self._fs_path, "aparc+aseg.mgz"))
        assert os.path.exists(os.path.join(self._fs_path, "orig/001.mgz"))

    def test_fp_output(self):
        assert os.path.exists(
            os.path.join(
                self.fixt_wf_setup.group_fp_path,
                f"{self.fixt_setup.subj}_{self.fixt_setup.sess}.html",
            )
        )
