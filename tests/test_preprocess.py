import pytest
import os
import json


@pytest.mark.preproc
def test_RunFreeSurfer_setup(fixt_setup, fixt_freesurfer):
    # Check work dir paths
    file_path = fixt_freesurfer.mgz_path.split("freesurfer/")[1]
    sess, subj, mri, orig, file_name = file_path.split("/")
    assert sess == fixt_setup.sess
    assert subj == fixt_setup.subj
    assert "orig" == orig
    assert "mri" == mri
    assert "001.mgz" == file_name

    # Check group dir
    fs_group = os.path.join(
        fixt_setup.group_deriv,
        "pre_processing/freesurfer",
        fixt_setup.sess,
        fixt_setup.subj,
    )
    assert os.path.exists(fs_group)


@pytest.mark.preproc
class TestRunFmriprep:

    @pytest.fixture(autouse=True)
    def _get_fixts(self, fixt_setup, fixt_fmriprep):
        self.fixt_setup = fixt_setup
        self.fixt_fmriprep = fixt_fmriprep

    def test_write_filter(self):
        self.fixt_fmriprep.run_fp._write_filter()
        with open(self.fixt_fmriprep.run_fp._json_filt) as jf:
            filt_dict = json.load(jf)
        assert "day2" == filt_dict["bold"]["session"]
        assert "anat" == filt_dict["t1w"]["datatype"]
        assert "epi" == filt_dict["fmap"]["suffix"]

    def test_write_fmriprep_bindings(self):
        fp_cmd = self.fixt_fmriprep.run_fp._write_fmriprep()
        assert "singularity run" in fp_cmd
        assert "--cleanenv" in fp_cmd
        assert (
            f"--bind {self.fixt_setup.group_raw}:{self.fixt_setup.group_raw}"
            in fp_cmd
        )
        assert (
            f"--bind {self.fixt_setup.log_dir}:{self.fixt_setup.log_dir}"
            in fp_cmd
        )
        assert (
            f"--bind {self.fixt_setup.work_dir}:{self.fixt_setup.work_dir}"
            in fp_cmd
        )
        assert (
            f"--bind {os.environ['SINGULARITYENV_TEMPLATEFLOW_HOME']}:"
            + f"{os.environ['SINGULARITYENV_TEMPLATEFLOW_HOME']}"
            in fp_cmd
        )
        fs_lic_dir = os.path.dirname(os.environ["FS_LICENSE"])
        assert f"--bind {fs_lic_dir}:{fs_lic_dir}" in fp_cmd
        assert f"--bind {self.fixt_setup.group_raw}:/data" in fp_cmd
        assert (
            f"--bind {self.fixt_setup.work_dir}/fmriprep/"
            + f"{self.fixt_setup.sess}:/out"
            in fp_cmd
        )
        assert (
            f"{os.environ['SING_FMRIPREP']} /data /out participant" in fp_cmd
        )

    def test_write_fmriprep_opts(self):
        fp_cmd = self.fixt_fmriprep.run_fp._write_fmriprep()
        assert "--ignore fieldmaps" in fp_cmd
        assert f"--participant-label {self.fixt_setup.subj[4:]}" in fp_cmd
        assert "--skull-strip-template MNI152NLin6Asym" in fp_cmd
        assert "--output-spaces MNI152NLin6Asym:res-2" in fp_cmd
        assert f"--fs-license {os.environ['FS_LICENSE']}" in fp_cmd
        assert "--use-aroma" in fp_cmd
        assert "--fd-spike-threshold 0.5" in fp_cmd
        assert "--skip-bids-validation" in fp_cmd
        assert "--nthreads 10 --omp-nthreads 8" in fp_cmd

    def test_get_output_paths(self):
        # Check dict organization
        assert ["preproc_bold", "mask_bold"] == list(
            self.fixt_fmriprep.fp_dict.keys()
        )
        assert 9 == len(self.fixt_fmriprep.fp_dict["preproc_bold"])
        assert 9 == len(self.fixt_fmriprep.fp_dict["mask_bold"])

        # Check file path -- note double session in path
        bold_path = self.fixt_fmriprep.fp_dict["preproc_bold"][0].split(
            "func/"
        )[0]
        bold_org = bold_path.split("fmriprep/")[1]
        sess, subj, sess2, _ = bold_org.split("/")
        assert sess == self.fixt_setup.sess
        assert subj == self.fixt_setup.subj
        assert sess2 == self.fixt_setup.sess

    def test_get_output_preproc_filename(self):
        bold_file = self.fixt_fmriprep.fp_dict["preproc_bold"][0].split(
            "func/"
        )[1]
        subj, sess, task, run, space, res, desc, suff = bold_file.split("_")
        assert subj == self.fixt_setup.subj
        assert sess == self.fixt_setup.sess
        assert "task-rest" == task
        assert "space-MNI152NLin6Asym" == space
        assert "res-2" == res
        assert "desc-preproc" == desc
        assert "bold.nii.gz" == suff

    def test_get_output_mask_filename(self):
        bold_file = self.fixt_fmriprep.fp_dict["mask_bold"][0].split("func/")[
            1
        ]
        subj, sess, task, run, space, res, desc, suff = bold_file.split("_")
        assert subj == self.fixt_setup.subj
        assert sess == self.fixt_setup.sess
        assert "task-rest" == task
        assert "space-MNI152NLin6Asym" == space
        assert "res-2" == res
        assert "desc-brain" == desc
        assert "mask.nii.gz" == suff


@pytest.mark.preproc
def test_fsl_preproc_paths_names(fixt_setup, fixt_fsl_preproc):
    # Check output path
    file_path, file_name = fixt_fsl_preproc.scaled_list[0].split("func/")
    file_org = file_path.split("tests/work/")[1]
    deriv, subj, sess, _ = file_org.split("/")
    assert "fsl_denoise" == deriv
    assert subj == fixt_setup.subj
    assert sess == fixt_setup.sess

    # Check file name
    subj, sess, task, run, space, res, desc, suff = file_name.split("_")
    assert subj == fixt_setup.subj
    assert sess == fixt_setup.sess
    assert "task-rest" == task
    assert "run-01" == run
    assert "space-MNI152NLin6Asym" == space
    assert "res-2" == res
    assert "desc-scaled" == desc
    assert "bold.nii.gz" == suff
