import pytest
import os


def test_PullPush_pull_rawdata(fixt_setup):
    assert 11 == len(fixt_setup.group_niis)
    anat_path = fixt_setup.group_niis[0]
    fmap_path = fixt_setup.group_niis[1]
    bold_path = fixt_setup.group_niis[-1]

    # Check anat
    subj, sess, suff = anat_path.split("anat/")[1].split("_")
    assert subj == fixt_setup.subj
    assert sess == fixt_setup.sess
    assert "T1w.nii.gz" == suff

    # Check fmap
    subj, sess, acq, dirc, suff = fmap_path.split("fmap/")[1].split("_")
    assert subj == fixt_setup.subj
    assert sess == fixt_setup.sess
    assert "acq-rpe" == acq
    assert "dir-PA" == dirc
    assert "epi.nii.gz" == suff

    # Check fmap
    subj, sess, task, run, suff = bold_path.split("func/")[1].split("_")
    assert subj == fixt_setup.subj
    assert sess == fixt_setup.sess
    assert "task-scenarios" == task
    assert "run-08" == run
    assert "bold.nii.gz" == suff


@pytest.mark.preproc
def test_FslCmds(fixt_afni_fsl):
    # Check temporal mean command
    cmd_tmean = fixt_afni_fsl.afni_fsl._cmd_tmean("in_epi", "out_path")
    cmd_tmean_list = cmd_tmean.split()
    chk_tmean = [
        "fslmaths",
        "in_epi",
        "-Tmean",
        "out_path",
    ]
    assert chk_tmean == cmd_tmean_list

    # Check bandpass command
    cmd_tfilt = fixt_afni_fsl.afni_fsl._cmd_tfilt(
        "in_epi", "out_path", 25, "in_tmean"
    )
    cmd_tfilt_list = cmd_tfilt.split()
    chk_tfilt = [
        "fslmaths",
        "in_epi",
        "-bptf",
        "25",
        "-1",
        "-add",
        "in_tmean",
        "out_path",
    ]
    assert chk_tfilt == cmd_tfilt_list

    # Chekc scale command
    cmd_scale = fixt_afni_fsl.afni_fsl._cmd_scale(
        "in_epi", "out_path", fixt_afni_fsl.med_value
    )
    cmd_scale_list = cmd_scale.split()
    chk_scale = [
        "fslmaths",
        "in_epi",
        "-mul",
        "24.228878",
        "out_path",
    ]
    assert chk_scale == cmd_scale_list


@pytest.mark.preproc
class TestHelperMeths:

    @pytest.fixture(autouse=True)
    def _get_fixts(self, fixt_setup, fixt_afni_fsl):
        self.fixt_setup = fixt_setup
        self.fixt_afni_fsl = fixt_afni_fsl

    def test_chk_path(self):
        fake_path = os.path.join(
            os.path.dirname(self.fixt_afni_fsl.out_scaled), "fake_dir"
        )
        with pytest.raises(FileNotFoundError):
            self.fixt_afni_fsl.afni_fsl._chk_path(fake_path)

    def test_set_subj(self):
        assert self.fixt_afni_fsl.afni_fsl.subj == self.fixt_setup.subj
        assert self.fixt_afni_fsl.afni_fsl.out_dir == os.path.join(
            self.fixt_setup.work_dir,
            "fsl_denoise",
            self.fixt_setup.subj,
            self.fixt_setup.sess,
            "func",
        )

    def test_submit_check(self):
        # Run submission and check return
        assert self.fixt_afni_fsl.afni_fsl._submit_check(
            "echo foo", self.fixt_afni_fsl.out_scaled, "foo"
        )

        # Check generated file location and content
        log_err = os.path.join(self.fixt_setup.log_dir, "err_foo.log")
        log_out = os.path.join(self.fixt_setup.log_dir, "out_foo.log")
        for chk_log in [log_err, log_out]:
            assert os.path.exists(chk_log)
        with open(log_out, "r") as lf:
            log_content = lf.readlines()
        assert ["foo\n"] == log_content

    def test_parse_epi(self):
        subj, sess, task, run, space, res, desc, suff = (
            self.fixt_afni_fsl.afni_fsl._parse_epi(
                self.fixt_afni_fsl.out_scaled
            )
        )
        assert subj == self.fixt_setup.subj
        assert sess == self.fixt_setup.sess
        assert "task-rest" == task
        assert "run-01" == run
        assert "space-MNI152NLin6Asym" == space
        assert "res-2" == res
        assert "desc-scaled" == desc
        assert "bold.nii.gz" == suff

    def test_job_name(self):
        job_name = self.fixt_afni_fsl.afni_fsl._job_name(
            self.fixt_afni_fsl.out_scaled, "foo"
        )
        assert (
            f"{self.fixt_setup.subj[-4:]}_"
            + f"{self.fixt_setup.sess.split('-')[-1]}"
            + "_r_r1_foo"
            == job_name
        )

    def test_get_out_path(self):
        out_path = self.fixt_afni_fsl.afni_fsl._get_out_path(
            self.fixt_afni_fsl.out_scaled, "desc-foo"
        )
        assert (
            self.fixt_afni_fsl.out_scaled.replace("desc-scaled", "desc-foo")
            == out_path
        )


@pytest.mark.preproc
def test_FslMethods(fixt_setup, fixt_afni_fsl):
    # init
    assert fixt_afni_fsl.afni_fsl._log_dir == fixt_setup.log_dir

    # Methods - check output desc fields, could be more robust.
    assert "tmean" == fixt_afni_fsl.run_tmean.split("desc-")[1].split("_")[0]
    assert (
        "tfilt" == fixt_afni_fsl.run_bandpass.split("desc-")[1].split("_")[0]
    )
    assert (
        "ScaleNoMask"
        == fixt_afni_fsl.run_scaled.split("desc-")[1].split("_")[0]
    )
    assert 412.730621 == fixt_afni_fsl.med_value


@pytest.mark.preproc
class TestAfniCmds:

    @pytest.fixture(autouse=True)
    def _get_fixts(self, fixt_setup, fixt_afni_fsl):
        self.fixt_setup = fixt_setup
        self.fixt_afni_fsl = fixt_afni_fsl

    def test_prepend_afni(self):
        afni_head = self.fixt_afni_fsl.afni_fsl._prepend_afni()
        out_dir = os.path.join(
            self.fixt_setup.work_dir,
            "fsl_denoise",
            self.fixt_setup.subj,
            self.fixt_setup.sess,
            "func",
        )
        chk_head = [
            "singularity",
            "run",
            "--cleanenv",
            f"--bind {out_dir}:{out_dir}",
            f"--bind {out_dir}:/opt/home",
            os.environ["SING_AFNI"],
        ]
        assert chk_head == afni_head

    def test_cmd_mask_epi(self):
        cmd_mask = self.fixt_afni_fsl.afni_fsl._cmd_mask_epi(
            "in_epi", "mask_path", "work_mask", "out_path"
        )
        for chk_str in [
            "cp mask_path work_mask ;",
            "3dcalc",
            "-a in_epi",
            "-b work_mask",
            "-float",
            "-prefix out_path",
            "-expr 'a*step(b)'",
        ]:
            assert chk_str in cmd_mask

    def test_cmd_smooth(self):
        cmd_smooth = self.fixt_afni_fsl.afni_fsl._cmd_smooth(
            "in_epi", 4, "out_path"
        )
        for chk_str in [
            "3dmerge",
            "-1blur_fwhm 4",
            "-doall",
            "-prefix out_path",
            "in_epi",
        ]:
            assert chk_str in cmd_smooth


@pytest.mark.preproc
def test_AfniMethods(fixt_afni_fsl):
    # Methods - checks output desc fields, could be more robust.
    assert "scaled" == fixt_afni_fsl.out_scaled.split("desc-")[1].split("_")[0]
    assert (
        "smoothed" == fixt_afni_fsl.out_smooth.split("desc-")[1].split("_")[0]
    )
    assert (
        "SmoothNoMask"
        == fixt_afni_fsl.run_smooth.split("desc-")[1].split("_")[0]
    )


@pytest.mark.preproc
def test_ExtraPreproc(fixt_afni_fsl):
    # Required methods in preprocess
    assert hasattr(fixt_afni_fsl.afni_fsl, "tmean")
    assert hasattr(fixt_afni_fsl.afni_fsl, "bandpass")
    assert hasattr(fixt_afni_fsl.afni_fsl, "median")
    assert hasattr(fixt_afni_fsl.afni_fsl, "scale")
    assert hasattr(fixt_afni_fsl.afni_fsl, "smooth")
    assert hasattr(fixt_afni_fsl.afni_fsl, "mask_epi")
