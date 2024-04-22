import pytest
import os


@pytest.mark.integ
def test_copy_clean():
    pass


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


def test_FslMethods_init(fixt_setup, fixt_afni_fsl):
    assert fixt_afni_fsl.afni_fsl._log_dir == fixt_setup.log_dir
    assert not fixt_afni_fsl.afni_fsl._run_local


def test_FslMethods_chk_path(fixt_afni_fsl):
    fake_path = os.path.join(
        os.path.dirname(fixt_afni_fsl.out_scaled), "fake_dir"
    )
    with pytest.raises(FileNotFoundError):
        fixt_afni_fsl.afni_fsl._chk_path(fake_path)


def test_FslMethods_set_subj(fixt_setup, fixt_afni_fsl):
    assert fixt_afni_fsl.afni_fsl.subj == fixt_setup.subj
    assert fixt_afni_fsl.afni_fsl.out_dir == os.path.join(
        fixt_setup.work_dir,
        "fsl_denoise",
        fixt_setup.subj,
        fixt_setup.sess,
        "func",
    )


def test_FslMethods_submit_check(fixt_setup, fixt_afni_fsl):
    # Run submission and check return
    assert fixt_afni_fsl.afni_fsl._submit_check(
        "echo foo", fixt_afni_fsl.out_scaled, "foo"
    )

    # Check generated file location and content
    log_err = os.path.join(fixt_setup.log_dir, "err_foo.log")
    log_out = os.path.join(fixt_setup.log_dir, "out_foo.log")
    for chk_log in [log_err, log_out]:
        assert os.path.exists(chk_log)
    with open(log_out, "r") as lf:
        log_content = lf.readlines()
    assert ["foo\n"] == log_content


def test_FslMethods_parse_epi(fixt_setup, fixt_afni_fsl):
    subj, sess, task, run, space, res, desc, suff = (
        fixt_afni_fsl.afni_fsl._parse_epi(fixt_afni_fsl.out_scaled)
    )
    assert subj == fixt_setup.subj
    assert sess == fixt_setup.sess
    assert "task-rest" == task
    assert "run-01" == run
    assert "space-MNI152NLin6Asym" == space
    assert "res-2" == res
    assert "desc-scaled" == desc
    assert "bold.nii.gz" == suff


def test_FslMethods_job_name(fixt_setup, fixt_afni_fsl):
    job_name = fixt_afni_fsl.afni_fsl._job_name(
        fixt_afni_fsl.out_scaled, "foo"
    )
    assert (
        f"{fixt_setup.subj[-4:]}_{fixt_setup.sess.split('-')[-1]}"
        + "_r_r1_foo"
        == job_name
    )


def test_FslMethods_tmean(fixt_afni_fsl):
    assert "tmean" == fixt_afni_fsl.run_tmean.split("desc-")[1].split("_")[0]


def test_FslMethods_bandpass(fixt_afni_fsl):
    assert (
        "tfilt" == fixt_afni_fsl.run_bandpass.split("desc-")[1].split("_")[0]
    )


def test_FslMethods_scale(fixt_afni_fsl):
    assert (
        "ScaleNoMask"
        == fixt_afni_fsl.run_scaled.split("desc-")[1].split("_")[0]
    )


def test_FslMethods_median(fixt_afni_fsl):
    assert 412.730621 == fixt_afni_fsl.med_value


def test_AfniFslMethods_init(fixt_afni_fsl):
    assert os.environ["SING_AFNI"] == fixt_afni_fsl.afni_fsl._sing_afni


def test_AfniFslMethods_prepend_afni(fixt_setup, fixt_afni_fsl):
    afni_head = fixt_afni_fsl.afni_fsl._prepend_afni()

    out_dir = os.path.join(
        fixt_setup.work_dir,
        "fsl_denoise",
        fixt_setup.subj,
        fixt_setup.sess,
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


def test_AfniFslMethods_mask_epi(fixt_afni_fsl):
    assert "scaled" == fixt_afni_fsl.out_scaled.split("desc-")[1].split("_")[0]
    assert (
        "smoothed" == fixt_afni_fsl.out_smooth.split("desc-")[1].split("_")[0]
    )


def test_AfniFslMethods_smooth(fixt_afni_fsl):
    assert (
        "SmoothNoMask"
        == fixt_afni_fsl.run_smooth.split("desc-")[1].split("_")[0]
    )
