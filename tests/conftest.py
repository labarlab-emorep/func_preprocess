import os
import shutil
import pytest
from typing import Iterator
import helper
from func_preprocess import helper_tools
from func_preprocess import preprocess


class UnitTestVars:
    """Allow each fixture to add respective attrs."""

    pass


@pytest.fixture(scope="session", autouse=True)
def fixt_setup() -> Iterator[UnitTestVars]:
    """Setup for testing and yield vars."""
    # Check for proper testing env
    helper.check_test_env()

    # Make testing dir structure
    test_dir = helper.test_dir()
    log_dir = os.path.join(test_dir, "logs")
    work_dir = os.path.join(test_dir, "work")
    group_dir = os.path.join(test_dir, "group")
    group_raw = os.path.join(group_dir, "rawdata")
    group_deriv = os.path.join(group_dir, "derivatives")
    keoki_path = (
        "/mnt/keoki/experiments2/EmoRep/Exp2_Compute_Emotion/data_scanner_BIDS"
    )
    for mk_dir in [log_dir, work_dir, group_raw, group_deriv]:
        if not os.path.exists(mk_dir):
            os.makedirs(mk_dir)

    # Set test subj, sess
    subj = "sub-ER0211"
    sess = "ses-day2"

    # Download test rawdata to group
    sync_data = helper_tools.PullPush(
        group_dir,
        log_dir,
        os.environ["USER"],
        os.environ["RSA_LS2"],
        keoki_path,
    )
    group_niis = sync_data.pull_rawdata(subj, sess)

    # Setup and yield vars
    setup_help = UnitTestVars()
    setup_help.test_dir = test_dir
    setup_help.log_dir = log_dir
    setup_help.work_dir = work_dir
    setup_help.group_raw = group_raw
    setup_help.group_deriv = group_deriv
    setup_help.group_niis = group_niis
    setup_help.sync_data = sync_data
    setup_help.subj = subj
    setup_help.sess = sess
    yield setup_help


def pytest_sessionfinish(session, exitstatus):
    """Teardown if all tests passed."""
    if 0 == exitstatus:
        shutil.rmtree(helper.test_dir())


@pytest.fixture(scope="session")
def fixt_freesurfer(fixt_setup) -> Iterator[UnitTestVars]:
    """Run freesurfer methods and yield vars."""
    # Start fs instance
    run_fs = preprocess.RunFreeSurfer(
        fixt_setup.subj,
        fixt_setup.group_raw,
        fixt_setup.work_dir,
        fixt_setup.log_dir,
        False,
    )

    # Set needed private attrs (run_fs._exec_fs), run setup
    run_fs._sess = fixt_setup.sess
    run_fs._work_fs = os.path.join(
        fixt_setup.work_dir, "freesurfer", fixt_setup.sess
    )
    mgz_path = run_fs._setup()

    # Get check file
    src = os.path.join(
        fixt_setup.sync_data._keoki_proj,
        "derivatives/pre_processing/freesurfer",
        fixt_setup.sess,
        fixt_setup.subj,
        "mri/aparc+aseg.mgz",
    )
    dst = os.path.join(run_fs._work_fs, fixt_setup.subj, "mri")
    _, _ = fixt_setup.sync_data._submit_rsync(src, dst)

    # Build and yield obj
    fs_help = UnitTestVars()
    fs_help.run_fs = run_fs
    fs_help.mgz_path = mgz_path
    yield fs_help


@pytest.fixture(scope="session")
def fixt_fmriprep(fixt_setup) -> Iterator[UnitTestVars]:
    """Run fmriprep methods and yield vars."""
    # Download fmriprep output
    dst_fp = os.path.join(
        fixt_setup.work_dir, "fmriprep", fixt_setup.sess, fixt_setup.subj
    )
    if not os.path.exists(dst_fp):
        os.makedirs(dst_fp)
    src_fp1 = os.path.join(
        fixt_setup.sync_data._keoki_proj,
        "derivatives/pre_processing/fmriprep",
        fixt_setup.subj,
        fixt_setup.sess,
    )
    src_fp2 = os.path.join(
        fixt_setup.sync_data._keoki_proj,
        "derivatives/pre_processing/fmriprep",
        f"{fixt_setup.subj}_{fixt_setup.sess}.html",
    )
    _, _ = fixt_setup.sync_data._submit_rsync(src_fp1, dst_fp)
    _, _ = fixt_setup.sync_data._submit_rsync(src_fp2, os.path.dirname(dst_fp))

    # Rename html to original
    html_path = os.path.join(
        os.path.dirname(dst_fp), f"{fixt_setup.subj}_{fixt_setup.sess}.html"
    )
    os.rename(
        html_path, html_path.replace(f"_{fixt_setup.sess}.html", ".html")
    )

    # Get fmriprep files
    run_fp = preprocess.RunFmriprep(
        fixt_setup.subj,
        fixt_setup.group_raw,
        fixt_setup.work_dir,
        os.environ["SING_FMRIPREP"],
        os.environ["SINGULARITYENV_TEMPLATEFLOW_HOME"],
        os.environ["FS_LICENSE"],
        0.5,
        True,
        fixt_setup.log_dir,
        False,
    )
    fp_dict = run_fp.get_output()

    # Set attrs because run_fp.fmriprep, run_fp._exec_fp not executed
    run_fp._sess = fixt_setup.sess
    run_fp._work_fp = os.path.join(
        fixt_setup.work_dir, "fmriprep", fixt_setup.sess
    )
    run_fp._work_fs = os.path.join(
        fixt_setup.work_dir, "freesurfer", fixt_setup.sess
    )
    run_fp._work_fp_tmp = os.path.join(
        run_fp._work_fp, "tmp_work", fixt_setup.subj
    )
    run_fp._work_fp_bids = os.path.join(run_fp._work_fp_tmp, "bids_layout")

    # Make smaller fp_dict for process testing
    small_fp_dict = {}
    for key in ["preproc_bold", "mask_bold"]:
        small_fp_dict[key] = [fp_dict[key][0]]

    # Build and yield obj
    fp_help = UnitTestVars()
    fp_help.fp_dict = fp_dict
    fp_help.small_fp_dict = small_fp_dict
    fp_help.run_fp = run_fp
    fp_help.subj_fp = dst_fp
    yield fp_help


@pytest.fixture(scope="session")
def fixt_fsl_preproc(fixt_setup, fixt_fmriprep) -> Iterator[UnitTestVars]:
    """Run fsl_preproc methods and yield vars."""
    fsl_help = UnitTestVars()
    fsl_help.scaled_list = preprocess.fsl_preproc(
        fixt_setup.work_dir,
        fixt_fmriprep.small_fp_dict,
        os.environ["SING_AFNI"],
        fixt_setup.subj,
        fixt_setup.log_dir,
        False,
    )
    yield fsl_help


@pytest.fixture(scope="session")
def fixt_afni_fsl(fixt_setup, fixt_fmriprep) -> Iterator[UnitTestVars]:
    """Run ExtraPreproc methods and yield vars."""
    # Setup for and make method instance
    out_dir = os.path.join(
        fixt_setup.work_dir,
        "fsl_denoise",
        fixt_setup.subj,
        fixt_setup.sess,
        "func",
    )
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    afni_fsl = helper_tools.ExtraPreproc(
        fixt_setup.log_dir, False, os.environ["SING_AFNI"]
    )
    afni_fsl.set_subj(fixt_setup.subj, out_dir)

    # Prep paths, prefix
    run_epi = fixt_fmriprep.small_fp_dict["preproc_bold"][0]
    run_mask = fixt_fmriprep.small_fp_dict["mask_bold"][0]

    # Use methods
    run_tmean = afni_fsl.tmean(run_epi)
    run_bandpass = afni_fsl.bandpass(run_epi, run_tmean)
    med_value = afni_fsl.median(run_bandpass, run_mask)
    run_scaled = afni_fsl.scale(
        run_bandpass, med_value, desc="desc-ScaleNoMask"
    )
    run_smooth = afni_fsl.smooth(run_scaled, 4, desc="desc-SmoothNoMask")
    out_scaled = afni_fsl.mask_epi(run_scaled, run_mask, desc="desc-scaled")
    out_smooth = afni_fsl.mask_epi(run_smooth, run_mask, desc="desc-smoothed")

    # Build and yield obj
    help_afni_fsl = UnitTestVars()
    help_afni_fsl.afni_fsl = afni_fsl
    help_afni_fsl.run_tmean = run_tmean
    help_afni_fsl.run_bandpass = run_bandpass
    help_afni_fsl.med_value = med_value
    help_afni_fsl.run_scaled = run_scaled
    help_afni_fsl.run_smooth = run_smooth
    help_afni_fsl.out_scaled = out_scaled
    help_afni_fsl.out_smooth = out_smooth
    yield help_afni_fsl
