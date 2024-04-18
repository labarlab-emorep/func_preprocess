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
    return  # TODO remove
    if 0 == exitstatus:
        shutil.rmtree(helper.test_dir())


@pytest.fixture(scope="session")
def fixt_preproc(fixt_setup):
    """Run preproc methods and yield vars."""
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

    #
    pp_help = UnitTestVars()
    pp_help.fp_dict = fp_dict
    pp_help.run_fp = run_fp
    yield pp_help
