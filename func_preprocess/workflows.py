"""Workflow for EmoRep fMRI preprocessing.

run_preproc : coordinate methods for preprocessing workflow

"""

import os
import shutil
from func_preprocess import preprocess, helper_tools


def run_preproc(
    subj,
    sess_list,
    proj_raw,
    proj_deriv,
    work_deriv,
    fd_thresh,
    ignore_fmaps,
    log_dir,
    keoki_path="/mnt/keoki/experiments2/EmoRep/Exp2_Compute_Emotion/data_scanner_BIDS",  # noqa: E501
    test_mode=False,
):
    """Functional preprocessing pipeline for EmoRep.

    Parameters
    ----------
    subj : str
        BIDS subject identifier
    sess_list : list
        BIDS session identifiers
    proj_raw : str, os.PathLike
        Location of project rawdata
    proj_deriv : str, os.PathLike
        Location of project derivatives
    work_deriv : str, os.PathLike
        Location of work derivatives
    fd_thresh : float
        Threshold for framewise displacement
    ignore_fmaps : bool
        Whether to incorporate fmaps in preprocessing
    log_dir : str, os.PathLike
        Location for writing logs
    keoki_path : str, os.PathLike, optional
        Location of project directory on Keoki
    test_mode : bool, optional
        Used to avoid data push, cleanup during testing

    Raises
    ------
    TypeError
        Unexpected types for passed args

    """
    # Check args
    if not isinstance(sess_list, list):
        raise TypeError("Expected type 'list' for sess_list")
    if not isinstance(ignore_fmaps, bool):
        raise TypeError("Expected type 'bool' for ignore_fmaps")
    if not isinstance(fd_thresh, float):
        raise TypeError("Expected type 'float' for fd_thresh")

    # Check env
    helper_tools.check_env()

    # Download needed files
    sync_data = helper_tools.PullPush(
        os.path.dirname(proj_raw), log_dir, keoki_path
    )
    for sess in sess_list:
        _ = sync_data.pull_rawdata(subj, sess)

    # Run FreeSurfer, fMRIPrep
    run_fs = preprocess.RunFreeSurfer(subj, proj_raw, work_deriv, log_dir)
    run_fs.recon_all(sess_list)
    run_fp = preprocess.RunFmriprep(
        subj,
        proj_raw,
        work_deriv,
        fd_thresh,
        ignore_fmaps,
        log_dir,
    )
    run_fp.fmriprep(sess_list)
    fp_dict = run_fp.get_output()

    # Finish preprocessing with FSL, AFNI
    _ = preprocess.fsl_preproc(
        work_deriv,
        fp_dict,
        subj,
        log_dir,
    )

    # Copy files from work to group partition
    helper_tools.copy_clean(
        subj,
        sess_list,
        proj_deriv,
        work_deriv,
        log_dir,
    )

    # Send data to keoki and clean up
    if test_mode:
        return
    sync_data.push_derivatives(sess_list)
    shutil.rmtree(os.path.join(proj_raw, subj))
