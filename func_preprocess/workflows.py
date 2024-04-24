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
    sing_fmriprep,
    tplflow_dir,
    fs_license,
    fd_thresh,
    ignore_fmaps,
    sing_afni,
    log_dir,
    run_local,
    user_name=None,
    rsa_key=None,
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
    sing_fmriprep : str, os.PathLike
        Location of fmiprep singularity image
    tplflow_dir : str, os.PathLike
        Clone location of templateflow
    fs_license : str, os.PathLike
        Location of FreeSurfer license
    fd_thresh : float
        Threshold for framewise displacement
    ignore_fmaps : bool
        Whether to incorporate fmaps in preprocessing
    sing_afni : str, os.PathLike
        Location of afni singularity iamge
    log_dir : str, os.PathLike
        Location for writing logs
    run_local : bool
        Whether job, subprocesses are run locally
    user_name : str, optional
        User name for DCC, labarserv2
    rsa_key : str, os.PathLike, optional
        Location of RSA key for labarserv2
    keoki_path : str, os.PathLike, optional
        Location of project directory on Keoki
    test_mode : bool, optional
        Used to avoid data push, cleanup during testing

    Raises
    ------
    TypeError
        Unexpected types for passed args

    """
    # Check passed args
    if not isinstance(sess_list, list):
        raise TypeError("Expected type list for sess_list")
    for _chk_bool in [ignore_fmaps, run_local]:
        if not isinstance(_chk_bool, bool):
            raise TypeError(
                "Expected bool type for options : --ignore_fmaps, "
                + "--run_local"
            )
    if not isinstance(fd_thresh, float):
        raise TypeError("Expected float type for --fd_thresh")
    if (not run_local and user_name is None) or (
        not run_local and rsa_key is None
    ):
        raise ValueError("user name and rsa key required on DCC")

    # Download needed files
    if not run_local:
        sync_data = helper_tools.PullPush(
            os.path.dirname(proj_raw), log_dir, user_name, rsa_key, keoki_path
        )
        for sess in sess_list:
            _ = sync_data.pull_rawdata(subj, sess)

    # Run FreeSurfer, fMRIPrep
    run_fs = preprocess.RunFreeSurfer(
        subj, proj_raw, work_deriv, log_dir, run_local
    )
    run_fs.recon_all(sess_list)
    run_fp = preprocess.RunFmriprep(
        subj,
        proj_raw,
        work_deriv,
        sing_fmriprep,
        tplflow_dir,
        fs_license,
        fd_thresh,
        ignore_fmaps,
        log_dir,
        run_local,
    )
    run_fp.fmriprep(sess_list)
    fp_dict = run_fp.get_output()

    # Finish preprocessing with FSL, AFNI
    _ = preprocess.fsl_preproc(
        work_deriv,
        fp_dict,
        sing_afni,
        subj,
        log_dir,
        run_local,
    )

    # Clean up only dcc
    if run_local:
        return

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
