"""Pipeline workflows for EmoRep fMRI data."""
import os
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
):
    """Functional preprocessing pipeline for EmoRep.

    Parameters
    ----------
    subj : str
        BIDS subject identifier
    sess_list : list
        BIDS session identifiers
    proj_raw : path
        Location of project rawdata, e.g.
        /hpc/group/labarlab/EmoRep_BIDS/rawdata
    proj_deriv : path
        Location of project derivatives, e.g.
        /hpc/group/labarlab/EmoRep_BIDS/derivatives
    work_deriv : path
        Location of work derivatives, e.g.
        /work/foo/EmoRep_BIDS/derivatives
    sing_fmriprep : path, str
        Location of fmiprep singularity image
    tplflow_dir : path, str
        Clone location of templateflow
    fs_license : path, str
        Location of FreeSurfer license
    fd_thresh : float
        Threshold for framewise displacement
    ignore_fmaps : bool
        Whether to incorporate fmaps in preprocessing
    sing_afni : path, str
        Location of afni singularity iamge
    log_dir : path
        Location for writing logs
    run_local : bool
        Whether job, subprocesses are run locally
    user_name : str, optional
        User name for DCC, labarserv2
    rsa_key : str, os.PathLike, optional
        Location of RSA key for labarserv2

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
            os.path.dirname(proj_raw), log_dir, user_name, rsa_key
        )
        for sess in sess_list:
            sync_data.pull_rawdata(subj, sess)

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

    # Clean up
    if not run_local:
        helper_tools.copy_clean(
            subj,
            sess_list,
            proj_deriv,
            work_deriv,
            log_dir,
        )
        sync_data.sess = "ses-all"
        sync_data.push_derivatives()
