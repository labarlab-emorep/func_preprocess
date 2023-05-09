"""Pipeline workflows for EmoRep fMRI data."""
import os
from func_preprocess import preprocess, helper_tools


def run_preproc(
    subj,
    proj_raw,
    proj_deriv,
    work_deriv,
    sing_fmriprep,
    tplflow_dir,
    fs_license,
    fd_thresh,
    ignore_fmaps,
    no_freesurfer,
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
    no_freesurfer : bool
        Whether to use the --fs-no-reconall option
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
        Unexpected types for bool and float args

    """
    # Check types
    for _chk_bool in [ignore_fmaps, no_freesurfer, run_local]:
        if not isinstance(_chk_bool, bool):
            raise TypeError(
                "Expected bool type for options : --ignore_fmaps, "
                + "--no_freesurfer, --run_local"
            )
    if not isinstance(fd_thresh, float):
        raise TypeError("Expected float type for --fd_thresh")
    if (not run_local and user_name is None) or (
        not run_local and rsa_key is None
    ):
        raise ValueError("user name and rsa key required on DCC")

    # Setup software derivatives dirs, for working
    work_fp = os.path.join(work_deriv, "fmriprep")
    work_fs = os.path.join(work_deriv, "freesurfer")
    work_fsl = os.path.join(work_deriv, "fsl_denoise")
    for h_dir in [work_fp, work_fs, work_fsl]:
        if not os.path.exists(h_dir):
            os.makedirs(h_dir)

    # Setup software derivatives dirs, for storage
    proj_fp = os.path.join(proj_deriv, "fmriprep")
    proj_fsl = os.path.join(proj_deriv, "fsl_denoise")
    for h_dir in [proj_fp, proj_fsl]:
        if not os.path.exists(h_dir):
            os.makedirs(h_dir)

    # Download needed files
    if not run_local:
        sync_data = helper_tools.PullPush(
            os.path.dirname(proj_raw), log_dir, user_name, rsa_key
        )
        sync_data.pull_rawdata(subj, "ses-day2")
        sync_data.pull_rawdata(subj, "ses-day3")

    # Run fMRIPrep
    fp_dict = preprocess.fmriprep(
        subj,
        proj_raw,
        work_deriv,
        sing_fmriprep,
        tplflow_dir,
        fs_license,
        fd_thresh,
        ignore_fmaps,
        no_freesurfer,
        log_dir,
        run_local,
    )

    # Finish preprocessing with FSL, AFNI
    _ = preprocess.fsl_preproc(
        work_fsl,
        fp_dict,
        sing_afni,
        subj,
        log_dir,
        run_local,
    )

    # Clean up
    if not run_local:
        preprocess.copy_clean(
            proj_deriv,
            work_deriv,
            subj,
            no_freesurfer,
            log_dir,
        )
        sync_data.sess = "ses-all"
        sync_data.push_derivatives()
