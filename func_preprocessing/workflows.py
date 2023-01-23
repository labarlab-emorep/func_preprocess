"""Pipeline workflows for EmoRep fMRI data."""
import os
from func_preprocessing import preprocess


def run_preproc(
    subj,
    proj_raw,
    proj_deriv,
    work_deriv,
    sing_fmriprep,
    fs_license,
    fd_thresh,
    ignore_fmaps,
    no_freesurfer,
    sing_afni,
    log_dir,
    run_local,
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

    Returns
    -------
    None

    """
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

    # Run fMRIPrep
    fp_dict = preprocess.fmriprep(
        subj,
        proj_raw,
        work_deriv,
        sing_fmriprep,
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
        )
