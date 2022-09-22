"""Preprocessing methods.

FreeSurfer (deprecated), fMRIPrep, FSL, and AFNI
software used for preprocessing EmoRep data.
"""
# %%
import os
import glob
import shutil
import itertools
from func_preprocessing import submit


# %%
def freesurfer(work_fs, subj_t1, subj, sess, log_dir):
    """Submit FreeSurfer for subject's session.

    Convert T1w NIfTI into Analyze format with FreeSurfer
    directory organization. Run FreeSurfer.

    Deprecated.

    Parameters
    ----------
    work_fs : path
        Location of freesurfer derivative directory
    subj_t1 : path
        Location, file of rawdata T1w nii
    subj : str
        BIDS subject
    sess : str
        BIDS session
    log_dir : path
        Location of output directory for writing logs

    Returns
    -------
    bool
        Whether FreeSurfer derivatives exist

    """
    fs_files = glob.glob(f"{work_fs}/**/aparc.a2009s+aseg.mgz", recursive=True)
    if not fs_files:
        bash_cmd = f"""
            mri_convert {subj_t1} {work_fs}/{subj}/mri/orig/001.mgz
            recon-all \
                -subjid {subj} \
                -all \
                -sd {work_fs} \
                -parallel \
                -openmp 6
        """
        print(f"Starting FreeSurfer for {subj}:\n\t{bash_cmd}\n")
        _, _ = submit.sbatch(
            bash_cmd,
            f"{subj[4:]}{sess[4:]}_freesurfer",
            log_dir,
            num_cpus=6,
            num_hours=10,
        )

    fs_files = glob.glob(f"{work_fs}/**/aparc.a2009s+aseg.mgz", recursive=True)
    fs_exists = True if fs_files else False
    return fs_exists


def fmriprep(
    subj,
    proj_raw,
    work_deriv,
    sing_fmriprep,
    fs_license,
    fd_thresh,
    ignore_fmaps,
    no_freesurfer,
    log_dir,
):
    """Run fMRIPrep for single subject.

    Conduct FreeSurfer and fMRIPrep routines on a subject's
    data. References the MNI152NLin6Asym space for AROMA.

    Parameters
    ----------
    subj : str
        BIDS subject
    proj_raw : path
        Location of project rawdata directory
    work_deriv : path
        Output location for pipeline intermediates, e.g.
        /work/foo/project/derivatives
    sing_fmriprep : path, str
        Location and image of fmriprep singularity file
    fs_license : path, str
        Location of FreeSurfer license
    fd_thresh : float
        Threshold for framewise displacement
    ignore_fmaps : bool
        Whether to incorporate fmaps in preprocessing
    no_freesurfer : bool
        Whether to use the --fs-no-reconall option
    log_dir : path
        Location of directory to capture logs

    Returns
    -------
    dict
        "preproc_bold": ["/paths/to/*preproc_bold.nii.gz"]
        "aroma_bold": ["/paths/to/*AROMAnonaggr_bold.nii.gz"]
        "mask_bold": ["/paths/to/*run-*desc-brain_mask.nii.gz"]
        "mask_anat": "/path/to/anat/*_res-2_desc-brain_mask.nii.gz"

    Raises
    ------
    FileNotFoundError
        <subj>.html missing
        Different lengths of dict["aroma_bold"], dict["preproc_bold"],
            and dict["mask_bold"]
        Preproc, AROMA, or mask files not detected

    """

    # Setup fmriprep specific dir/paths, parent directory
    # of fs_license should contain templateflow.
    research_dir = os.path.dirname(fs_license)
    work_fs = os.path.join(work_deriv, "freesurfer")
    work_fp = os.path.join(work_deriv, "fmriprep")
    work_fp_tmp = os.path.join(work_fp, "tmp_work", subj)
    work_fp_bids = os.path.join(work_fp_tmp, "bids_layout")
    if not os.path.exists(work_fp_bids):
        os.makedirs(work_fp_bids)

    # Construct fmriprep call
    check_file = f"{work_fp}/{subj}.html"
    if not os.path.exists(check_file):
        # bash_cmd = f"""
        #     singularity run \\
        #     --cleanenv \\
        #     --bind {proj_raw}:{proj_raw} \\
        #     --bind {work_deriv}:{work_deriv} \\
        #     --bind {research_dir}:{research_dir} \\
        #     --bind {proj_raw}:/data \\
        #     --bind {work_fp}:/out \\
        #     {sing_fmriprep} \\
        #     /data \\
        #     /out \\
        #     participant \\
        #     --work-dir {work_fp_tmp} \\
        #     --participant-label {subj[4:]} \\
        #     --skull-strip-template MNI152NLin6Asym \\
        #     --output-spaces MNI152NLin6Asym:res-2 \\
        #     --fs-license {fs_license} \\
        #     --fs-subjects-dir {work_fs} \\
        #     --use-aroma \\
        #     --fd-spike-threshold {fd_thresh} \\
        #     --skip-bids-validation \\
        #     --bids-database-dir {work_fp_bids} \\
        #     --nthreads 10 \\
        #     --omp-nthreads 10 \\
        #     --stop-on-first-crash \\
        # """
        # # Append fmriprep call, submit
        # if ignore_fmaps:
        #     bash_cmd += " --ignore fieldmaps"

        bash_list = [
            "singularity run",
            "--cleanenv",
            f"--bind {proj_raw}:{proj_raw}",
            f"--bind {work_deriv}:{work_deriv}",
            f"--bind {research_dir}:{research_dir}",
            f"--bind {proj_raw}:/data",
            f"--bind {work_fp}:/out",
            f"{sing_fmriprep} /data /out participant",
            f"--work-dir {work_fp_tmp}",
            f"--participant-label {subj[4:]}",
            "--skull-strip-template MNI152NLin6Asym",
            "--output-spaces MNI152NLin6Asym:res-2",
            f"--fs-license {fs_license}",
            f"--fs-subjects-dir {work_fs}",
            "--use-aroma",
            f"--fd-spike-threshold {fd_thresh}",
            "--skip-bids-validation",
            f"--bids-database-dir {work_fp_bids}",
            "--nthreads 10 --omp-nthreads 10",
            "--stop-on-first-crash",
        ]

        # Adjust fmriprep call from user input
        if ignore_fmaps:
            bash_list.append("--ignore fieldmaps")

        if no_freesurfer:
            bash_list.append("--fs-no-reconall")

        # Submit fmriprep call
        bash_cmd = " ".join(bash_list)
        _, _ = submit.sbatch(
            bash_cmd,
            f"{subj[7:]}_fmriprep",
            log_dir,
            mem_gig=10,
            num_cpus=10,
            num_hours=20,
        )

        # Check for output
        if not os.path.exists(check_file):
            raise FileNotFoundError(
                f"FMRIPrep output file {subj}.html not found."
            )

    # Clean fmriprep work, freesurfer
    try:
        shutil.rmtree(work_fp_tmp)
        shutil.rmtree(f"{work_fs}/{subj}")
    except FileNotFoundError:
        "FreeSurfer output not found, continuing."

    # Make list of files for FSL
    preproc_bold = sorted(
        glob.glob(
            f"{work_fp}/{subj}/**/func/*desc-preproc_bold.nii.gz",
            recursive=True,
        )
    )
    aroma_bold = sorted(
        glob.glob(
            f"{work_fp}/{subj}/**/func/*desc-smoothAROMAnonaggr_bold.nii.gz",
            recursive=True,
        )
    )
    mask_bold = sorted(
        glob.glob(
            f"{work_fp}/{subj}/**/func/*desc-brain_mask.nii.gz",
            recursive=True,
        )
    )
    mask_anat = glob.glob(
        f"{work_fp}/{subj}/anat/{subj}_space-*_res-2_desc-brain_mask.nii.gz"
    )[0]

    # Check lists
    if not preproc_bold and not aroma_bold and not mask_bold and not mask_anat:
        raise FileNotFoundError(f"Missing fMRIPrep output for {subj}.")

    if len(aroma_bold) != len(preproc_bold) != len(mask_bold):
        raise FileNotFoundError(
            "Number of AROMA, preprocessed, and mask bold files not equal."
        )

    return {
        "preproc_bold": preproc_bold,
        "aroma_bold": aroma_bold,
        "mask_bold": mask_bold,
        "mask_anat": mask_anat,
    }


# %%
def _temporal_filt(run_preproc, out_dir, run_tfilt, subj, log_dir):
    """Temporally filter data with FSL.

    Filter data for specific subject, session, run.

    Parameters
    ----------
    run_preproc : path, str
        Location of fmriprep preprocessed bold file
    out_dir : path
        Location of derivatives, e.g.
        /work/foo/EmoRep_BIDS/derivatives/fsl/sub/sess/func
    run_tfilt : str
        File name of temporally filtered bold, will be made
    subj : str
        BIDS subject
    log_dir : path
        Location of directory to capture logs

    Returns
    -------
    None

    Raises
    ------
    FileNotFoundError
        If <out_dir>/<run_tfilt> not detected.

    Notes
    -----
    Writes <out_dir>/<run_tfilt>.

    """
    run_tmean = run_tfilt.split("desc-")[0] + "desc-tmean_bold.nii.gz"
    bash_cmd = f"""
        fslmaths \
            {run_preproc} \
            -Tmean \
            {out_dir}/{run_tmean}

        fslmaths \
            {run_preproc} \
            -bptf 25 -1 \
            -add {out_dir}/{run_tmean} \
            {out_dir}/{run_tfilt}
    """
    _, _ = submit.sbatch(
        bash_cmd,
        f"{subj[7:]}_tempfilt",
        log_dir,
        mem_gig=6,
    )

    # Check for output
    if not os.path.exists(f"{out_dir}/{run_tfilt}"):
        raise FileNotFoundError(
            f"Failed to make temporal filter file for {subj}."
        )


def _apply_brain_mask(
    sing_afni, out_dir, run_tfilt_masked, run_tfilt, brain_mask, subj, log_dir
):
    """Mask temporally filtered data with AFNI.

    Mask data for specific subject, session, run.

    Parameters
    ----------
    sing_afni : path, str
        Location, file of afni singularity image
    out_dir : path
        Location of derivatives, e.g.
        /work/foo/EmoRep_BIDS/derivatives/fsl/sub/sess/func
    run_tfilt_masked : str
        File name of masked, temporally filtered bold,
        will be made.
    run_tfilt : str
        File name of temporally filtered bold
    brain_mask : str
        Location, file name of brain mask
    subj : str
        BIDS subject
    log_dir : path
        Location of directory to capture logs

    Returns
    -------
    None

    Raises
    ------
    FileNotFoundError
        If <out_dir>/<run_tfilt_masked> not detected.

    Notes
    -----
    Writes <out_dir>/<run_tfilt_masked>.

    """
    brain_mask_name = os.path.basename(brain_mask)
    bash_cmd = f"""
        cp {brain_mask} {out_dir}/{brain_mask_name}

        singularity run \\
        --cleanenv \\
        --bind {out_dir}:{out_dir} \\
        --bind {out_dir}:/opt/home \\
        {sing_afni} \\
        3dcalc \\
            -a {out_dir}/{run_tfilt} \\
            -b {out_dir}/{brain_mask_name} \\
            -float \\
            -prefix {out_dir}/{run_tfilt_masked} \\
            -expr 'a*b'

        rm {out_dir}/{brain_mask_name}
    """
    _, _ = submit.sbatch(
        bash_cmd,
        f"{subj[7:]}_tempmask",
        log_dir,
    )
    # Check for output
    if not os.path.exists(f"{out_dir}/{run_tfilt_masked}"):
        raise FileNotFoundError(
            f"Failed to make masked temporal filter file for {subj}."
            + "Check preprocessing._apply_mask, preprocessing._temporal_filt,"
            + " or preprocessing.fsl_preproc."
        )


# %%
def fsl_preproc(work_fsl, fp_dict, sing_afni, subj, log_dir):
    """Conduct extra preprocessing via FSL and AFNI.

    Temporally filter BOLD data and then multiply with
    a run-specific brain mask.

    Parameters
    ----------
    work_fsl : path
        Location of FSL derivatives, e.g.
        /work/foo/EmoRep_BIDS/derivatives/fsl
    fp_dict : dict
        Returned from preprocessing.fmriprep, contains
        paths to preprocessed BOLD and mask files.
    sing_afni : path, str
        Location of afni singularity image
    subj : str
        BIDS subject
    log_dir : path
        Location of directory to capture logs

    Returns
    -------
    list
        path, location of denoised run files

    Raises
    ------
    NameError
        When preprocess EPI and mask have misaligned runs in dictionary
    FileNotFoundError
        When not all bold runs have a corresponding masked temporal filter file

    """
    # Unpack dict for readability
    run_aroma_bold = fp_dict["aroma_bold"]
    run_preproc_bold = fp_dict["preproc_bold"]
    # run_mask_list = fp_dict["mask_bold"]
    anat_mask = fp_dict["mask_anat"]

    # TODO refactor for job parallelization
    # for run_preproc, run_mask in zip(run_aroma_bold, run_mask_list):

    # # Check runs are same
    # epi_run_num = run_preproc.split("run-")[1].split("_")[0]
    # mask_run_num = run_mask.split("run-")[1].split("_")[0]
    # if epi_run_num != mask_run_num:
    #     raise NameError(
    #         "Runs misalgined in dictionary,"
    #         + f" for files {run_preproc} and {run_mask}."
    #         + " Check preprocessing.fmriprep return."
    #     )

    for run_preproc in itertools.chain(run_preproc_bold, run_aroma_bold):

        # Setup output location
        sess = "ses-" + run_preproc.split("ses-")[1].split("/")[0]
        out_dir = os.path.join(work_fsl, subj, sess, "func")
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)

        # Set description based off aroma/preproc
        h_desc = (
            "tfiltAROMAMasked"
            if "smoothAROMA" in run_preproc
            else "tfiltMasked"
        )
        print(f"run_preproc : {run_preproc}\nh_desc : {h_desc}")

        # Avoid repeating work
        run_tfilt_masked = (
            os.path.basename(run_preproc).split("desc-")[0]
            + f"desc-{h_desc}_bold.nii.gz"
        )
        if os.path.exists(f"{out_dir}/{run_tfilt_masked}"):
            continue

        # Apply temporal filter
        run_tfilt = (
            os.path.basename(run_preproc).split("desc-")[0]
            + "desc-tfilt_bold.nii.gz"
        )
        if not os.path.exists(f"{out_dir}/{run_tfilt}"):
            _temporal_filt(run_preproc, out_dir, run_tfilt, subj, log_dir)

        # Apply mask
        _apply_brain_mask(
            sing_afni,
            out_dir,
            run_tfilt_masked,
            run_tfilt,
            anat_mask,
            subj,
            log_dir,
        )

    denoise_files = glob.glob(
        f"{work_fsl}/{subj}/**/*desc-tfilt*Masked_bold.nii.gz", recursive=True
    )
    if len(denoise_files) != (len(run_aroma_bold) + len(run_preproc_bold)):
        raise FileNotFoundError(f"Missing tfiltMasked files for {subj}.")
    return denoise_files
