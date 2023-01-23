"""Preprocessing methods.

FreeSurfer (deprecated), fMRIPrep, FSL, and AFNI
software used for preprocessing EmoRep data.

"""
# %%
import os
import glob
import shutil
from fnmatch import fnmatch
from func_preprocessing import submit


# %%
def freesurfer(work_fs, subj_t1, subj, sess, log_dir):
    """Submit FreeSurfer for subject's session.

    DEPRECATED

    Convert T1w NIfTI into Analyze format with FreeSurfer
    directory organization. Run FreeSurfer.

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
    run_local,
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
    run_local : bool
        Whether job, subprocesses are run locally

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
        Different lengths of dict["aroma_bold"] and dict["mask_bold"]
        AROMA or mask files not detected

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
            "--debug all",
        ]

        # Adjust fmriprep call from user input
        if ignore_fmaps:
            bash_list.append("--ignore fieldmaps")

        if no_freesurfer:
            bash_list.append("--fs-no-reconall")

        # Submit fmriprep call
        bash_cmd = " ".join(bash_list)
        _, _ = submit.submit_subprocess(
            run_local,
            bash_cmd,
            f"{subj[7:]}_fmriprep",
            log_dir,
            mem_gig=12,
            num_cpus=10,
            num_hours=40,
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

    # Make list of needed files for FSL denoising
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

    # Find anatomical mask based on multiple or single sessions
    mask_str = f"{subj}_*_res-2_desc-brain_mask.nii.gz"
    try:
        anat_path = f"{work_fp}/{subj}/anat"
        mask_anat = sorted(glob.glob(f"{anat_path}/{mask_str}"))[0]
    except IndexError:
        anat_path = f"{work_fp}/{subj}/ses-*/anat"
        mask_anat = sorted(glob.glob(f"{anat_path}/{mask_str}"))[0]

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
def fsl_preproc(work_fsl, fp_dict, sing_afni, subj, log_dir, run_local):
    """Conduct extra preprocessing via FSL and AFNI.

    Temporally filter BOLD data and then multiply with
    an anatomical brain mask.

    Parameters
    ----------
    work_fsl : path
        Location of FSL derivatives, e.g.
        /work/foo/EmoRep_BIDS/derivatives/fsl
    fp_dict : dict
        Returned from preprocessing.fmriprep, contains
        paths to preprocessed BOLD and mask files. Required keys:
        -   [aroma_bold] = list, paths to fmriprep aroma run output
        -   [preproc_bold] = list, paths to fmriprep preproc run output
        -   [mask_bold] = list, paths to fmriprep preproc run masks
    sing_afni : path, str
        Location of afni singularity image
    subj : str
        BIDS subject
    log_dir : path
        Location of directory to capture logs
    run_local : bool
        Whether job, subprocesses are run locally

    Returns
    -------
    list
        path, location of denoised run files

    Raises
    ------
    NameError
        Preprocess EPI and mask have misaligned runs in dictionary
    FileNotFoundError
        Not all bold runs have a corresponding masked temporal filter file
    KeyError
        Missing required key in fp_dict

    """

    # Set inner functions
    def _temporal_filt(
        run_preproc: str, out_dir: str, run_tfilt: str, bptf: int = 25
    ) -> None:
        """Temporally filter data with FSL."""
        print(f"Temporally filtering file : {run_preproc}")
        run_tmean = run_tfilt.split("desc-")[0] + "desc-tmean_bold.nii.gz"
        bash_cmd = f"""
            fslmaths \
                {run_preproc} \
                -Tmean \
                {out_dir}/{run_tmean}

            fslmaths \
                {run_preproc} \
                -bptf {bptf} -1 \
                -add {out_dir}/{run_tmean} \
                {out_dir}/{run_tfilt}
        """
        _, _ = submit.submit_subprocess(
            run_local,
            bash_cmd,
            f"{subj[7:]}_tempfilt",
            log_dir,
            mem_gig=6,
        )
        chk_path = os.path.join(out_dir, run_tfilt)
        if not os.path.exists(chk_path):
            raise FileNotFoundError(f"Failed to find : {chk_path}")

    def _apply_brain_mask(
        out_dir: str,
        run_tfilt_masked: str,
        run_tfilt: str,
        brain_mask: str,
    ) -> None:
        """Mask temporally filtered data with AFNI."""
        brain_mask_name = os.path.basename(brain_mask)
        print(f"Masking file : {run_tfilt}")
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
        """
        _, _ = submit.submit_subprocess(
            run_local,
            bash_cmd,
            f"{subj[7:]}_tempmask",
            log_dir,
        )
        # Check for output
        chk_path = os.path.join(out_dir, run_tfilt_masked)
        if not os.path.exists(chk_path):
            raise FileNotFoundError(f"Failed to find : {chk_path}")

    # Check for required fmriprep keys
    req_keys = ["aroma_bold", "preproc_bold", "mask_bold"]
    for _key in req_keys:
        if _key not in fp_dict.keys():
            raise KeyError(f"Expected key in fp_dict : {_key}")

    # Temporal filter and mask both preproc and aroma files
    for cnt, run_mask in enumerate(fp_dict["mask_bold"]):
        run_preproc = fp_dict["preproc_bold"][cnt]
        run_aroma = fp_dict["aroma_bold"][cnt]
        for run_epi in [run_preproc, run_aroma]:

            # Setup output location
            sess = "ses-" + run_epi.split("ses-")[1].split("/")[0]
            out_dir = os.path.join(work_fsl, subj, sess, "func")
            if not os.path.exists(out_dir):
                os.makedirs(out_dir)

            # Set description based off aroma/preproc
            h_desc = (
                "tfiltAROMAMasked"
                if "smoothAROMA" in run_epi
                else "tfiltMasked"
            )

            # Avoid repeating work
            run_tfilt_masked = (
                os.path.basename(run_epi).split("desc-")[0]
                + f"desc-{h_desc}_bold.nii.gz"
            )
            if os.path.exists(f"{out_dir}/{run_tfilt_masked}"):
                continue

            # Apply temporal filter
            run_tfilt = (
                os.path.basename(run_epi).split("desc-")[0]
                + "desc-tfilt_bold.nii.gz"
            )
            if not os.path.exists(f"{out_dir}/{run_tfilt}"):
                _temporal_filt(run_epi, out_dir, run_tfilt)

            # Apply mask
            _apply_brain_mask(
                out_dir,
                run_tfilt_masked,
                run_tfilt,
                run_mask,
            )

    denoise_files = glob.glob(
        f"{work_fsl}/{subj}/**/*desc-tfilt*Masked_bold.nii.gz", recursive=True
    )
    if len(denoise_files) != (
        len(fp_dict["preproc_bold"]) + len(fp_dict["armoa_bold"])
    ):
        raise FileNotFoundError(f"Missing tfiltMasked files for {subj}.")
    return denoise_files


def copy_clean(proj_deriv, work_deriv, subj, no_freesurfer, log_dir):
    """Housekeeping for data.

    Delete unneeded files from work_deriv, copy remaining to
    the proj_deriv location.

    Parameters
    ----------
    proj_deriv : path
        Project derivative location, e.g.
        /hpc/group/labarlab/EmoRep_BIDS/derivatives
    work_deirv : path
        Working derivative location, e.g.
        /work/foo/EmoRep_BIDS/derivatives
    subj : str
        BIDS subject
    no_freesurfer : bool
        Whether to use the --fs-no-reconall option

    """
    # Clean FSL files
    print("\n\tCleaning FSL files ...")
    work_fsl_subj = os.path.join(work_deriv, "fsl_denoise", subj)
    nii_list = sorted(
        glob.glob(f"{work_fsl_subj}/**/*.nii.gz", recursive=True)
    )
    remove_fsl = [
        x
        for x in nii_list
        if not fnmatch(x, "*Masked_bold.nii.gz")
        and not fnmatch(x, "*res-2*_bold.nii.gz")
    ]
    for rm_file in remove_fsl:
        os.remove(rm_file)

    # Copy remaining FSL files to proj_deriv, use faster bash
    print("\n\tCopying fsl_denoise files ...")
    proj_fsl_subj = os.path.join(proj_deriv, "fsl_denoise", subj)
    cp_cmd = f"cp -r {work_fsl_subj} {proj_fsl_subj}"
    _, _ = submit.submit_subprocess(True, cp_cmd, f"{subj[7:]}_cp", log_dir)

    # Copy fMRIPrep files, reflect freesurfer choice
    print("\n\tCopying fMRIPrep files ...")
    work_fp_subj = os.path.join(work_deriv, "fmriprep", subj)
    work_fp = os.path.dirname(work_fp_subj)
    proj_fp = os.path.join(proj_deriv, "fmriprep")
    keep_fmriprep = [
        f"{subj}.html",
    ]
    if not no_freesurfer:
        keep_fmriprep.append("desc-aparcaseg_dseg.tsv")
        keep_fmriprep.append("desc-aseg_dseg.tsv")
    for kp_file in keep_fmriprep:
        shutil.copyfile(f"{work_fp}/{kp_file}", f"{proj_fp}/{kp_file}")

    proj_fp_subj = os.path.join(proj_fp, subj)
    cp_cmd = f"cp -r {work_fp_subj} {proj_fp_subj}"
    _, _ = submit.submit_subprocess(True, cp_cmd, f"{subj[7:]}_cp", log_dir)

    # Turn out the lights
    shutil.rmtree(work_fp_subj)
    shutil.rmtree(work_fsl_subj)
