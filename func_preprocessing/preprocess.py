"""Functions for controlling FreeSurfer and fMRIPrep."""
# %%
import os
import glob
import shutil
from func_preprocessing import submit


# %%
def freesurfer(work_fs, subj_t1, subj, sess, log_dir):
    """Submit FreeSurfer for subject's session.

    Convert T1w NIfTI into Analyze format with FreeSurfer
    directory organization. Run FreeSurfer.

    Deprecated

    Parameters
    ----------
    work_fs : Path
        Location of freesurfer derivative directory
    subj_t1 : Path
        Location, file of rawdata T1w nii
    subj : str
        BIDS subject
    sess : str
        BIDS session
    log_dir : Path
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
            f"{subj[4:]}{sess[4:]}fs",
            log_dir,
            num_cpus=6,
            num_hours=10,
        )

    fs_files = glob.glob(f"{work_fs}/**/aparc.a2009s+aseg.mgz", recursive=True)
    fs_exists = True if fs_files else False
    return fs_exists


def fmriprep(
    subj,
    raw_dir,
    work_fp,
    work_fs,
    sing_fmriprep,
    sing_tf,
    fs_license,
    log_dir,
    proj_home,
    proj_work,
    fd_spike_thresh=0.3,
):
    """Title.

    Desc.

    Parameters
    ----------
    """
    work_fp_tmp = os.path.join(work_fp, "tmp_work", subj)
    work_fp_bids = os.path.join(work_fp_tmp, "bids_layout")
    if not os.path.exists(work_fp_bids):
        os.makedirs(work_fp_bids)

    check_file = f"{work_fp}/{subj}.html"
    if not os.path.exists(check_file):
        bash_cmd = f"""
            singularity run \\
            --cleanenv \\
            --bind {proj_home}:{proj_home} \\
            --bind {proj_work}:{proj_work} \\
            --bind {raw_dir}:/data \\
            --bind {work_fp}:/out \\
            {sing_fmriprep} \\
            /data \\
            /out \\
            participant \\
            --work-dir {work_fp_tmp} \\
            --participant-label {subj[4:]} \\
            --skull-strip-template MNI152NLin6Asym \\
            --output-spaces MNI152NLin6Asym:res-2 \\
            --fs-license {fs_license} \\
            --fs-subjects-dir {work_fs} \\
            --use-aroma \\
            --fd-spike-threshold {fd_spike_thresh} \\
            --skip-bids-validation \\
            --bids-database-dir {work_fp_bids} \\
            --nthreads 10 \\
            --omp-nthreads 10 \\
            --stop-on-first-crash
        """
        _, _ = submit.sbatch(
            bash_cmd,
            f"{subj[4:]}fp",
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

    # Clean
    try:
        shutil.rmtree(work_fp_tmp)
        shutil.rmtree(f"{work_fs}/{subj}")
    except FileNotFoundError:
        "FreeSurfer output not found, continuing."

    # Make list of files for FSL
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

    if aroma_bold and mask_bold:
        if len(aroma_bold) != len(mask_bold):
            raise FileNotFoundError(
                "Number of AROMA and mask bold files not equal."
            )
        return {"aroma_bold": aroma_bold, "mask_bold": mask_bold}
    else:
        raise FileNotFoundError(
            "Failed to detect desc-smoothAROMAnonaggr_bold"
            + f" or run desc-brain_mask for {subj}."
        )


# %%
def _temporal_filt(bold_preproc, out_dir, bold_tfilt, subj, log_dir):
    """Title.

    Desc.
    """
    out_tmean = bold_tfilt.split("desc-")[0] + "desc-tmean_bold.nii.gz"
    bash_cmd = f"""
        fslmaths \
            {bold_preproc} \
            -Tmean \
            {out_dir}/{out_tmean}

        fslmaths \
            {bold_preproc} \
            -bptf 25 -1 \
            -add {out_dir}/{out_tmean} \
            {out_dir}/{bold_tfilt}
    """
    _, _ = submit.sbatch(
        bash_cmd,
        f"{subj[4:]}tf",
        log_dir,
        mem_gig=6,
    )

    # Check for output
    if not os.path.exists(f"{out_dir}/{bold_tfilt}"):
        raise FileNotFoundError(
            f"Failed to make temporal filter file for {subj}."
        )


def _apply_mask(
    sing_afni, out_dir, masked_file, bold_tfilt, bold_mask, subj, log_dir
):
    """Title.

    Desc.
    """
    bold_mask_name = os.path.basename(bold_mask)
    bash_cmd = f"""
        cp {bold_mask} {out_dir}/{bold_mask_name}

        singularity run \\
        --cleanenv \\
        --bind {out_dir}:/opt/home \\
        {sing_afni} \\
        3dcalc \\
            -a {out_dir}/{bold_tfilt} \\
            -b {out_dir}/{bold_mask_name} \\
            -float \\
            -prefix {out_dir}/{masked_file} \\
            -expr 'a*b'

        rm {out_dir}/{bold_mask_name}
    """
    print(bash_cmd)
    # return
    _, _ = submit.sbatch(
        bash_cmd,
        f"{subj[4:]}tm",
        log_dir,
    )
    # Check for output
    if not os.path.exists(f"{out_dir}/{masked_file}"):
        raise FileNotFoundError(
            f"Failed to make masked temporal filter file for {subj}."
        )


# %%
def fsl_preproc(work_fsl, fp_dict, sing_afni, subj, log_dir):
    """Title.

    Desc.
    """
    for bold_preproc, bold_mask in zip(
        fp_dict["aroma_bold"], fp_dict["mask_bold"]
    ):

        # Setup output location
        sess = "ses-" + bold_preproc.split("ses-")[1].split("/")[0]
        out_dir = os.path.join(work_fsl, subj, sess, "func")
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)

        # Apply temporal filter
        bold_tfilt = (
            os.path.basename(bold_preproc).split("desc-")[0]
            + "desc-tfilt_bold.nii.gz"
        )
        if not os.path.exists(f"{out_dir}/{bold_tfilt}"):
            _temporal_filt(bold_preproc, out_dir, bold_tfilt, subj, log_dir)

        # Apply mask
        masked_file = (
            bold_tfilt.split("desc-")[0] + "desc-tfiltMasked_bold.nii.gz"
        )
        if not os.path.exists(f"{out_dir}/{masked_file}"):
            _apply_mask(
                sing_afni,
                out_dir,
                masked_file,
                bold_tfilt,
                bold_mask,
                subj,
                log_dir,
            )
