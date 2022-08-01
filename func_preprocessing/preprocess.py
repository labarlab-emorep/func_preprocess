"""Functions for controlling FreeSurfer and fMRIPrep."""
import os
import glob
from func_preprocessing import submit


def freesurfer(work_fs, subj_t1, subj, sess, log_dir):
    """Submit FreeSurfer for subject's session.

    Convert T1w NIfTI into Analyze format with FreeSurfer
    directory organization. Run FreeSurfer.

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
    subj_raw,
    work_fp,
    work_fs,
    sing_fmriprep,
    sing_tf,
    fs_license,
    log_dir,
    proj_home,
    proj_work,
):
    """Title.

    Desc.

    Parameters
    ----------
    """
    subj_num = subj[4:]
    work_par = os.path.dirname(work_fp)
    work_fp_tmp = os.path.join(work_fp, "tmp_work", subj)
    work_fp_bids = os.path.join(work_fp_tmp, "bids_layout")
    if not os.path.exists(work_fp_bids):
        os.makedirs(work_fp_bids)

    bash_cmd = f"""
        singularity run \\
        --cleanenv \\
        --bind {proj_home}:{proj_home} \\
        --bind {proj_work}:{proj_work} \\
        --bind {subj_raw}:/data \\
        --bind {work_par}:/out \\
        {sing_fmriprep} \\
        /data \\
        /out \\
        participant \\
        --work-dir {work_fp_tmp} \\
        --participant-label {subj_num} \\
        --skull-strip-template MNI152NLin2009cAsym \\
        --output-spaces MNI152NLin2009cAsym:res-2 \\
        --fs-license {fs_license} \\
        --fs-subjects-dir {work_fs} \\
        --use-aroma \\
        --skip-bids-validation \\
        --bids-database-dir {work_fp_bids} \\
        --nthreads 10 \\
        --omp-nthreads 10 \\
        --stop-on-first-crash
    """
    _, _ = submit.sbatch(
        bash_cmd, f"{subj[4:]}fp", log_dir, num_cpus=10, num_hours=20
    )
