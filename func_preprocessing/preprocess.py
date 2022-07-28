"""Functions for controlling FreeSurfer and fMRIPrep."""
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
        bash_cmd, f"{subj[4:]}{sess[4:]}fs", log_dir, num_cpus=6, num_hours=10
    )

    # TODO update fs_files to find something useful
    fs_files = glob.glob(f"{work_fs}/**/001.mgz", recursive=True)
    fs_exists = True if fs_files else False
    return fs_exists


def fmriprep():
    """Title.

    Desc.
    """
    pass
