"""Title.

Desc.
"""
import os
from func_preprocessing import submit


def freesurfer(work_fs, subj_t1, subj):
    """Title.

    Desc.
    """
    bash_cmd = f"""
        mri_convert {subj_t1} {work_fs}/{subj}/mri/orig/001.mgz
        # recon-all \
        #     -subjid {subj} \
        #     -all \
        #     -sd {work_fs} \
        #     -parallel \
        #     -openmp 6
    """
    print(f"Starting FreeSurfer for {subj}:\n\t{bash_cmd}")
    log_dir = os.path.join(os.path.dirname(work_fs), "logs")
    _, _ = submit.sbatch(
        bash_cmd, f"fs{subj[4:]}{sess[4:]}", log_dir, num_cpus=6, num_hours=10
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
