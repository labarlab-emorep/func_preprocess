"""Title.

Desc.
"""
import os
from func_preprocessing import submit


def freesurfer(deriv_dir, subj_deriv, subj_t1, subj, sess):
    """Title.

    Desc.
    """
    bash_cmd = f"""
        mri_convert {subj_t1} {subj_deriv}/mri/orig/001.mgz
        recon-all \
            -subjid {subj} \
            -all \
            -sd {deriv_dir}/freesurfer \
            -parallel \
            -openmp 6
    """
    print(f"Starting FreeSurfer for {subj}:\n\t{bash_cmd}")
    _, _ = submit.sbatch(
        bash_cmd, f"fs{subj[4:]}", subj_deriv, num_cpus=6, num_hours=10
    )

    # TODO update fs_files to find something useful
    fs_files = glob.glob(f"{subj_deriv}//mri/foo")
    fs_exists = True if fs_files else False
    return fs_exists
