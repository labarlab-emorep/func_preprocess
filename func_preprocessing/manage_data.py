"""Methods for managing directories and data"""
import os
import glob
import subprocess
import shutil
from fnmatch import fnmatch


def copy_clean(proj_deriv, work_deriv, subj):
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

    """
    # Clean FSL files
    print("\n\tCleaning FSL files ...")
    work_fsl_subj = os.path.join(work_deriv, "fsl", subj)
    nii_list = sorted(
        glob.glob(f"{work_fsl_subj}/**/*.nii.gz", recursive=True)
    )
    remove_fsl = [
        x for x in nii_list if not fnmatch(x, "*desc-tfiltMasked_bold.nii.gz")
    ]
    for rm_file in remove_fsl:
        os.remove(rm_file)

    # Copy remaining FSL files to proj_deriv, use faster bash
    proj_fsl_subj = os.path.join(proj_deriv, "fsl", subj)
    cp_cmd = f"cp -r {work_fsl_subj} {proj_fsl_subj}"
    cp_sp = subprocess.Popen(cp_cmd, shell=True, stdout=subprocess.PIPE)
    _ = cp_sp.communicate()

    # Clean fMRIprep files
    print("\n\tCleaning fMRIPrep files ...")
    work_fp_subj = os.path.join(work_deriv, "fmriprep", subj)
    remove_fp = glob.glob(
        f"{work_fp_subj}/**/*desc-smoothAROMAnonaggr_bold.nii.gz",
        recursive=True,
    )
    for rm_file in remove_fp:
        os.remove(rm_file)

    # Copy fMRIPrep files
    work_fp = os.path.dirname(work_fp_subj)
    proj_fp = os.path.join(proj_deriv, "fmriprep")
    keep_fmriprep = [
        "desc-aparcaseg_dseg.tsv",
        "desc-aseg_dseg.tsv",
        f"{subj}.html",
    ]
    for kp_file in keep_fmriprep:
        shutil.copyfile(f"{work_fp}/{kp_file}", f"{proj_fp}/{kp_file}")

    proj_fp_subj = os.path.join(proj_fp, subj)
    cp_cmd = f"cp -r {work_fp_subj} {proj_fp_subj}"
    cp_sp = subprocess.Popen(cp_cmd, shell=True, stdout=subprocess.PIPE)
    _ = cp_sp.communicate()

    # Turn out the lights
    shutil.rmtree(work_fp_subj)
    shutil.rmtree(work_fsl_subj)
