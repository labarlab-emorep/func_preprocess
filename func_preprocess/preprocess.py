"""Preprocessing methods.

RunFreeSurfer : setup and run freesurfer for subject
fmriprep : run fMRIPrep for all
FreeSurfer, fMRIPrep, FSL, and AFNI
software used for preprocessing EmoRep data.

"""
import os
import glob
import shutil
from typing import Union
from multiprocessing import Process
from func_preprocess import submit, helper_tools


class RunFreeSurfer:
    """Run FreeSurfer's recon-all for a specific subject.

    FreeSurfer SUBJECTS_DIR is organized by session.

    Methods
    -------
    recon_all()
        Spawn a recon-all job for each session

    Example
    -------
    run_fs = RunFreeSurfer(*args)
    run_fs.recon_all(["ses-day2", "ses-day3"])

    """

    def __init__(self, subj, proj_raw, work_deriv, log_dir, run_local):
        """Initialize.

        Parameters
        ----------
        subj : str
            BIDS subject
        proj_raw : str, os.PathLike
            Location of project rawdata directory
        work_deriv : str, os.PathLike
            Output location for pipeline intermediates
        log_dir : str, os.PathLike
            Location for capturing stdout/err

        """
        print("Initializing RunFreeSurfer")
        self._subj = subj
        self._proj_raw = proj_raw
        self._work_deriv = work_deriv
        self._log_dir = log_dir
        self._run_local = run_local

    def recon_all(self, sess_list):
        """Spawn a freesurfer recon-all command for each session.

        Parameters
        ----------
        sess_list : list
            Session identifiers

        """
        mult_proc = [
            Process(
                target=self._exec_fs,
                args=(sess,),
            )
            for sess in sess_list
        ]
        for proc in mult_proc:
            proc.start()
        for proc in mult_proc:
            proc.join()

    def _exec_fs(self, sess):
        """Run FreeSurfer recon-all."""
        self._sess = sess
        self._work_fs = os.path.join(
            self._work_deriv, "freesurfer", self._sess
        )

        # Avoid repeating work
        out_file = os.path.join(
            self._work_fs, self._subj, "mri/aparc+aseg.mgz"
        )
        if os.path.exists(out_file):
            return

        # Check for required sess anat
        if not os.path.exists(
            os.path.join(self._proj_raw, self._subj, self._sess)
        ):
            print(f"Session rawdata not found for {self._subj} : {self._sess}")
            return

        # Construct recon-all command, execute
        self._setup()
        bash_list = [
            "recon-all",
            f"-subjid {self._subj}",
            "-all",
            f"-sd {self._work_fs}",
            "-parallel",
            "-openmp 6",
        ]
        _, _ = submit.submit_subprocess(
            self._run_local,
            " ".join(bash_list),
            f"{self._subj[-4:]}_{self._sess[4:]}_freesurfer",
            self._log_dir,
            num_hours=8,
            num_cpus=6,
        )
        if not os.path.exists(out_file):
            raise FileNotFoundError(f"Expected FreeSurfer output : {out_file}")

    def _setup(self):
        """Setup freesurfer subject's directory, make 001.mgz."""

        # Get rawdata T1w
        subj_t1 = os.path.join(
            self._proj_raw,
            self._subj,
            self._sess,
            "anat",
            f"{self._subj}_{self._sess}_T1w.nii.gz",
        )
        if not os.path.exists(subj_t1):
            raise FileNotFoundError(
                f"Missing rawdata T1w for : {self._subj}, {self._sess}"
            )

        # Setup data structure and convert nii-mgz
        subj_fs = os.path.join(self._work_fs, self._subj, "mri/orig")
        if os.path.exists(os.path.join(subj_fs, "001.mgz")):
            return
        if not os.path.exists(subj_fs):
            os.makedirs(subj_fs)
        bash_cmd = f"mri_convert {subj_t1} {subj_fs}/001.mgz"
        _, _ = submit.submit_subprocess(
            self._run_local,
            bash_cmd,
            f"{self._subj[-4:]}_{self._sess[4:]}_conv",
            self._log_dir,
        )


def fmriprep(
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
    log_dir : path
        Location of directory to capture logs
    run_local : bool
        Whether job, subprocesses are run locally

    Returns
    -------
    dict
        "preproc_bold": ["/paths/to/*preproc_bold.nii.gz"]
        "mask_bold": ["/paths/to/*run-*desc-brain_mask.nii.gz"]
        "mask_anat": "/path/to/anat/*_res-2_desc-brain_mask.nii.gz"

    Raises
    ------
    FileNotFoundError
        <subj>.html missing
        Different lengths of dict["aroma_bold"] and dict["mask_bold"]
        Missing preproc or mask files

    """
    # Setup fmriprep specific dir/paths
    fs_license_dir = os.path.dirname(fs_license)
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
            f"--bind {tplflow_dir}:{tplflow_dir}",
            f"--bind {fs_license_dir}:{fs_license_dir}",
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
        std_out, std_err = submit.submit_subprocess(
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
            print(f"\nstdout : {std_out}\nstderr : {std_err}")
            raise FileNotFoundError(
                f"FMRIPrep output file {subj}.html not found."
            )

    # Clean fmriprep work, freesurfer
    try:
        shutil.rmtree(work_fp_tmp)
        shutil.rmtree(f"{work_fs}/{subj}")
    except FileNotFoundError:
        print("FreeSurfer output not found, continuing.")

    # Make list of needed files for FSL denoising
    preproc_bold = sorted(
        glob.glob(
            f"{work_fp}/{subj}/**/func/*desc-preproc_bold.nii.gz",
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
    if not preproc_bold and not mask_bold and not mask_anat:
        raise FileNotFoundError(f"Missing fMRIPrep output for {subj}.")

    if len(preproc_bold) != len(mask_bold):
        raise FileNotFoundError(
            "Number of preprocessed and mask bold files not equal."
        )

    return {
        "preproc_bold": preproc_bold,
        "mask_bold": mask_bold,
        "mask_anat": mask_anat,
    }


# %%
def fsl_preproc(work_fsl, fp_dict, sing_afni, subj, log_dir, run_local):
    """Conduct extra preprocessing via FSL and AFNI.

    Bandpass filter and mask each EPI run, scale EPI timeseries by
    10000/median, and then smooth by 4mm FWHM.

    Parameters
    ----------
    work_fsl : path
        Location of FSL derivatives, e.g.
        /work/foo/EmoRep_BIDS/derivatives/fsl
    fp_dict : dict
        Returned from preprocessing.fmriprep, contains
        paths to preprocessed BOLD and mask files. Required keys:
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
        path, location of scaled run files

    Raises
    ------
    NameError
        Preprocess EPI and mask have misaligned runs in dictionary
    FileNotFoundError
        Not all bold runs have a corresponding masked temporal filter file
    KeyError
        Missing required key in fp_dict

    """
    # Check for required fmriprep keys
    req_keys = ["preproc_bold", "mask_bold"]
    for _key in req_keys:
        if _key not in fp_dict.keys():
            raise KeyError(f"Expected key in fp_dict : {_key}")

    # Mutliprocess extra preprocessing steps across runs
    afni_fsl = helper_tools.AfniFslMethods(log_dir, run_local, sing_afni)

    def _preproc(
        run_epi: Union[str, os.PathLike], run_mask: Union[str, os.PathLike]
    ):
        """Conduct extra preprocessing via FSL, AFNI."""
        # Setup output location
        sess = "ses-" + run_epi.split("ses-")[1].split("/")[0]
        out_dir = os.path.join(work_fsl, subj, sess, "func")
        try:
            os.makedirs(out_dir)
        except FileExistsError:
            pass
        afni_fsl.set_subj(subj, out_dir)

        # Set up filenames, check for work
        file_prefix = os.path.basename(run_epi).split("desc-")[0]
        run_smoothed = os.path.join(
            out_dir, f"{file_prefix}desc-smoothed_bold.nii.gz"
        )
        if os.path.exists(run_smoothed):
            return

        # Find mean timeseries, bandpass filter, and mask
        run_scaled = os.path.join(
            out_dir, f"{file_prefix}desc-scaled_bold.nii.gz"
        )
        if not os.path.exists(run_scaled):
            run_tmean = afni_fsl.tmean(
                run_epi, f"{file_prefix}desc-tmean_bold.nii.gz"
            )
            run_bandpass = afni_fsl.bandpass(
                run_epi, run_tmean, f"{file_prefix}desc-tfilt_bold.nii.gz"
            )
            run_masked = afni_fsl.mask_epi(
                run_bandpass,
                run_mask,
                f"{file_prefix}desc-tfiltMasked_bold.nii.gz",
            )

            # Scale timeseries and smooth
            med_value = afni_fsl.median(run_masked, run_mask)
            run_scaled = afni_fsl.scale(
                run_masked, f"{file_prefix}desc-scaled_bold.nii.gz", med_value
            )
        _ = afni_fsl.smooth(run_scaled, 4, os.path.basename(run_smoothed))

    mult_proc = [
        Process(
            target=_preproc,
            args=(
                run_epi,
                run_mask,
            ),
        )
        for run_epi, run_mask in zip(
            fp_dict["preproc_bold"], fp_dict["mask_bold"]
        )
    ]
    for proc in mult_proc:
        proc.start()
    for proc in mult_proc:
        proc.join()

    # Check for expected number of files
    scaled_files = glob.glob(
        f"{work_fsl}/{subj}/**/*desc-scaled_bold.nii.gz", recursive=True
    )
    if len(scaled_files) != len(fp_dict["preproc_bold"]):
        raise FileNotFoundError(f"Missing scaled files for {subj}.")

    # Clean intermediate files
    fsl_all = glob.glob(f"{work_fsl}/{subj}/**/func/*.nii.gz", recursive=True)
    tmp_all = glob.glob(f"{work_fsl}/{subj}/**/func/tmp_*", recursive=True)
    list_all = fsl_all + tmp_all
    remove_files = [
        x for x in list_all if "scaled" not in x and "smoothed" not in x
    ]
    for rm_file in remove_files:
        os.remove(rm_file)
    return scaled_files


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
    log_dir : path
        Location of directory to capture logs

    """
    # Copy remaining FSL files to proj_deriv, use faster bash
    print("\n\tCopying fsl_denoise files ...")
    work_fsl_subj = os.path.join(work_deriv, "fsl_denoise", subj)
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
