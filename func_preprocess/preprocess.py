"""Preprocessing methods.

RunFreeSurfer : setup and run freesurfer for subject
RunFmriprep : run fmriprep for each session
fsl_preproc : conduct extra preprocessing steps

"""

import os
import glob
import json
from typing import Union, Type
from multiprocessing import Process
from func_preprocess import submit, helper_tools


class RunFreeSurfer:
    """Run FreeSurfer's recon-all for a specific subject.

    FreeSurfer SUBJECTS_DIR is organized by session.

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

    Methods
    -------
    recon_all(sess_list)
        Spawn a recon-all job for each session

    Example
    -------
    run_fs = RunFreeSurfer(*args)
    run_fs.recon_all(["ses-day2", "ses-day3"])

    """

    def __init__(self, subj, proj_raw, work_deriv, log_dir):
        """Initialize."""
        print("Initializing RunFreeSurfer")
        self._subj = subj
        self._proj_raw = proj_raw
        self._work_deriv = work_deriv
        self._log_dir = log_dir

    def recon_all(self, sess_list: list):
        """Spawn a freesurfer recon-all command for each session."""
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

    def _exec_fs(self, sess: str):
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
        _ = self._setup()
        bash_list = [
            "recon-all",
            f"-subjid {self._subj}",
            "-all",
            f"-sd {self._work_fs}",
            "-parallel",
            "-openmp 6",
        ]
        _, _ = submit.schedule_subprocess(
            " ".join(bash_list),
            f"{self._subj[-4:]}_{self._sess[4:]}_freesurfer",
            self._log_dir,
            num_hours=8,
            num_cpus=8,
            mem_gig=16,
        )
        if not os.path.exists(out_file):
            raise FileNotFoundError(f"Expected FreeSurfer output : {out_file}")

    def _setup(self) -> Union[str, os.PathLike]:
        """Setup freesurfer subject's directory, make 001.mgz."""

        # Setup directory structures
        proj_subj = os.path.join(
            os.path.dirname(self._proj_raw),
            "derivatives",
            "pre_processing",
            "freesurfer",
            self._sess,
            self._subj,
        )
        out_dir = os.path.join(self._work_fs, self._subj, "mri/orig")
        for h_dir in [proj_subj, out_dir]:
            if not os.path.exists(h_dir):
                os.makedirs(h_dir)

        # Check for previous setup
        out_path = os.path.join(out_dir, "001.mgz")
        if os.path.exists(out_path):
            return out_path

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

        # Convert anat nifti to mgz
        _, _ = submit.submit_subprocess(f"mri_convert {subj_t1} {out_path}")
        if not os.path.exists(out_path):
            raise FileNotFoundError(
                f"Expected mri_convert output : {out_path}"
            )
        return out_path


class RunFmriprep:
    """Run fMRIPrep for a specific subject.

    Parameters
    ----------
    subj : str
        BIDS subject
    proj_raw : str, os.PathLike
        Location of project rawdata directory
    work_deriv : str, os.PathLike
        Output location for pipeline intermediates
    fd_thresh : float
        Threshold for framewise displacement
    ignore_fmaps : bool
        Whether to incorporate fmaps in preprocessing
    log_dir : str, os.PathLike
        Location of directory to capture logs

    Methods
    -------
    fmriprep(sess_list)
        Spawn an fmriprep job for each session
    get_output()
        Return a dict of fmriprep output

    Example
    -------
    run_fp = RunFmriprep(*args)
    run_fp.fmriprep(["ses-day2", "ses-day3"])
    fp_dict = run_fp.get_output()

    """

    def __init__(
        self,
        subj,
        proj_raw,
        work_deriv,
        fd_thresh,
        ignore_fmaps,
        log_dir,
    ):
        """Initialize."""
        print("Initializing RunFmriprep")
        helper_tools.check_env()

        self._subj = subj
        self._proj_raw = proj_raw
        self._work_deriv = work_deriv
        self._fd_thresh = fd_thresh
        self._ignore_fmaps = ignore_fmaps
        self._log_dir = log_dir

    def fmriprep(self, sess_list: list):
        """Spawn an fMRIPrep job for each session."""
        mult_proc = [
            Process(
                target=self._exec_fp,
                args=(sess,),
            )
            for sess in sess_list
        ]
        for proc in mult_proc:
            proc.start()
        for proc in mult_proc:
            proc.join()

    def _exec_fp(self, sess: str):
        """Setup for, write, and execute fmriprep."""
        # Avoid repeating work
        self._sess = sess
        self._work_fp = os.path.join(self._work_deriv, "fmriprep", self._sess)
        check_file = os.path.join(self._work_fp, f"{self._subj}.html")
        if os.path.exists(check_file):
            return

        # Check and setup
        self._work_fs = os.path.join(
            self._work_deriv, "freesurfer", self._sess
        )
        if not os.path.exists(os.path.join(self._work_fs, self._subj)):
            raise FileNotFoundError(
                f"Expected freesurfer output for {self._subj} "
                + f"at : {self._work_fs}"
            )
        self._work_fp_tmp = os.path.join(self._work_fp, "tmp_work", self._subj)
        self._work_fp_bids = os.path.join(self._work_fp_tmp, "bids_layout")
        os.makedirs(self._work_fp_bids, exist_ok=True)

        # Write and execute command
        self._write_filter()
        bash_fmriprep = self._write_fmriprep()
        std_out, std_err = submit.schedule_subprocess(
            bash_fmriprep,
            f"{self._subj[-4:]}_{self._sess[4:]}_fmriprep",
            self._log_dir,
            mem_gig=24,
            num_cpus=10,
            num_hours=40,
        )

        # Check for output
        if not os.path.exists(check_file):
            print(f"\nstdout : {std_out}\nstderr : {std_err}")
            raise FileNotFoundError(
                f"Missing fMRIPrep output for {self._subj}, {self._sess}"
            )

    def _write_filter(self):
        """Write PyBIDS session filter."""
        filt_dict = {
            "bold": {
                "datatype": "func",
                "suffix": "bold",
                "session": self._sess[4:],
            },
            "t1w": {
                "datatype": "anat",
                "suffix": "T1w",
                "session": self._sess[4:],
            },
            "fmap": {
                "datatype": "fmap",
                "suffix": "epi",
                "session": self._sess[4:],
            },
        }
        self._json_filt = os.path.join(
            self._log_dir, f"{self._subj}_{self._sess}_filt.json"
        )
        with open(self._json_filt, "w") as jf:
            json.dump(filt_dict, jf)

    def _write_fmriprep(self) -> str:
        """Return fMRIPrep singulartity call.."""
        tplflow_dir = os.environ["SINGULARITYENV_TEMPLATEFLOW_HOME"]
        fs_license_dir = os.path.dirname(os.environ["FS_LICENSE"])

        bash_list = [
            "singularity run",
            "--cleanenv",
            f"--bind {self._proj_raw}:{self._proj_raw}",
            f"--bind {self._log_dir}:{self._log_dir}",
            f"--bind {self._work_deriv}:{self._work_deriv}",
            f"--bind {tplflow_dir}:{tplflow_dir}",
            f"--bind {fs_license_dir}:{fs_license_dir}",
            f"--bind {self._proj_raw}:/data",
            f"--bind {self._work_fp}:/out",
            os.environ["SING_FMRIPREP"],
            "/data /out participant",
            f"--work-dir {self._work_fp_tmp}",
            f"--participant-label {self._subj[4:]}",
            "--skull-strip-template MNI152NLin6Asym",
            "--output-spaces MNI152NLin6Asym:res-2",
            f"--bids-filter-file {self._json_filt}",
            f"--fs-license {os.environ['FS_LICENSE']}",
            f"--fs-subjects-dir {self._work_fs}",
            "--use-aroma",
            f"--fd-spike-threshold {self._fd_thresh}",
            "--skip-bids-validation",
            f"--bids-database-dir {self._work_fp_bids}",
            "--nthreads 10 --omp-nthreads 8",
            "--stop-on-first-crash",
            "--debug all",
        ]

        # Adjust fmriprep call from user input
        if self._ignore_fmaps:
            bash_list.append("--ignore fieldmaps")
        return " ".join(bash_list)

    def get_output(self) -> dict:
        """Return dictionary of files for extra preprocessing."""

        # Make list of needed files for FSL denoising
        search_path = (
            f"{self._work_deriv}/fmriprep/ses-*/{self._subj}/ses-*/func"
        )
        preproc_bold = sorted(
            glob.glob(
                f"{search_path}/*space-MNI152NLin6Asym_res-2_"
                + "desc-preproc_bold.nii.gz"
            )
        )
        mask_bold = sorted(
            glob.glob(
                f"{search_path}/*space-MNI152NLin6Asym_res-2_"
                + "desc-brain_mask.nii.gz"
            )
        )

        # Check lists
        if not preproc_bold and not mask_bold:
            raise FileNotFoundError(
                f"Missing fMRIPrep output for {self._subj}."
            )
        if len(preproc_bold) != len(mask_bold):
            raise FileNotFoundError(
                "Number of preprocessed and mask bold files not equal."
            )
        return {
            "preproc_bold": preproc_bold,
            "mask_bold": mask_bold,
        }


def _preproc(
    subj: str,
    run_epi: Union[str, os.PathLike],
    run_mask: Union[str, os.PathLike],
    work_fsl: Union[str, os.PathLike],
    afni_fsl: Type[helper_tools.ExtraPreproc],
):
    """Conduct extra preprocessing via FSL, AFNI for run."""
    # Setup output location
    sess = "ses-" + run_epi.split("ses-")[1].split("/")[0]
    out_dir = os.path.join(work_fsl, subj, sess, "func")
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    afni_fsl.set_subj(subj, out_dir)

    # Set up filenames, check for work
    file_prefix = os.path.basename(run_epi).split("desc-")[0]
    run_out = os.path.join(out_dir, f"{file_prefix}desc-scaled_bold.nii.gz")
    if os.path.exists(run_out):
        return

    # Find mean timeseries and filter
    run_tmean = afni_fsl.tmean(run_epi)
    run_bandpass = afni_fsl.bandpass(run_epi, run_tmean)

    # Scale timeseries and smooth, mask
    med_value = afni_fsl.median(run_bandpass, run_mask)
    run_scaled = afni_fsl.scale(
        run_bandpass, med_value, desc="desc-ScaleNoMask"
    )
    run_smooth = afni_fsl.smooth(run_scaled, 4, desc="desc-SmoothNoMask")
    _ = afni_fsl.mask_epi(run_scaled, run_mask, desc="desc-scaled")
    _ = afni_fsl.mask_epi(run_smooth, run_mask, desc="desc-smoothed")


def fsl_preproc(work_deriv, fp_dict, subj, log_dir):
    """Conduct extra preprocessing via FSL and AFNI.

    Bandpass filter and mask each EPI run, scale EPI timeseries by
    10000/median, and then smooth by 4mm FWHM.

    Parameters
    ----------
    work_deriv : str, os.PathLike
        Output location for pipeline intermediates
    fp_dict : dict
        Returned from preprocessing.fmriprep, contains
        paths to preprocessed BOLD and mask files. Required keys:
        -   [preproc_bold] = list, paths to fmriprep preproc run output
        -   [mask_bold] = list, paths to fmriprep preproc run masks
    subj : str
        BIDS subject
    log_dir : str, os.PathLike
        Location of directory to capture logs

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

    # Set up for extra preprocessing
    work_fsl = os.path.join(work_deriv, "fsl_denoise")
    afni_fsl = helper_tools.ExtraPreproc(log_dir)

    # Conduct extra preprocessing
    for run_epi, run_mask in zip(
        fp_dict["preproc_bold"], fp_dict["mask_bold"]
    ):
        _preproc(subj, run_epi, run_mask, work_fsl, afni_fsl)

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
