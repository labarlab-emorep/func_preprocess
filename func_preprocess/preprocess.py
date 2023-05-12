"""Preprocessing methods.

RunFreeSurfer : setup and run freesurfer for subject
RunFmriprep : run fmriprep for each session
fsl_preproc : conduct extra preprocessing steps

"""
import os
import glob
import json
from typing import Union
from multiprocessing import Process
from func_preprocess import submit, helper_tools


class RunFreeSurfer:
    """Run FreeSurfer's recon-all for a specific subject.

    FreeSurfer SUBJECTS_DIR is organized by session.

    Methods
    -------
    recon_all(sess_list)
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

        # Setup directory structures
        proj_subj = os.path.join(
            os.path.dirname(self._proj_raw),
            "derivatives",
            "pre_processing",
            "freesurfer",
            self._sess,
            self._subj,
        )
        subj_fs = os.path.join(self._work_fs, self._subj, "mri/orig")
        if os.path.exists(os.path.join(subj_fs, "001.mgz")):
            return
        for h_dir in [proj_subj, subj_fs]:
            os.makedirs(h_dir, exist_ok=True)

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
        bash_cmd = f"mri_convert {subj_t1} {subj_fs}/001.mgz"
        _, _ = submit.submit_subprocess(
            True,
            bash_cmd,
            f"{self._subj[-4:]}_{self._sess[4:]}_conv",
            self._log_dir,
        )


class RunFmriprep:
    """Run fMRIPrep for a specific subject.

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
        sing_fmriprep,
        tplflow_dir,
        fs_license,
        fd_thresh,
        ignore_fmaps,
        log_dir,
        run_local,
    ):
        """Initialize.

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
        log_dir : path
            Location of directory to capture logs
        run_local : bool
            Whether job, subprocesses are run locally

        """
        print("Initializing RunFmriprep")
        self._subj = subj
        self._proj_raw = proj_raw
        self._work_deriv = work_deriv
        self._sing_fmriprep = sing_fmriprep
        self._tplflow_dir = tplflow_dir
        self._fs_license = fs_license
        self._fs_license_dir = os.path.dirname(fs_license)
        self._fd_thresh = fd_thresh
        self._ignore_fmaps = ignore_fmaps
        self._log_dir = log_dir
        self._run_local = run_local

    def fmriprep(self, sess_list):
        """Spawn an fMRIPrep job for each session.

        Parameters
        ----------
        sess_list : list
            Session identifiers

        """
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

    def _exec_fp(self, sess):
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
        std_out, std_err = submit.submit_subprocess(
            self._run_local,
            bash_fmriprep,
            f"{self._subj[-4:]}_{self._sess[4:]}_fmriprep",
            self._log_dir,
            mem_gig=12,
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
        bash_list = [
            "singularity run",
            "--cleanenv",
            f"--bind {self._proj_raw}:{self._proj_raw}",
            f"--bind {self._log_dir}:{self._log_dir}",
            f"--bind {self._work_deriv}:{self._work_deriv}",
            f"--bind {self._tplflow_dir}:{self._tplflow_dir}",
            f"--bind {self._fs_license_dir}:{self._fs_license_dir}",
            f"--bind {self._proj_raw}:/data",
            f"--bind {self._work_fp}:/out",
            f"{self._sing_fmriprep} /data /out participant",
            f"--work-dir {self._work_fp_tmp}",
            f"--participant-label {self._subj[4:]}",
            "--skull-strip-template MNI152NLin6Asym",
            "--output-spaces MNI152NLin6Asym:res-2",
            f"--bids-filter-file {self._json_filt}",
            f"--fs-license {self._fs_license}",
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

    def get_output(self):
        """Return dictionary of files for extra preprocessing."""

        # Make list of needed files for FSL denoising
        search_path = (
            f"{self._work_deriv}/fmriprep/ses-*/{self._subj}/ses-*/func"
        )
        preproc_bold = sorted(
            glob.glob(f"{search_path}/*desc-preproc_bold.nii.gz")
        )
        mask_bold = sorted(glob.glob(f"{search_path}/*desc-brain_mask.nii.gz"))

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


def fsl_preproc(work_deriv, fp_dict, sing_afni, subj, log_dir, run_local):
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
    work_fsl = os.path.join(work_deriv, "fsl_denoise")
    afni_fsl = helper_tools.AfniFslMethods(log_dir, run_local, sing_afni)

    def _preproc(
        run_epi: Union[str, os.PathLike], run_mask: Union[str, os.PathLike]
    ):
        """Conduct extra preprocessing via FSL, AFNI."""
        # Setup output location
        sess = "ses-" + run_epi.split("ses-")[1].split("/")[0]
        out_dir = os.path.join(work_fsl, subj, sess, "func")
        os.makedirs(out_dir, exist_ok=True)
        afni_fsl.set_subj(subj, out_dir)

        # Set up filenames, check for work
        file_prefix = os.path.basename(run_epi).split("desc-")[0]
        run_smoothed = os.path.join(
            out_dir, f"{file_prefix}desc-smoothed_bold.nii.gz"
        )
        if os.path.exists(run_smoothed):
            return

        # Find mean timeseries, bandpass filter, and mask
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
