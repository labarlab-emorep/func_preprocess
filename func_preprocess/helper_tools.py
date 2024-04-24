"""Methods to support preprocessing.

copy_clean : copy intermediates in work to group, purge work
PullPush : down/upload relevant files from/to Keoki
ExtraPreproc : FSL and AFNI methods for extra preprocessing steps

"""

import os
import time
import glob
import subprocess
from typing import Union, Tuple
from func_preprocess import submit


def copy_clean(subj, sess_list, proj_deriv, work_deriv, log_dir):
    """Copy data from work to proj_deriv location, remove work dirs.

    Parameters
    ----------
    subj : str
        BIDS subject
    sess_list : list
        BIDS session identifiers
    proj_deriv : str, os.PathLike
        Project derivative location
    work_deirv : str, os.PathLike
        Working derivative location
    log_dir : str, os.PathLike
        Location of directory to capture logs

    """
    # Setup source, destination for fsl preprocessing
    work_fsl_subj = f"{work_deriv}/fsl_denoise/{subj}"
    proj_fsl_subj = f"{proj_deriv}/fsl_denoise"
    if not os.path.exists(proj_fsl_subj):
        os.makedirs(proj_fsl_subj)
    map_dest = {work_fsl_subj: proj_fsl_subj}

    # Source, destination for fmriprep, freesurfer
    for sess in sess_list:
        work_fp = f"{work_deriv}/fmriprep/{sess}/{subj}/*"
        proj_fp = f"{proj_deriv}/fmriprep/{subj}"
        if not os.path.exists(proj_fp):
            os.makedirs(proj_fp)
        map_dest[work_fp] = proj_fp

        work_fp_html = f"{work_deriv}/fmriprep/{sess}/{subj}.html"
        proj_fp_html = f"{proj_deriv}/fmriprep/{subj}_{sess}.html"
        map_dest[work_fp_html] = proj_fp_html

        work_fs = os.path.join(work_deriv, "freesurfer", sess, subj)
        proj_fs = os.path.join(proj_deriv, "freesurfer", sess)
        if not os.path.exists(proj_fs):
            os.makedirs(proj_fs)
        map_dest[work_fs] = proj_fs

    # Copy directories from work to group, then remove dir from work
    for src, dst in map_dest.items():
        _, _ = submit.submit_subprocess(
            True, f"cp -r {src} {dst} && rm -r {src}", "cp", log_dir
        )


class PullPush:
    """Interact with Keoki to get and send data.

    Download required files for preprocessing, send
    final files back to Keoki.

    Parameters
    ----------
    proj_dir : str, os.PathLike
        Location of project directory on group partition
    log_dir : str, os.PathLike
        Output location for log files and scripts
    user_name : str
        User name for DCC, labarserv2
    rsa_key : str, os.PathLike
        Location of RSA key for labarserv2
    keoki_path : str, os.PathLike
        Location of project directory on Keoki

    Methods
    -------
    pull_rawdata(subj, sess)
        Download rawdata from Keoki
    push_derivatives(sess_list)
        Upload preprocessed files to Keoki

    Example
    -------
    sync_data = PullPush(*args)
    nii_list = sync_data.pull_rawdata("sub-ER0009", "ses-day2")
    sync_data.push_derivatives(["ses-day2", "ses-day3"])

    Notes
    -----
    push_derivatives is required to run after pull_rawdata

    """

    def __init__(
        self,
        proj_dir,
        log_dir,
        user_name,
        rsa_key,
        keoki_path,
    ):
        """Initialize."""
        print("Initializing PullPush")
        self._dcc_proj = proj_dir
        self._user_name = user_name
        self._keoki_ip = "ccn-labarserv2.vm.duke.edu"
        self._keoki_proj = f"{self._user_name}@{self._keoki_ip}:{keoki_path}"
        self._rsa_key = rsa_key
        self._log_dir = log_dir

    def pull_rawdata(self, subj, sess):
        """Download subject, session rawdata from Keoki.

        Parameters
        ----------
        subj : str
            BIDS subject identifier
        sess : str
            BIDS session identifier

        Returns
        -------
        list
            Locations of downloaded NIfTI files

        """
        self._subj = subj
        self._sess = sess

        # Setup destination
        dcc_raw = os.path.join(self._dcc_proj, "rawdata", subj)
        if not os.path.exists(dcc_raw):
            os.makedirs(dcc_raw)

        # Identify source, pull data
        print(f"\tDownloading rawdata to : {dcc_raw}")
        keoki_raw = os.path.join(self._keoki_proj, "rawdata", subj, sess)
        raw_out, raw_err = self._submit_rsync(keoki_raw, dcc_raw)

        # Check setup
        raw_niis = sorted(
            glob.glob(f"{dcc_raw}/{sess}/**/*.nii.gz", recursive=True)
        )
        if not raw_niis:
            raise FileNotFoundError(
                "Error in Keoki->DCC rawdata file transfer:\n\n"
                + f"stdout:\t{raw_out}\n\nstderr:\t{raw_err}"
            )
        return raw_niis

    def push_derivatives(self, sess_list):
        """Send final derivatives to Keoki and clean DCC.

        Parameters
        ----------
        sess_list : list
            List of BIDS session identifier

        """
        self._sess_list = sess_list
        for step in ["fmriprep", "freesurfer", "fsl_denoise"]:
            self._dcc_step = os.path.join(
                self._dcc_proj, "derivatives", "pre_processing", step
            )
            self._keoki_step = os.path.join(
                self._keoki_proj, "derivatives", "pre_processing", step
            )
            push_meth = getattr(self, f"_push_{step}")
            push_meth()

    def _push_fmriprep(self):
        """Send fMRIPrep to Keoki."""
        print(f"\tUploading fMRIPrep for : {self._subj}")
        src_fp = os.path.join(self._dcc_step, f"{self._subj}*")
        _, _ = self._submit_rsync(src_fp, self._keoki_step)
        self._submit_rm(src_fp)

    def _push_freesurfer(self):
        """Send freesurfer to Keoki."""
        for self._sess in self._sess_list:
            print(f"\tUploading FreeSurfer for : {self._subj}, {self._sess}")
            src_fs = os.path.join(self._dcc_step, self._sess, self._subj)
            dst_fs = os.path.join(self._keoki_step, self._sess)
            _, _ = self._submit_rsync(src_fs, dst_fs)
            self._submit_rm(src_fs)

    def _push_fsl_denoise(self):
        """Send FSL preproc to Keoki."""
        print(f"\tUploading FSL preproc for : {self._subj}")
        src_fsl = os.path.join(self._dcc_step, self._subj)
        _, _ = self._submit_rsync(src_fsl, self._keoki_step)
        self._submit_rm(src_fsl)

    def _submit_rsync(self, src: str, dst: str) -> Tuple:
        """Execute rsync between DCC and labarserv2."""
        bash_cmd = f"""\
            rsync \
            -e 'ssh -i {self._rsa_key}' \
            -rauv {src} {dst}
        """
        job_out, job_err = submit.submit_subprocess(
            True,
            bash_cmd,
            f"{self._subj[-4:]}_{self._sess[4:]}_pullPush",
            self._log_dir,
        )
        return (job_out, job_err)

    def _submit_rm(self, rm_path: Union[str, os.PathLike]):
        """Remove file tree."""
        _, _ = submit.submit_subprocess(
            True, f"rm -r {rm_path}", "rm", self._log_dir
        )


class _FslCmds:
    """Collection of FSL commands."""

    def _cmd_tmean(
        self,
        in_epi: Union[str, os.PathLike],
        out_path: Union[str, os.PathLike],
    ) -> str:
        """TMean command."""
        return f"""\
            fslmaths \
                {in_epi} \
                -Tmean \
                {out_path}
        """

    def _cmd_tfilt(
        self,
        in_epi: Union[str, os.PathLike],
        out_path: Union[str, os.PathLike],
        bptf: int,
        in_tmean: Union[str, os.PathLike],
    ) -> str:
        """Temporal filtering command."""
        return f"""\
            fslmaths \
                {in_epi} \
                -bptf {bptf} -1 \
                -add {in_tmean} \
                {out_path}
        """

    def _cmd_scale(
        self,
        in_epi: Union[str, os.PathLike],
        out_path: Union[str, os.PathLike],
        med_value: float,
    ) -> str:
        """Scaling command."""
        mul_value = round(10000 / med_value, 6)
        return f"""\
            fslmaths \
                {in_epi} \
                -mul {mul_value} \
                {out_path}
        """


class _HelperMeths:
    """Helper methods for FSL, AFNI preprocessing."""

    def _chk_path(self, file_path: Union[str, os.PathLike]):
        """Raise error if file is missing."""
        if not os.path.exists(file_path):
            raise FileNotFoundError(
                f"Expected file or directory : {file_path}"
            )

    def set_subj(self, subj: str, out_dir: Union[str, os.PathLike]):
        """Set subject, output directory attributes."""
        self._chk_path(out_dir)
        self.subj = subj
        self.out_dir = out_dir

    def _submit_check(
        self, bash_cmd: str, out_path: Union[str, os.PathLike], job_name: str
    ) -> bool:
        """Submit shell command and return True if output is found."""
        stdout, stderr = submit.submit_subprocess(
            self._run_local,
            bash_cmd,
            job_name,
            self._log_dir,
            mem_gig=6,
        )

        # Give commands like tmean and scale time to write
        if not os.path.exists(out_path):
            time.sleep(120)
        if not os.path.exists(out_path):
            raise FileNotFoundError(
                f"Missing {job_name} output\n{stdout}\n{stderr}"
            )
        return True

    def _parse_epi(self, epi_path: Union[str, os.PathLike]) -> Tuple:
        """Return BIDS sub, ses, task, run, space, res, desc, suff values."""
        return os.path.basename(epi_path).split("_")

    def _job_name(self, epi_path: Union[str, os.PathLike], name: str) -> str:
        """Return job name, including session and run number."""
        _, sess, task, run, _, _, _, _ = self._parse_epi(epi_path)
        return f"{self.subj[6:]}_{sess[4:]}_{task[5:6]}_r{run[-1]}_{name}"

    def _get_out_path(
        self, in_epi: Union[str, os.PathLike], desc: str
    ) -> Union[str, os.PathLike]:
        """Return new output file path/name given desc."""
        subj, sess, task, run, space, res, _, suff = self._parse_epi(in_epi)
        return os.path.join(
            self.out_dir,
            f"{subj}_{sess}_{task}_{run}_{space}_{res}_{desc}_{suff}",
        )


class _FslMethods(_FslCmds, _HelperMeths):
    """Collection of FSL methods for processing EPI data."""

    def __init__(self, log_dir: Union[str, os.PathLike], run_local: bool):
        """Initialize."""
        self._chk_path(log_dir)
        if not isinstance(run_local, bool):
            raise TypeError("Unexpected type for run_local")
        self._log_dir = log_dir
        self._run_local = run_local

    def tmean(
        self,
        in_epi: Union[str, os.PathLike],
        desc: str = "desc-tmean",
    ) -> Union[str, os.PathLike]:
        """Make, return path to mean EPI timeseries NIfTI."""
        self._chk_path(in_epi)
        out_path = self._get_out_path(in_epi, desc)
        if os.path.exists(out_path):
            return out_path

        print("\tFinding mean timeseries")
        if self._submit_check(
            self._cmd_tmean(in_epi, out_path),
            out_path,
            self._job_name(in_epi, "tmean"),
        ):
            return out_path

    def bandpass(
        self,
        in_epi: Union[str, os.PathLike],
        in_tmean: Union[str, os.PathLike],
        desc: str = "desc-tfilt",
        bptf: int = 25,
    ) -> Union[str, os.PathLike]:
        """Make, return path to bandpass filtered EPI NIfTI."""
        self._chk_path(in_epi)
        if not isinstance(bptf, int):
            raise TypeError("Expected type int for bptf")
        out_path = self._get_out_path(in_epi, desc)
        if os.path.exists(out_path):
            return out_path

        print("\tBandpass filtering")
        if self._submit_check(
            self._cmd_tfilt(in_epi, out_path, bptf, in_tmean),
            out_path,
            self._job_name(in_epi, "band"),
        ):
            return out_path

    def scale(
        self,
        in_epi: Union[str, os.PathLike],
        med_value: float,
        desc: str = "desc-scale",
    ) -> Union[str, os.PathLike]:
        """Make, return path to scaled EPI NIfTI."""
        self._chk_path(in_epi)
        if not isinstance(med_value, float):
            raise TypeError("Expected med_value type float")

        out_path = self._get_out_path(in_epi, desc)
        if os.path.exists(out_path):
            return out_path

        print("\tScaling timeseries")
        if self._submit_check(
            self._cmd_scale(in_epi, out_path, med_value),
            out_path,
            self._job_name(in_epi, "scale"),
        ):
            return out_path

    def median(
        self,
        in_epi: Union[str, os.PathLike],
        mask_path: Union[str, os.PathLike],
    ) -> float:
        """Calculate median EPI voxel value."""
        self._chk_path(in_epi)
        self._chk_path(mask_path)

        print("\tCalculating median voxel value")
        bash_cmd = f"""
            fslstats \
                {in_epi} \
                -k {mask_path} \
                -p 50
        """
        job_sp = subprocess.Popen(bash_cmd, shell=True, stdout=subprocess.PIPE)
        job_out, job_err = job_sp.communicate()
        try:
            return float(job_out.decode("utf-8").split()[0])
        except IndexError:
            raise RuntimeError(
                f"""
            fslstats failed.
            stdout: {job_out}
            stderr: {job_err}
            """
            )


class _AfniCmds:
    """Collection of AFNI commands."""

    def _prepend_afni(self) -> list:
        """Return singularity call setup."""
        return [
            "singularity",
            "run",
            "--cleanenv",
            f"--bind {self.out_dir}:{self.out_dir}",
            f"--bind {self.out_dir}:/opt/home",
            self._sing_afni,
        ]

    def _cmd_mask_epi(
        self,
        in_epi: Union[str, os.PathLike],
        mask_path: Union[str, os.PathLike],
        work_mask: Union[str, os.PathLike],
        out_path: Union[str, os.PathLike],
    ) -> str:
        """Copy and 3dcalc command."""
        cp_list = ["cp", mask_path, work_mask, ";"]
        calc_list = [
            "3dcalc",
            f"-a {in_epi}",
            f"-b {work_mask}",
            "-float",
            f"-prefix {out_path}",
            "-expr 'a*step(b)'",
        ]
        return " ".join(cp_list + self._prepend_afni() + calc_list)

    def _cmd_smooth(
        self,
        in_epi: Union[str, os.PathLike],
        k_size: int,
        out_path: Union[str, os.PathLike],
    ) -> str:
        """3dmerge blur command."""
        smooth_list = [
            "3dmerge",
            f"-1blur_fwhm {k_size}",
            "-doall",
            f"-prefix {out_path}",
            in_epi,
        ]
        return " ".join(self._prepend_afni() + smooth_list)

    def _calc_median(self, median_txt: Union[str, os.PathLike]) -> float:
        """Deprecated; Calculate median from 3dROIstats columns."""
        # Strip stdout, column names from txt file, fourth column
        # contains median values for each volume.
        no_head = median_txt.replace("_median.txt", "_nohead.txt")
        # cut_head = 4 if self._run_local else 5
        cut_head = 4
        bash_cmd = f"""
            tail -n +{cut_head} {median_txt} > {no_head}
            awk \
                '{{total += $4; count++ }} END {{ print total/count }}' \
                {no_head}
        """
        job_sp = subprocess.Popen(bash_cmd, shell=True, stdout=subprocess.PIPE)
        job_out, job_err = job_sp.communicate()
        return float(job_out.decode("utf-8").split()[0])

    def _cmd_median(
        self,
        in_epi: Union[str, os.PathLike],
        mask_path: Union[str, os.PathLike],
        work_mask: Union[str, os.PathLike],
        out_path: Union[str, os.PathLike],
    ) -> str:
        """Deprecated; Median via 3dROIstats."""
        cp_list = ["cp", mask_path, work_mask, ";"]
        bash_list = [
            "3dROIstats",
            "-median",
            f"-mask {work_mask}",
            f"{in_epi}",
            f"> {out_path}",
        ]
        return " ".join(cp_list + self._prepend_afni() + bash_list)


class _AfniMethods(_AfniCmds, _HelperMeths):
    """Collection of AFNI methods for processing EPI data."""

    def __init__(
        self,
        log_dir: Union[str, os.PathLike],
        run_local: bool,
        sing_afni: Union[str, os.PathLike],
    ):
        """Initialize."""
        self._chk_path(sing_afni)
        self._log_dir = log_dir
        self._run_local = run_local
        self._sing_afni = sing_afni

    def mask_epi(
        self,
        in_epi: Union[str, os.PathLike],
        mask_path: Union[str, os.PathLike],
        desc: str = "desc-masked",
    ) -> Union[str, os.PathLike]:
        """Make, return path to masked EPI NIfTI."""
        self._chk_path(in_epi)
        self._chk_path(mask_path)
        out_path = self._get_out_path(in_epi, desc)
        if os.path.exists(out_path):
            return out_path

        print("\tMasking EPI")
        work_mask = os.path.join(self.out_dir, os.path.basename(mask_path))
        if self._submit_check(
            self._cmd_mask_epi(in_epi, mask_path, work_mask, out_path),
            out_path,
            self._job_name(in_epi, "mask"),
        ):
            return out_path

    def afni_median(
        self,
        in_epi: Union[str, os.PathLike],
        mask_path: Union[str, os.PathLike],
    ) -> float:
        """Calcualte median EPI voxel value.

        Deprecated.

        """
        self._chk_path(in_epi)
        self._chk_path(mask_path)

        print("\tCalculating median voxel value")
        _, _, _task, _run, _, _, _, _ = self._parse_epi(in_epi)
        out_path = os.path.join(
            self.out_dir, f"tmp_{_task}_r{_run[-1]}_median.txt"
        )
        work_mask = os.path.join(self.out_dir, os.path.basename(mask_path))
        _ = self._submit_check(
            self._cmd_median(in_epi, mask_path, work_mask, out_path),
            out_path,
            self._job_name(in_epi, "median"),
        )
        med_value = self._calc_median(out_path)
        return med_value

    def smooth(
        self,
        in_epi: Union[str, os.PathLike],
        k_size: int,
        desc: str = "desc-scale",
    ) -> Union[str, os.PathLike]:
        """Spatially smooth EPI data."""
        self._chk_path(in_epi)
        if not type(k_size) == int:  # noqa : E721
            raise TypeError("Expected type int for k_size")
        out_path = self._get_out_path(in_epi, desc)
        if os.path.exists(out_path):
            return out_path

        print("\tSmoothing EPI dataset")
        if self._submit_check(
            self._cmd_smooth(in_epi, k_size, out_path),
            out_path,
            self._job_name(in_epi, "smooth"),
        ):
            return out_path


class ExtraPreproc(_AfniMethods, _FslMethods):
    """Collection of AFNI and FSL commands for processing EPI data.

    Used for conducting extra preprocessing steps following fMRIPrep.

    Inherits _AfniMethods, _FslMethods.

    Parameters
    ----------
    log_dir : str, os.PathLike
        Location of directory for logging
    run_local : bool
        Whether workflow is running locally (labarserv2) or remotely (DCC)
    sing_afni : str, os.PathLike
        Location of AFNI singularity image

    Methods
    -------
    bandpass()
        Temporally filter data
    mask_epi()
        Multiple EPI by binary mask
    median()
        Calculate median voxel value
    scale()
        Scale timeseries
    set_subj()
        Set required subject-level attributes
    smooth()
        Smooth timeseries
    tmean()
        Calculate timeseries mean

    """

    def __init__(
        self,
        log_dir: Union[str, os.PathLike],
        run_local: bool,
        sing_afni: Union[str, os.PathLike],
    ):
        """Initialize."""
        print("Initializing ExtraPreproc.")
        _AfniMethods.__init__(self, log_dir, run_local, sing_afni)
        _FslMethods.__init__(self, log_dir, run_local)
