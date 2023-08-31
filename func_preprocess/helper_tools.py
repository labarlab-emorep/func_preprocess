"""Methods to support preprocessing.

copy_clean : copy intermediates in work to group, purge work
PullPush : down/upload relevant files from/to Keoki
FslMethods : FSL methods for preprocessing, inherited
AfniFslMethods : FSL and AFNI methods for preprocessing

"""
import os
import time
import glob
import subprocess
from typing import Union, Tuple
from func_preprocess import submit


def copy_clean(subj, sess_list, proj_deriv, work_deriv, log_dir):
    """Housekeeping for data.

    Delete unneeded files from work_deriv, copy remaining to
    the proj_deriv location.

    Parameters
    ----------
    subj : str
        BIDS subject
    sess_list : list
        BIDS session identifiers
    proj_deriv : path
        Project derivative location, e.g.
        /hpc/group/labarlab/EmoRep_BIDS/derivatives
    work_deirv : path
        Working derivative location, e.g.
        /work/foo/EmoRep_BIDS/derivatives
    log_dir : path
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

    for src, dst in map_dest.items():
        _, _ = submit.submit_subprocess(
            True, f"cp -r {src} {dst} && rm -r {src}", "cp", log_dir
        )


class PullPush:
    """Interact with Keoki to get and send data.

    Download required files for preprocessing, send
    final files back to Keoki.

    Methods
    -------
    pull_rawdata(subj, sess)
    push_derivatives()

    Example
    -------
    sync_data = PullPush(*args)
    sync_data.pull_rawdata("sub-ER0009", "ses-day2")
    sync_data.sess = "ses-all"
    sync_data.push_derivatives()

    """

    def __init__(
        self,
        proj_dir,
        log_dir,
        user_name,
        rsa_key,
        keoki_path,
    ):
        """Initialize.

        Parameters
        ----------
        proj_dir : str, os.PathLike
            Location of project directory on group partition
        log_dir : path
            Output location for log files and scripts
        user_name : str
            User name for DCC, labarserv2
        rsa_key : str, os.PathLike
            Location of RSA key for labarserv2
        keoki_path : str, os.PathLike
            Location of project directory on Keoki

        """
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

        Attributes
        ----------
        sess : str
            BIDS session identifier, used to keep job logs straight

        """
        self._subj = subj
        self.sess = sess

        # Setup destination
        dcc_raw = os.path.join(self._dcc_proj, "rawdata", subj)
        if not os.path.exists(dcc_raw):
            os.makedirs(dcc_raw)

        # Identify source, pull data
        print(f"\tDownloading rawdata to : {dcc_raw}")
        keoki_raw = os.path.join(self._keoki_proj, "rawdata", subj, sess)
        raw_out, raw_err = self._submit_rsync(keoki_raw, dcc_raw)

        # Check setup
        cnt_raw = glob.glob(f"{dcc_raw}/{sess}/**/*.nii.gz", recursive=True)
        if not cnt_raw:
            raise FileNotFoundError(
                "Error in Keoki->DCC rawdata file transfer:\n\n"
                + f"stdout:\t{raw_out}\n\nstderr:\t{raw_err}"
            )

    def push_derivatives(self):
        """Send final derivatives to Keoki and clean DCC."""
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
        src_fp = os.path.join(self._dcc_step, f"{self._subj}*")
        _, _ = self._submit_rsync(src_fp, self._keoki_step)
        self._submit_rm(src_fp)

    def _push_freesurfer(self):
        """Send freesurfer to Keoki."""
        for day in ["ses-day2", "ses-day3"]:
            src_fs = os.path.join(self._dcc_step, day, self._subj)
            dst_fs = os.path.join(self._keoki_step, day)
            _, _ = self._submit_rsync(src_fs, dst_fs)
            self._submit_rm(src_fs)

    def _push_fsl_denoise(self):
        """Send FSL preproc to Keoki."""
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
            f"{self._subj[-4:]}_{self.sess[4:]}_pullPush",
            self._log_dir,
        )
        return (job_out, job_err)

    def _submit_rm(self, rm_path: Union[str, os.PathLike]):
        """Remove file tree."""
        _, _ = submit.submit_subprocess(
            True, f"rm -r {rm_path}", "rm", self._log_dir
        )


class FslMethods:
    """Collection of FSL commands for processing EPI data.

    Used for conducting extra preprocessing steps following fMRIPrep.

    Methods
    -------
    bandpass(in_epi, in_tmean, out_name, bptf=25)
        Apply temporal filtering to EPI data
    median(in_epi, mask_path)
        Deprecated.
        Derive median EPI voxel value
    scale(in_epi, out_name, median_value)
        Scale EPI timeseries by median value
    set_subj(subj, out_dir)
        Set subject, output directory attributes
    tmean(in_epi, out_name)
        Calculate mean timeseries

    Example
    -------
    The example below conducts the following steps for two participants:
        -   Find mean of EPI timeseries
        -   Bandpass filter EPI timeseries
        -   Calculate median EPI voxel value (FslMethods.median is outdated)
        -   Scale EPI timeseries

    fm = FslMethods("/path/to/log/dir", True)
    for subj in ["sub-ER0009", "sub-ER0010"]:
        fm.set_subj(subj, f"/path/to/{subj}/output/dir")
        tmean_path = fm.tmean(
            f"/path/to/{subj}/input.nii.gz", "output_tmean.nii.gz"
        )
        band_path = fm.bandpass(
            f"/path/to/{subj}/input.nii.gz",
            tmean_path,
            "output_bandpass.nii.gz",
        )
        med_value = fm.median(band_path, f"/path/to/{subj}/brain_mask.nii.gz")
        scale_path = fm.scale(band_path, "output_scale.nii.gz", med_value)

    """

    def __init__(self, log_dir: Union[str, os.PathLike], run_local: bool):
        """Initialize."""
        self._chk_path(log_dir)
        if not isinstance(run_local, bool):
            raise TypeError("Unexpected type for run_local")

        print("Initializing FslMethods")
        self._log_dir = log_dir
        self._run_local = run_local

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
        """Return BIDS sub, ses, task, and run values."""
        out_list = []
        for field in ["sub-", "ses-", "task-", "run-"]:
            bids_value = (
                os.path.basename(epi_path).split(field)[1].split("_")[0]
            )
            out_list.append(bids_value)
        return tuple(out_list)

    def _job_name(self, epi_path: Union[str, os.PathLike], name: str) -> str:
        """Return job name, including session and run number."""
        _, _sess, _task, _run = self._parse_epi(epi_path)
        return f"{self.subj[-4:]}_{_sess}_{_task}_r{_run[-1]}_{name}"

    def tmean(
        self,
        in_epi: Union[str, os.PathLike],
        out_name: str,
    ) -> Union[str, os.PathLike]:
        """Make, return path to mean EPI timeseries NIfTI."""
        self._chk_path(in_epi)
        out_path = os.path.join(self.out_dir, out_name)
        if os.path.exists(out_path):
            return out_path

        print("\tFinding mean timeseries")
        bash_cmd = f"""
            fslmaths \
                {in_epi} \
                -Tmean \
                {out_path}
        """
        if self._submit_check(
            bash_cmd, out_path, self._job_name(in_epi, "tmean")
        ):
            return out_path

    def bandpass(
        self,
        in_epi: Union[str, os.PathLike],
        in_tmean: Union[str, os.PathLike],
        out_name: str,
        bptf: int = 25,
    ) -> Union[str, os.PathLike]:
        """Make, return path to bandpass filtered EPI NIfTI."""
        self._chk_path(in_epi)
        if not isinstance(bptf, int):
            raise TypeError("Expected type int for bptf")

        out_path = os.path.join(self.out_dir, out_name)
        if os.path.exists(out_path):
            return out_path

        print("\tBandpass filtering")
        bash_cmd = f"""
            fslmaths \
                {in_epi} \
                -bptf {bptf} -1 \
                -add {in_tmean} \
                {out_path}
        """
        if self._submit_check(
            bash_cmd, out_path, self._job_name(in_epi, "band")
        ):
            return out_path

    def scale(
        self,
        in_epi: Union[str, os.PathLike],
        out_name: str,
        med_value: float,
    ) -> Union[str, os.PathLike]:
        """Make, return path to scaled EPI NIfTI."""
        self._chk_path(in_epi)
        if not isinstance(med_value, float):
            raise TypeError("Expected med_value type float")

        out_path = os.path.join(self.out_dir, out_name)
        if os.path.exists(out_path):
            return out_path

        print("\tScaling timeseries")
        mul_value = round(10000 / med_value, 6)
        bash_cmd = f"""
            fslmaths \
                {in_epi} \
                -mul {mul_value} \
                {out_path}
        """
        if self._submit_check(
            bash_cmd, out_path, self._job_name(in_epi, "scale")
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


class AfniFslMethods(FslMethods):
    """Collection of AFNI and FSL commands for processing EPI data.

    Used for conducting extra preprocessing steps following fMRIPrep.

    Inherits FslMethods.

    Methods
    -------
    mask_epi(in_epi, mask_path, out_name)
        Multiply an EPI NIfTI with a mask
    afni_median(in_epi, mask_path)
        Deprecated.
        Derive median voxel value from EPI file

    Example
    -------
    The example below conducts the following steps for two participants:
        -   Find mean of EPI timeseries
        -   Bandpass filter EPI timeseries
        -   Mask filtered EPI
        -   Calculate median EPI voxel value
        -   Scale EPI timeseries

    afm = AfniFslMethods("/path/to/log/dir", True, "/path/to/afni.simg")
    for subj in ["sub-ER0009", "sub-ER0010"]:
        afm.set_subj(subj, f"/path/to/{subj}/output/dir")
        tmean_path = afm.tmean(
            f"/path/to/{subj}/input.nii.gz", "output_tmean.nii.gz"
        )
        band_path = afm.bandpass(
            f"/path/to/{subj}/input.nii.gz",
            tmean_path,
            "output_bandpass.nii.gz",
        )
        run_mask = f"/path/to/{subj}/brain_mask.nii.gz"
        band_masked = afm.mask_epi(
            band_path, run_mask, "output_bandpass_masked.nii.gz"
        )
        med_value = fm.median(band_masked, run_mask)
        scale_path = fm.scale(
            band_masked, "output_scale.nii.gz", med_value
        )

    """

    def __init__(
        self,
        log_dir: Union[str, os.PathLike],
        run_local: bool,
        sing_afni: Union[str, os.PathLike],
    ):
        """Initialize."""
        print("Initializing AfniMethods")
        super().__init__(log_dir, run_local)
        self._chk_path(sing_afni)
        self._sing_afni = sing_afni

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

    def mask_epi(
        self,
        in_epi: Union[str, os.PathLike],
        mask_path: Union[str, os.PathLike],
        out_name: str,
    ) -> Union[str, os.PathLike]:
        """Make, return path to masked EPI NIfTI."""
        self._chk_path(in_epi)
        self._chk_path(mask_path)
        out_path = os.path.join(self.out_dir, out_name)
        if os.path.exists(out_path):
            return out_path

        print("\tMasking EPI")
        work_mask = os.path.join(self.out_dir, os.path.basename(mask_path))
        cp_list = ["cp", mask_path, work_mask, ";"]
        calc_list = [
            "3dcalc",
            f"-a {in_epi}",
            f"-b {work_mask}",
            "-float",
            f"-prefix {out_path}",
            "-expr 'a*step(b)'",
        ]
        bash_cmd = " ".join(cp_list + self._prepend_afni() + calc_list)
        if self._submit_check(
            bash_cmd, out_path, self._job_name(in_epi, "mask")
        ):
            return out_path

    def _calc_median(self, median_txt: Union[str, os.PathLike]) -> float:
        """Calculate median from 3dROIstats columns."""
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
        _, _, _task, _run = self._parse_epi(in_epi)
        out_path = os.path.join(
            self.out_dir, f"tmp_{_task}_r{_run[-1]}_median.txt"
        )
        work_mask = os.path.join(self.out_dir, os.path.basename(mask_path))
        cp_list = ["cp", mask_path, work_mask, ";"]
        bash_list = [
            "3dROIstats",
            "-median",
            f"-mask {work_mask}",
            f"{in_epi}",
            f"> {out_path}",
        ]
        bash_cmd = " ".join(cp_list + self._prepend_afni() + bash_list)
        _ = self._submit_check(
            bash_cmd, out_path, self._job_name(in_epi, "median")
        )
        med_value = self._calc_median(out_path)
        return med_value

    def smooth(
        self, in_epi: Union[str, os.PathLike], k_size: int, out_name: str
    ):
        """Spatially smooth EPI data."""
        self._chk_path(in_epi)
        if not type(k_size) == int:
            raise TypeError("Expected type int for k_size")
        out_path = os.path.join(self.out_dir, out_name)
        if os.path.exists(out_path):
            return out_path

        print("\tSmoothing EPI dataset")
        smooth_list = [
            "3dmerge",
            f"-1blur_fwhm {k_size}",
            "-doall",
            f"-prefix {out_path}",
            in_epi,
        ]
        bash_cmd = " ".join(self._prepend_afni() + smooth_list)
        if self._submit_check(
            bash_cmd, out_path, self._job_name(in_epi, "smooth")
        ):
            return out_path
