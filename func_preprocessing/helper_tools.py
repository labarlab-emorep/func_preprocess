"""Methods for FSL and AFNI commands."""
import os
import time
import subprocess
from typing import Union
from func_preprocessing import submit


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
            f"{self.subj[7:]}{job_name}",
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
        if self._submit_check(bash_cmd, out_path, "tmean"):
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
        if self._submit_check(bash_cmd, out_path, "band"):
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
        if self._submit_check(bash_cmd, out_path, "scale"):
            return out_path

    def median(
        self,
        in_epi: Union[str, os.PathLike],
        mask_path: Union[str, os.PathLike],
    ) -> float:
        """Calculate median EPI voxel value.

        DEPRECATED.

        Notes
        -----
        fslstats requires a finicky interactive shell, difficult
            to implement on DCC.

        """
        self._chk_path(in_epi)
        self._chk_path(mask_path)

        bash_cmd = f"""
            fslstats \
                {in_epi} \
                -k {mask_path} \
                -p 50
        """
        job_sp = subprocess.Popen(bash_cmd, shell=True, stdout=subprocess.PIPE)
        job_out, job_err = job_sp.communicate()
        return float(job_out.decode("utf-8").split()[0])


class AfniFslMethods(FslMethods):
    """Collection of AFNI and FSL commands for processing EPI data.

    Used for conducting extra preprocessing steps following fMRIPrep.

    Inherits FslMethods.

    Methods
    -------
    mask_epi(in_epi, mask_path, out_name)
        Multiply an EPI NIfTI with a mask
    median(in_epi, mask_path)
        Derive median voxel value from EPI file

    Example
    -------
    The example below conducts the following steps for two participants:
        -   Find mean of EPI timeseries
        -   Bandpass filter EPI timeseries
        -   Mask filtered EPI
        -   Calculate median EPI voxel value
        -   Scale EPI timeseries

    afm = AfniFslMethods("/path/to/log/dir", True)
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
            "-expr 'a*b'",
        ]
        bash_cmd = " ".join(cp_list + self._prepend_afni() + calc_list)
        if self._submit_check(bash_cmd, out_path, "mask"):
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

    def median(
        self,
        in_epi: Union[str, os.PathLike],
        mask_path: Union[str, os.PathLike],
    ) -> float:
        """Calcualte median EPI voxel value."""
        self._chk_path(in_epi)
        self._chk_path(mask_path)

        print("\tCalculating median voxel value")
        out_path = os.path.join(self.out_dir, "tmp_median.txt")
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
        _ = self._submit_check(bash_cmd, out_path, "median")
        med_value = self._calc_median(out_path)
        return med_value
