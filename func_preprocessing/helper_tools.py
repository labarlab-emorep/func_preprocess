"""Methods for FSL and AFNI commands."""
import os
import subprocess
from typing import Union
from func_preprocessing import submit


class FslMethods:
    """Title."""

    def __init__(self, log_dir, run_local):
        """Title."""
        print("Initializing FslMethods")
        self._log_dir = log_dir
        self._run_local = run_local

    def set_subj(self, subj: str, out_dir: Union[str, os.PathLike]):
        """Title."""
        self._subj = subj
        self._out_dir = out_dir

    def _submit_check(
        self, bash_cmd: str, out_path: Union[str, os.PathLike], job_name: str
    ) -> bool:
        """Submit shell command and return True if output detected."""
        stdout, stderr = submit.submit_subprocess(
            self._run_local,
            bash_cmd,
            f"{self._subj[7:]}{job_name}",
            self._log_dir,
            mem_gig=6,
        )
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
        """Return path to timeseries mean NIfTI."""
        out_path = os.path.join(self._out_dir, out_name)
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
        """Return path to bandpass filtered NIfTI."""
        out_path = os.path.join(self._out_dir, out_name)
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
        """Return path to scaled EPI."""
        out_path = os.path.join(self._out_dir, out_name)
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


# def median():
#     bash_cmd = f"""
#         med_value=$(fslstats \
#             {out_dir}/{run_preproc} \
#             -k {brain_mask} \
#             -p 50)

#         scale=$(echo 10000/$med_value | bc -l)
#         mul_value=$(printf '%.6f \\n' $scale)

#         fslmaths \
#             {out_dir}/{run_preproc} \
#             -mul $mul_value \
#             {out_path}
#     """
#     _, _ = submit.submit_subprocess(
#         run_local,
#         bash_cmd,
#         f"{subj[7:]}_scale",
#         log_dir,
#         mem_gig=6,
#     )


class AfniFslTools(FslMethods):
    """Title.

    Inherits FslMethods.

    """

    def __init__(self, log_dir, run_local, sing_afni):
        """Title."""
        print("Initializing AfniMethods")
        super().__init__(log_dir, run_local)
        self._sing_afni = sing_afni

    def _prepend_afni(self) -> list:
        """Return singularity call."""
        return [
            "singularity",
            "run",
            "--cleanenv",
            f"--bind {self._out_dir}:{self._out_dir}",
            f"--bind {self._out_dir}:/opt/home",
            self._sing_afni,
        ]

    def mask_epi(
        self,
        epi_path: Union[str, os.PathLike],
        mask_path: Union[str, os.PathLike],
        out_name: str,
    ) -> Union[str, os.PathLike]:
        """Return path to masked EPI NIfTI."""
        out_path = os.path.join(self._out_dir, out_name)
        if os.path.exists(out_path):
            return out_path

        print("\tMasking EPI")
        work_mask = os.path.join(self._out_dir, os.path.basename(mask_path))
        cp_list = ["cp", mask_path, work_mask, ";"]
        calc_list = [
            "3dcalc",
            f"-a {epi_path}",
            f"-b {work_mask}",
            "-float",
            f"-prefix {out_path}",
            "-expr 'a*b'",
        ]
        bash_cmd = " ".join(cp_list + self._prepend_afni() + calc_list)
        if self._submit_check(bash_cmd, out_path, "mask"):
            return out_path

    def _calc_median(self, median_txt: Union[str, os.PathLike]) -> float:
        """Title."""
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
        """Return median voxel value."""
        print("\tCalculating median voxel value")
        out_path = os.path.join(self._out_dir, "tmp_median.txt")
        work_mask = os.path.join(self._out_dir, os.path.basename(mask_path))
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
        return self._calc_median(out_path)
