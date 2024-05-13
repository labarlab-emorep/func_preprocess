"""Methods for controlling sbatch and subprocess submissions.

submit_subprocess : submit bash commands via subprocess
schedule_subprocess : submit bash commands to SLURM scheduler
schedule_subj : generate and submit a python preprocessing script

"""

import sys
import subprocess
import textwrap


def submit_subprocess(bash_cmd: str) -> tuple:
    """Submit bash subprocess."""
    job_sp = subprocess.Popen(
        bash_cmd,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    job_out, job_err = job_sp.communicate()
    job_sp.wait()
    return (job_out, job_err)


def schedule_subprocess(
    bash_cmd,
    job_name,
    log_dir,
    num_hours=1,
    num_cpus=1,
    mem_gig=4,
):
    """Run bash commands as scheduled subprocesses.

    Parameters
    ----------
    bash_cmd : str
        Bash syntax, work to schedule
    job_name : str
        Name for scheduler
    log_dir : str, os.PathLike
        Location of output dir for writing logs
    num_hours : int, optional
        Walltime to schedule
    num_cpus : int, optional
        Number of CPUs required by job
    mem_gig : int, optional
        Job RAM requirement for each CPU (GB)

    Returns
    -------
    tuple
        [0] = stdout of subprocess
        [1] = stderr of subprocess

    Notes
    -----
    Avoid using double quotes in <bash_cmd> (particularly relevant
    with AFNI) to avoid conflict with --wrap syntax.

    """

    sbatch_cmd = f"""\
        sbatch \
        -J {job_name} \
        -t {num_hours}:00:00 \
        --cpus-per-task={num_cpus} \
        --mem={mem_gig}G \
        -o {log_dir}/out_{job_name}.log \
        -e {log_dir}/err_{job_name}.log \
        --wait \
        --wrap="{bash_cmd}"
    """
    print(f"Submitting SBATCH job:\n\t{sbatch_cmd}\n")
    return submit_subprocess(sbatch_cmd)


def schedule_subj(
    subj,
    sess_list,
    proj_raw,
    proj_deriv,
    work_deriv,
    fd_thresh,
    ignore_fmaps,
    log_dir,
    schedule_job=True,
):
    """Schedule pipeline on compute cluster.

    Generate a python script that runs preprocessing workflow.
    Submit the work on schedule resources.

    Parameters
    ----------
    subj : str
        BIDS subject identifier
    sess_list : list
        BIDS session identifiers
    proj_raw :str, os.PathLike
        Location of project rawdata
    proj_deriv : str, os.PathLike
        Location of project derivatives
    work_deriv : str, os.PathLike
        Location of work derivatives
    fd_thresh : float
        Threshold for framewise displacement
    ignore_fmaps : bool
        Whether to incorporate fmaps in preprocessing
    log_dir : str, os.PathLike
        Location for writing logs
    schedule_job : bool, optional
        Whether to submit job to SLURM scheduler,
        used for testing.

    """
    sbatch_cmd = f"""\
        #!/bin/env {sys.executable}

        #SBATCH --job-name=p{subj[4:]}
        #SBATCH --output={log_dir}/par{subj[4:]}.txt
        #SBATCH --time=60:00:00
        #SBATCH --cpus-per-task=4
        #SBATCH --mem-per-cpu=6G

        import os
        import sys
        from func_preprocess import workflows

        workflows.run_preproc(
            "{subj}",
            {sess_list},
            "{proj_raw}",
            "{proj_deriv}",
            "{work_deriv}",
            {fd_thresh},
            {ignore_fmaps},
            "{log_dir}",
        )

    """
    sbatch_cmd = textwrap.dedent(sbatch_cmd)
    py_script = f"{log_dir}/run_preprocess_{subj}.py"
    with open(py_script, "w") as ps:
        ps.write(sbatch_cmd)

    if schedule_job:
        h_sp = subprocess.Popen(
            f"sbatch {py_script}",
            shell=True,
            stdout=subprocess.PIPE,
        )
        h_out, h_err = h_sp.communicate()
        print(f"{h_out.decode('utf-8')}\tfor {subj}")
