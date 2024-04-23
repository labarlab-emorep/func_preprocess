"""Methods for controlling sbatch and subprocess submissions.

submit_subprocess : submit or schedule bash commands via subprocess
schedule_subj : generate and submit a python preprocessing script

"""

import sys
import subprocess
import textwrap


def submit_subprocess(
    run_local,
    bash_cmd,
    job_name,
    log_dir,
    num_hours=1,
    num_cpus=1,
    mem_gig=4,
    env_input=None,
):
    """Run bash commands as subprocesses.

    Schedule a SBATCH subprocess when run_local=False, otherwise
    submit normal subprocess.

    Parameters
    ----------
    run_local : bool
        Whether to run job locally
    bash_cmd : str
        Bash syntax, work to schedule
    job_name : str
        Name for scheduler
    log_dir : Path
        Location of output dir for writing logs
    num_hours : int, optional
        Walltime to schedule
    num_cpus : int, optional
        Number of CPUs required by job
    mem_gig : int, optional
        Job RAM requirement for each CPU (GB)
    env_input : dict, optional
        Extra environmental variables required by processes
        e.g. singularity reqs

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

    def _bash_sp(job_cmd: str) -> tuple:
        """Submit bash as subprocess."""
        job_sp = subprocess.Popen(
            job_cmd,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env_input,
        )
        job_out, job_err = job_sp.communicate()
        job_sp.wait()
        return (job_out, job_err)

    def _write_sbatch(job_cmd: str) -> tuple:
        """Schedule child SBATCH job."""
        sbatch_cmd = f"""
            sbatch \
            -J {job_name} \
            -t {num_hours}:00:00 \
            --cpus-per-task={num_cpus} \
            --mem={mem_gig}G \
            -o {log_dir}/out_{job_name}.log \
            -e {log_dir}/err_{job_name}.log \
            --wait \
            --wrap="{job_cmd}"
        """
        print(f"Submitting SBATCH job:\n\t{sbatch_cmd}\n")
        job_out, job_err = _bash_sp(sbatch_cmd)
        return (job_out, job_err)

    if run_local:
        job_out, job_err = _bash_sp(bash_cmd)
    else:
        job_out, job_err = _write_sbatch(bash_cmd)
    return (job_out, job_err)


def schedule_subj(
    subj,
    sess_list,
    proj_raw,
    proj_deriv,
    work_deriv,
    sing_fmriprep,
    tplflow_dir,
    fs_license,
    fd_thresh,
    ignore_fmaps,
    sing_afni,
    log_dir,
    run_local,
    user_name,
    rsa_key,
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
    proj_raw : path
        Location of project rawdata, e.g.
        /hpc/group/labarlab/EmoRep_BIDS/rawdata
    proj_deriv : path
        Location of project derivatives, e.g.
        /hpc/group/labarlab/EmoRep_BIDS/derivatives
    work_deriv : path
        Location of work derivatives, e.g.
        /work/foo/EmoRep_BIDS/derivatives
    sing_fmriprep : path, str
        Location of fmiprep singularity image
    tplflow_dir : path, str
        Clone location of templateflow
    fs_license : path, str
        Location of FreeSurfer license
    fd_thresh : float
        Threshold for framewise displacement
    ignore_fmaps : bool
        Whether to incorporate fmaps in preprocessing
    sing_afni : path, str
        Location of afni singularity iamge
    log_dir : path
        Location for writing logs
    run_local : bool
        Whether job, subprocesses are run locally
    user_name : str
        User name for DCC, labarserv2
    rsa_key : str, os.PathLike
        Location of RSA key for labarserv2
    schedule_job : bool, optional
        Whether to submit job to SLURM scheduler,
        used for testing

    Returns
    -------
    None

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
            "{sing_fmriprep}",
            "{tplflow_dir}",
            "{fs_license}",
            {fd_thresh},
            {ignore_fmaps},
            "{sing_afni}",
            "{log_dir}",
            {run_local},
            "{user_name}",
            "{rsa_key}",
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
