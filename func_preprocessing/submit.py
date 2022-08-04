"""Functions for controlling sbatch and subprocess submissions."""
import os
import sys
import subprocess
import textwrap


def sbatch(
    bash_cmd,
    job_name,
    log_dir,
    num_hours=1,
    num_cpus=4,
    mem_gig=4,
    env_input=None,
):
    """Schedule child SBATCH job.

    Parameters
    ----------
    bash_cmd : str
        Bash syntax, work to schedule
    job_name : str
        Name for scheduler
    log_dir : Path
        Location of output dir for writing logs
    num_hours : int
        Walltime to schedule
    num_cpus : int
        Number of CPUs required by job
    mem_gig : int
        Job RAM requirement for each CPU (GB)
    env_input : dict, None
        Extra environmental variables required by processes
        e.g. singularity reqs

    Returns
    -------
    tuple
        [0] = stdout of subprocess
        [1] = stderr of subprocess
    """
    sbatch_cmd = f"""
        sbatch \
        -J {job_name} \
        -t {num_hours}:00:00 \
        --cpus-per-task={num_cpus} \
        --mem-per-cpu={mem_gig}000 \
        -o {log_dir}/out_{job_name}.log \
        -e {log_dir}/err_{job_name}.log \
        --wait \
        --wrap="{bash_cmd}"
    """
    print(f"Submitting SBATCH job:\n\t{sbatch_cmd}\n")
    h_sp = subprocess.Popen(
        sbatch_cmd, shell=True, stdout=subprocess.PIPE, env=env_input
    )
    h_out, h_err = h_sp.communicate()
    h_sp.wait()
    return (h_out, h_err)


def schedule_subj(
    subj,
    raw_dir,
    work_fp,
    work_fs,
    work_fsl,
    sing_fmriprep,
    sing_tf,
    sing_afni,
    fs_license,
    log_dir,
    proj_home,
    proj_work,
):
    """Write and schedule pipeline.

    Generate a python script that controls preprocessing. Submit
    the work on schedule resources.

    Currently controls FreeSurfer, fMRIPrep and other preprocessing
    steps to follow (Jul 28, 2022).

    Parameters
    ----------


    Returns
    -------
    tuple
        [0] subprocess stdout
        [1] subprocess stderr
    """
    sbatch_cmd = f"""\
        #!/bin/env {sys.executable}

        #SBATCH --job-name=p{subj[4:]}
        #SBATCH --output={log_dir}/p{subj[4:]}.txt
        #SBATCH --time=20:00:00
        #SBATCH --mem=4000

        import os
        import sys
        from func_preprocessing import preprocess

        # Run fMRIPrep
        fp_dict = preprocess.fmriprep(
            "{subj}",
            "{raw_dir}",
            "{work_fp}",
            "{work_fs}",
            "{sing_fmriprep}",
            "{sing_tf}",
            "{fs_license}",
            "{log_dir}",
            "{proj_home}",
            "{proj_work}",
        )

        # Finish preprocessing with FSL
        preprocess.fsl_preproc(
            "{work_fsl}",
            fp_dict,
            "{sing_afni}",
            "{subj}",
            "{log_dir}",
        )
    """
    sbatch_cmd = textwrap.dedent(sbatch_cmd)
    py_script = f"{log_dir}/run_fmriprep_{subj}.py"
    with open(py_script, "w") as ps:
        ps.write(sbatch_cmd)
    h_sp = subprocess.Popen(
        f"sbatch {py_script}",
        shell=True,
        stdout=subprocess.PIPE,
        env=os.environ,
    )
    h_out, h_err = h_sp.communicate()
    print(f"{h_out.decode('utf-8')}\tfor {subj}")
    return (h_out, h_err)
