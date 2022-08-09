"""Methods for controlling sbatch and subprocess submissions."""
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

    Notes
    -----
    Avoid using double quotes in <bash_cmd> (particularly relevant
    with AFNI) to avoid conflict with --wrap syntax.
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
    proj_raw,
    proj_deriv,
    work_deriv,
    sing_fmriprep,
    fs_license,
    fd_thresh,
    ignore_fmaps,
    sing_afni,
    log_dir,
):
    """Write and schedule pipeline.

    Generate a python script that controls preprocessing. Submit
    the work on schedule resources.

    Parameters
    ----------
    subj : str
        BIDS subject identifier
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

    Returns
    -------
    tuple
        [0] subprocess stdout
        [1] subprocess stderr
    """
    # Setup software derivatives dirs, for working
    work_fp = os.path.join(work_deriv, "fmriprep")
    work_fs = os.path.join(work_deriv, "freesurfer")
    work_fsl = os.path.join(work_deriv, "fsl")
    for h_dir in [work_fp, work_fs, work_fsl]:
        if not os.path.exists(h_dir):
            os.makedirs(h_dir)

    # Setup software derivatives dirs, for storage
    proj_fp = os.path.join(proj_deriv, "fmriprep")
    proj_fsl = os.path.join(proj_deriv, "fsl")
    for h_dir in [proj_fp, proj_fsl]:
        if not os.path.exists(h_dir):
            os.makedirs(h_dir)

    # Write parent python script
    sbatch_cmd = f"""\
        #!/bin/env {sys.executable}

        #SBATCH --job-name=p{subj[4:]}
        #SBATCH --output={log_dir}/par{subj[4:]}.txt
        #SBATCH --time=30:00:00
        #SBATCH --mem=4000

        import os
        import sys
        from func_preprocessing import preprocess

        # Run fMRIPrep
        fp_dict = preprocess.fmriprep(
            "{subj}",
            "{proj_raw}",
            "{work_deriv}",
            "{sing_fmriprep}",
            "{fs_license}",
            {fd_thresh},
            "{ignore_fmaps}",
            "{log_dir}",
        )

        # Finish preprocessing with FSL, AFNI
        preprocess.fsl_preproc(
            "{work_fsl}",
            fp_dict,
            "{sing_afni}",
            "{subj}",
            "{log_dir}",
        )

        # Clean up
        preprocess.copy_clean(
            "{proj_deriv}",
            "{work_deriv}",
            "{subj}"
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
    )
    h_out, h_err = h_sp.communicate()
    print(f"{h_out.decode('utf-8')}\tfor {subj}")
    return (h_out, h_err)
