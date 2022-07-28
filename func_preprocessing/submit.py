"""Title.

Desc.
"""
import subprocess


def sbatch(
    bash_cmd,
    job_name,
    log_dir,
    num_hours=1,
    num_cpus=4,
    mem_gig=4,
    env_input=None,
):
    """Title.

    Desc.
    """
    sbatch_cmd = f"""
        sbatch \
        -J {job_name} \
        -t {num_hours}:00:00 \
        --cpus-per-task={num_cpus} \
        --mem-per-cpu={mem_gig}000 \
        -o {log_dir}/{job_name}.out \
        -e {log_dir}/{job_name}.err \
        --wait \
        --wrap="{bash_cmd}"
    """
    print(f"Submitting SBATCH job:\n\t{sbatch_cmd}\n")
    h_sp = subprocess.Popen(sbatch_cmd, shell=True, stdout=subprocess.PIPE)
    h_out, h_err = h_sp.communicate()
    h_sp.wait()
    return (h_out, h_err)
