r"""Title.

Desc.

Example
-------
func_preprocessing -s sub-ER0009
"""
# %%
import os
import sys
import glob
import textwrap
import subprocess
from datetime import datetime
from fnmatch import fnmatch
from argparse import ArgumentParser, RawTextHelpFormatter


# %%
def _schedule_subj(
    subj,
    subj_raw,
    work_fp,
    work_fs,
    sing_fmriprep,
    sing_tf,
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
    raw_path = os.path.dirname(subj_raw)
    sbatch_cmd = f"""\
        #!/bin/env {sys.executable}

        #SBATCH --job-name=p{subj[4:]}
        #SBATCH --output={log_dir}/p{subj[4:]}.txt
        #SBATCH --time=20:00:00
        #SBATCH --mem=4000

        import os
        import sys
        from func_preprocessing import preprocess

        # # Run FreeSurfer
        # work_fs = "work_dir/freesurfer_sess[4:]"
        # work_orig = os.path.join(work_fs, "{subj}/mri/orig")
        # if not os.path.exists(work_orig):
        #     os.makedirs(work_orig)

        # fs_exists = preprocess.freesurfer(
        #     work_fs,
        #     "subj_t1",
        #     "{subj}",
        #     "sess",
        #     "{log_dir}",
        # )
        # if not fs_exists:
        #     raise FileNotFoundError

        # Run fMRIPrep
        preprocess.fmriprep(
            "{subj}",
            "{raw_path}",
            "{work_fp}",
            "{work_fs}",
            "{sing_fmriprep}",
            "{sing_tf}",
            "{fs_license}",
            "{log_dir}",
            "{proj_home}",
            "{proj_work}",
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


# %%fnma
def _get_args():
    """Get and parse arguments."""
    parser = ArgumentParser(
        description=__doc__, formatter_class=RawTextHelpFormatter
    )
    parser.add_argument(
        "--proj-dir",
        type=str,
        default="/hpc/group/labarlab/EmoRep_BIDS",
        help=textwrap.dedent(
            """\
            path to BIDS-formatted project directory
            (default : %(default)s)
            """
        ),
    )

    required_args = parser.add_argument_group("Required Arguments")
    required_args.add_argument(
        "-s",
        "--sub-list",
        nargs="+",
        help=textwrap.dedent(
            """\
            List of subject IDs to submit for pre-processing,
            e.g. "-s ER4414" or "--sub-list ER4414 ER4415 ER4416".
            """
        ),
        type=str,
        required=True,
    )

    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1)

    return parser


# %%
def main():
    """Title."""
    # # For testing
    # subj = "sub-ER0009"
    # proj_dir = "/hpc/group/labarlab/EmoRep_BIDS"
    # sing_fmriprep = "/hpc/group/labarlab/research_bin/sing_images/fmriprep-22.0.0"
    # sing_tf = "/hpc/home/nmm51/research_bin/templateflow"
    # user_name = "nmm51"
    # fs_license = "/hpc/home/nmm51/research_bin/license.txt"

    args = _get_args().parse_args()
    subj_list = args.sub_list
    proj_dir = args.proj_dir

    # Make variables, lists
    deriv_dir = os.path.join(proj_dir, "derivatives")

    # Get environmental vars
    proj_home = os.environ["PROJ_HOME"]
    proj_work = os.environ["PROJ_WORK"]
    sing_fmriprep = os.environ["sing_fmriprep"]
    sing_tf = os.environ["SINGULARITYENV_TEMPLATEFLOW_HOME"]
    user_name = os.environ["USER"]
    fs_license = os.environ["FS_LICENSE"]

    # Setup working, log directories
    proj_name = os.path.basename(proj_dir)
    work_dir = os.path.join("/work", user_name, proj_name, "derivatives")
    work_fs = os.path.join(work_dir, "freesurfer")
    work_fp = os.path.join(work_dir, "fmriprep")
    now_time = datetime.now()
    log_dir = os.path.join(
        work_dir, f"logs/func_pp_{now_time.strftime('%y-%m-%d_%H:%M')}"
    )
    for h_dir in [work_fs, log_dir]:
        if not os.path.exists(h_dir):
            os.makedirs(h_dir)

    # Submit jobs for subj_list
    for subj in subj_list:
        subj_raw = os.path.join(proj_dir, "rawdata", subj)
        _, _ = _schedule_subj(
            subj,
            subj_raw,
            work_fp,
            work_fs,
            sing_fmriprep,
            sing_tf,
            fs_license,
            log_dir,
            proj_home,
            proj_work,
        )
        # sess_list = [x for x in os.listdir(subj_raw) if fnmatch(x, "ses-*")]
        # for sess in sess_list:
        #     subj_t1 = glob.glob(f"{subj_raw}/{sess}/anat/*_T1w.nii.gz")[0]
        #     _schedule_subj(work_dir, subj, sess, subj_t1, log_dir)


if __name__ == "__main__":

    # Require proj env
    env_found = [x for x in sys.path if "emorep" in x]
    if not env_found:
        print("\nERROR: missing required project environment 'emorep'.")
        print("\tHint: $labar_env emorep\n")
        sys.exit(1)
    main()
