r"""Title.

Desc.

Example
-------
func_preprocessing -s sub-ER0009
"""
# %%
import os
import sys
import textwrap
from datetime import datetime
from argparse import ArgumentParser, RawTextHelpFormatter
from func_preprocessing import submit


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

    # Capture CLI arguments
    args = _get_args().parse_args()
    subj_list = args.sub_list
    proj_dir = args.proj_dir

    # Setup paths to rawdata, derivatives
    raw_dir = os.path.join(proj_dir, "rawdata")
    deriv_dir = os.path.join(proj_dir, "derivatives")
    deriv_fp = os.path.join(deriv_dir, "fmriprep")
    deriv_fsl = os.path.join(deriv_dir, "fsl")
    for h_dir in [deriv_fp, deriv_fsl]:
        if not os.path.exists(h_dir):
            os.makedirs(h_dir)

    # Get environmental vars
    proj_home = os.environ["PROJ_HOME"]
    proj_work = os.environ["PROJ_WORK"]
    sing_afni = os.environ["sing_afni"]
    sing_fmriprep = os.environ["sing_fmriprep"]
    sing_tf = os.environ["SINGULARITYENV_TEMPLATEFLOW_HOME"]
    user_name = os.environ["USER"]
    fs_license = os.environ["FS_LICENSE"]

    # Setup paths to /work/user/project
    proj_name = os.path.basename(proj_dir)
    work_dir = os.path.join("/work", user_name, proj_name, "derivatives")
    work_fs = os.path.join(work_dir, "freesurfer")
    work_fp = os.path.join(work_dir, "fmriprep")
    work_fsl = os.path.join(work_dir, "fsl")
    now_time = datetime.now()
    log_dir = os.path.join(
        work_dir, f"logs/func_pp_{now_time.strftime('%y-%m-%d_%H:%M')}"
    )
    for h_dir in [work_fs, log_dir]:
        if not os.path.exists(h_dir):
            os.makedirs(h_dir)

    # Submit jobs for subj_list
    for subj in subj_list:
        _, _ = submit.schedule_subj(
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
        )


if __name__ == "__main__":

    # Require proj env
    env_found = [x for x in sys.path if "emorep" in x]
    if not env_found:
        print("\nERROR: missing required project environment 'emorep'.")
        print("\tHint: $labar_env emorep\n")
        sys.exit(1)
    main()
