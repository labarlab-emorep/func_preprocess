r"""Conduct preprocessing for EmoRep.

Run data through FreeSurfer and fMRIPrep, then conduct temporal
filtering via FSL and AFNI. Work is conducted in
"/work/$(whoami)/EmoRep_BIDS/derivatives", and final files are saved
to "<proj_dir>/derivatives/<fmriprep|fsl>/<subj>".

For each subject, a parent job "p<subj>" is submitted that controls
the pipeline. Named subprocess "<subj>foo>" are spawned when
additional resources are required.

Log files and scripts written to:
    "/work/$(whoami)/EmoRep_BIDS/derivatives/logs/func_pp_<timestamp>"

Examples
--------
func_preprocessing -s sub-ER0009 --ignore-fmaps

func_preprocessing \
    -s sub-ER0009 sub-ER0010 \
    --proj-dir /hpc/group/labarlab/foo
"""
# %%
import os
import sys
import time
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
        "--ignore-fmaps",
        action="store_true",
        help=textwrap.dedent(
            """\
            Whether fmriprep will ignore fmaps,
            True if "--ignore-fmap" else False.
            """
        ),
    )
    parser.add_argument(
        "--fd-thresh",
        type=float,
        default=0.3,
        help=textwrap.dedent(
            """\
            Framewise displacement threshold
            (default : %(default)s)
            """
        ),
    )
    parser.add_argument(
        "--proj-dir",
        type=str,
        default="/hpc/group/labarlab/EmoRep_BIDS",
        help=textwrap.dedent(
            """\
            Path to BIDS-formatted project directory
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
    """Setup working environment."""

    # Capture CLI arguments
    args = _get_args().parse_args()
    subj_list = args.sub_list
    proj_dir = args.proj_dir
    ignore_fmaps = args.ignore_fmaps
    fd_thresh = args.fd_thresh

    # Setup group project directory, paths
    proj_raw = os.path.join(proj_dir, "rawdata")
    proj_deriv = os.path.join(proj_dir, "derivatives")

    # Get environmental vars
    sing_afni = os.environ["SING_AFNI"]
    sing_fmriprep = os.environ["SING_FMRIPREP"]
    user_name = os.environ["USER"]
    fs_license = os.environ["FS_LICENSE"]

    # Setup work directory, for intermediates
    proj_name = os.path.basename(proj_dir)
    work_deriv = os.path.join("/work", user_name, proj_name, "derivatives")
    now_time = datetime.now()
    log_dir = os.path.join(
        work_deriv, f"logs/func_pp_{now_time.strftime('%y-%m-%d_%H:%M')}"
    )
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Submit jobs for subj_list
    for subj in subj_list:
        _, _ = submit.schedule_subj(
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
        )
        time.sleep(3)


if __name__ == "__main__":

    # Require proj env
    env_found = [x for x in sys.path if "emorep" in x]
    if not env_found:
        print("\nERROR: missing required project environment 'emorep'.")
        print("\tHint: $labar_env emorep\n")
        sys.exit(1)
    main()
