r"""Conduct preprocessing for EmoRep.

Download required data from Keoki, preprocess EPI data via FreeSurfer,
fMRIPrep, and extra FSL and AFNI steps. Generates scaled and smoothed
EPI output. Upload files to Keoki. Sessions are treated independently
for FreeSurfer and fMRIPrep.

The workflow writes files to <work_dir>, and when finished purges
some intermediates and saves final files to <proj_dir>.
Specifically, final files are saved to:
    <proj-dir>/derivatives/pre_processing/[fmriprep|freesurfer|fsl_denoise]

Log files and scripts are generated for review and troubleshooting,
and written to:
    <work_dir>/logs/func_preprocess_<timestamp>

Notes
-----
- AFNI and fMRIPrep are executed from singularity, FSL and
    FreeSurfer from a subprocess call.

- Requires the following environmental global variables:
    -   SING_AFNI = path to AFNI singularity
    -   SING_FMRIPREP = path to fMRIPrep singularity
    -   SINGULARITYENV_TEMPLATEFLOW_HOME = path to templateflow for fmriprep
    -   FS_LICENSE = path to FreeSurfer license
    -   FSLDIR = path to FSL binaries
    -   RSA_LS2 = path to RSA key for labarserv2

- FSL should be also be configured in the environment.

- Long file paths can result in a buffer overflow of FreeSurfer tools!

Examples
--------
func_preprocess -s sub-ER0009
func_preprocess \
    -s sub-ER0009 sub-ER0010 \
    --sess ses-day2 \
    --fd-thresh 0.2 \
    --ignore-fmaps

"""

# %%
import os
import sys
import time
import textwrap
from datetime import datetime
from argparse import ArgumentParser, RawTextHelpFormatter
import func_preprocess._version as ver
from func_preprocess import submit


# %%
def _get_args():
    """Get and parse arguments."""
    ver_info = f"\nVersion : {ver.__version__}\n\n"
    parser = ArgumentParser(
        description=ver_info + __doc__, formatter_class=RawTextHelpFormatter
    )
    parser.add_argument(
        "--fd-thresh",
        type=float,
        default=0.5,
        help=textwrap.dedent(
            """\
            Framewise displacement threshold
            (default : %(default)s)
            """
        ),
    )
    parser.add_argument(
        "--ignore-fmaps",
        action="store_true",
        help="Whether fmriprep will ignore fmaps",
    )
    parser.add_argument(
        "--proj-dir",
        type=str,
        default="/hpc/group/labarlab/EmoRep/Exp2_Compute_Emotion/data_scanner_BIDS",  # noqa: E501
        help=textwrap.dedent(
            """\
            Path to BIDS-formatted project directory
            (default : %(default)s)
            """
        ),
    )

    parser.add_argument(
        "--sess",
        nargs="+",
        default=["ses-day2", "ses-day3"],
        choices=["ses-day2", "ses-day3"],
        help=textwrap.dedent(
            """\
            List of session IDs to submit for pre-processing
            (default : %(default)s)
            """
        ),
        type=str,
    )

    required_args = parser.add_argument_group("Required Arguments")
    required_args.add_argument(
        "-s",
        "--subj",
        nargs="+",
        help="List of subject IDs to submit for pre-processing",
        type=str,
        required=True,
    )

    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(0)

    return parser


# %%
def main():
    """Trigger workflow for each subject."""

    # Capture CLI arguments
    args = _get_args().parse_args()
    subj_list = args.subj
    proj_dir = args.proj_dir
    ignore_fmaps = args.ignore_fmaps
    fd_thresh = args.fd_thresh
    sess_list = args.sess

    # Check run_local, work_deriv, and proj_dir. Set paths.
    if not os.path.exists(proj_dir):
        raise FileNotFoundError(f"Expected to find directory : {proj_dir}")
    proj_raw = os.path.join(proj_dir, "rawdata")
    proj_deriv = os.path.join(proj_dir, "derivatives/pre_processing")

    # Setup work directory, for intermediates and logs
    work_deriv = os.path.join("/work", os.environ["USER"], "EmoRep")
    now_time = datetime.now()
    log_dir = os.path.join(
        work_deriv,
        f"logs/func_preproc_{now_time.strftime('%y%m%d_%H%M')}",
    )
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Submit workflow
    for subj in subj_list:
        submit.schedule_subj(
            subj,
            sess_list,
            proj_raw,
            proj_deriv,
            work_deriv,
            fd_thresh,
            ignore_fmaps,
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

# %%
