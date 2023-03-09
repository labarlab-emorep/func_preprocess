r"""Conduct preprocessing for EmoRep.

Run participants through fMRIPrep preprocessing and then add
additional steps in FSL for denoising and masking.

The pipeline workflow writes files to <work_dir>, and when finished
purges some intermediates and saves final files to <proj_dir>.
Specifically, final files are saved to:
    <proj-dir>/derivatives/pre_processing/[fmriprep|freesurfer|fsl_denoise]

Log files and scripts are generated for review and troubleshooting,
and written to:
    <work_dir>/logs/func_pp_<timestamp>

Notes
-----
- AFNI and fMRIPrep are executed from singularity images, FSL from
    a subprocess call.

- Requires the following environmental global variables:
    -   SING_AFNI = path to AFNI singularity
    -   SING_FMRIPREP = path to fMRIPrep singularity
    -   SINGULARITYENV_TEMPLATEFLOW_HOME = path to templateflow for fmriprep
    -   FS_LICENSE = path to FreeSurfer license
    -   FSLDIR = path to FSL binaries

- FSL should be also be configured in the environment.

- When running remotely, parent job "p<subj>" is submitted for each subject
    that controls the workflow. Named subprocesses "<subj>foo" are spawned
    when additional resources are required.

Examples
--------
func_preprocessing -s sub-ER0009

func_preprocessing \
    -s sub-ER0009 sub-ER0010 \
    --no-freesurfer \
    --fd-thresh 0.2 \
    --ignore-fmaps

func_preprocessing \
    -s sub-ER0009 sub-ER0016 \
    --run-local \
    --proj-dir /path/to/local/project \
    --work-dir /path/to/local/work

"""
# %%
import os
import sys
import time
import textwrap
from datetime import datetime
from argparse import ArgumentParser, RawTextHelpFormatter


# %%
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
            True if "--ignore-fmaps" else False.
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
        "--no-freesurfer",
        action="store_true",
        help=textwrap.dedent(
            """\
            Whether to use the --fs-no-reconall option with fmriprep,
            True if "--no--freesurfer" else False.
            """
        ),
    )
    parser.add_argument(
        "--proj-dir",
        type=str,
        default="/hpc/group/labarlab/EmoRep/Exp2_Compute_Emotion/data_scanner_BIDS",  # noqa: E501
        help=textwrap.dedent(
            """\
            Required when --run-local.
            Path to BIDS-formatted project directory
            (default : %(default)s)
            """
        ),
    )
    parser.add_argument(
        "--run-local",
        action="store_true",
        help=textwrap.dedent(
            """\
            Run pipeline locally on labarserv2 rather than on
            default DCC.
            True if "--run-local" else False.
            """
        ),
    )
    parser.add_argument(
        "--work-dir",
        type=str,
        default=None,
        help=textwrap.dedent(
            """\
            Required when --run-local.
            Path to derivatives location on work partition, for processing
            intermediates. If None, the work-dir will setup in
            /work/<user>/EmoRep/derivatives. Be mindful of path lengths
            to avoid a buffer overflow in FreeSurfer.
            (default : %(default)s)
            """
        ),
    )
    parser.add_argument(
        "-v",
        "--version",
        help="Print package version",
        action="store_true",
    )

    required_args = parser.add_argument_group("Required Arguments")
    required_args.add_argument(
        "-s",
        "--sub-list",
        nargs="+",
        help=textwrap.dedent(
            """\
            List of subject IDs to submit for pre-processing,
            e.g. "-s sub-ER4414" or "--sub-list sub-ER4414 sub-ER4415"
            """
        ),
        type=str,
        required=True,
    )

    if len(sys.argv) == 2 and (
        sys.argv[1] == "-v" or sys.argv[1] == "--version"
    ):
        import func_preprocessing._version as ver

        print(ver.__version__)
        sys.exit(0)

    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(0)

    return parser


# %%
def main():
    """Setup working environment."""

    # Capture CLI arguments
    args = _get_args().parse_args()
    subj_list = args.sub_list
    proj_dir = args.proj_dir
    work_deriv = args.work_dir
    ignore_fmaps = args.ignore_fmaps
    no_freesurfer = args.no_freesurfer
    fd_thresh = args.fd_thresh
    run_local = args.run_local

    # Check run_local, work_deriv, and proj_dir. Set paths.
    if run_local and not work_deriv:
        raise ValueError("Option --work-deriv required with --run-local.")
    if run_local and not os.path.exists(work_deriv):
        raise FileNotFoundError(f"Expected to find directory : {work_deriv}")
    if not os.path.exists(proj_dir):
        raise FileNotFoundError(f"Expected to find directory : {proj_dir}")
    proj_raw = os.path.join(proj_dir, "rawdata")
    proj_deriv = os.path.join(proj_dir, "derivatives/pre_processing")

    # Get, check environmental vars
    sing_afni = os.environ["SING_AFNI"]
    sing_fmriprep = os.environ["SING_FMRIPREP"]
    fs_license = os.environ["FS_LICENSE"]
    tplflow_dir = os.environ["SINGULARITYENV_TEMPLATEFLOW_HOME"]
    if not run_local:
        user_name = os.environ["USER"]

    try:
        os.environ["FSLDIR"]
    except KeyError:
        print("Missing required global variable FSLDIR")
        sys.exit(1)

    # Setup work directory, for intermediates and logs
    if not work_deriv:
        work_deriv = os.path.join(
            "/work",
            user_name,
            "EmoRep/pre_processing",
        )
    now_time = datetime.now()
    log_dir = os.path.join(
        os.path.dirname(work_deriv),
        f"logs/func_pp_{now_time.strftime('%y-%m-%d_%H:%M')}",
    )
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Get, submit appropriate workflow method
    if run_local:
        from func_preprocessing.workflows import run_preproc as wf_obj
    else:
        from func_preprocessing.submit import schedule_subj as wf_obj

    for subj in subj_list:
        wf_obj(
            subj,
            proj_raw,
            proj_deriv,
            work_deriv,
            sing_fmriprep,
            tplflow_dir,
            fs_license,
            fd_thresh,
            ignore_fmaps,
            no_freesurfer,
            sing_afni,
            log_dir,
            run_local,
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
