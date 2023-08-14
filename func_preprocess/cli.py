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

- FSL should be also be configured in the environment.

- When running remotely, parent job "p<subj>" is submitted for each subject
    that controls the workflow. Named subprocesses "<subj>foo" are spawned
    when additional resources are required.

Examples
--------
func_preprocess -s sub-ER0009 --rsa-key $RSA_LS2

func_preprocess \
    -s sub-ER0009 sub-ER0010 \
    --ses-list ses-day2 \
    --rsa-key $RSA_LS2 \
    --fd-thresh 0.2 \
    --ignore-fmaps

projDir=/mnt/keoki/experiments2/EmoRep/Exp2_Compute_Emotion/data_scanner_BIDS
workDir=${projDir}/derivatives/pre_processing
func_preprocess \
    --run-local \
    --proj-dir $projDir \
    --work-dir $workDir \
    -s sub-ER0009 sub-ER0016

"""
# %%
import os
import sys
import time
import textwrap
from datetime import datetime
from argparse import ArgumentParser, RawTextHelpFormatter
import func_preprocess._version as ver


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
        help=textwrap.dedent(
            """\
            Whether fmriprep will ignore fmaps,
            True if "--ignore-fmaps" else False.
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
        "--rsa-key",
        type=str,
        help="Required on DCC; location of labarserv2 RSA key",
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
        "--ses-list",
        nargs="+",
        default=["ses-day2", "ses-day3"],
        help=textwrap.dedent(
            """\
            List of session IDs to submit for pre-processing
            (default : %(default)s)
            """
        ),
        type=str,
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

    required_args = parser.add_argument_group("Required Arguments")
    required_args.add_argument(
        "-s",
        "--sub-list",
        nargs="+",
        help=textwrap.dedent(
            """\
            List of subject IDs to submit for pre-processing
            """
        ),
        type=str,
        required=True,
    )

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
    fd_thresh = args.fd_thresh
    run_local = args.run_local
    rsa_key = args.rsa_key
    ses_list = args.ses_list

    # Check run_local, work_deriv, and proj_dir. Set paths.
    if run_local and not work_deriv:
        raise ValueError("Option --work-deriv required with --run-local.")
    if run_local and not os.path.exists(work_deriv):
        raise FileNotFoundError(f"Expected to find directory : {work_deriv}")
    if not os.path.exists(proj_dir):
        raise FileNotFoundError(f"Expected to find directory : {proj_dir}")
    if not run_local and rsa_key is None:
        raise ValueError("RSA key required on DCC")
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
        f"logs/func_preproc_{now_time.strftime('%y%m%d_%H%M')}",
    )
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Get, submit appropriate workflow method
    if run_local:
        from func_preprocess.workflows import run_preproc as wf_obj
    else:
        from func_preprocess.submit import schedule_subj as wf_obj

    for subj in subj_list:
        wf_args = [
            subj,
            ses_list,
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
        ]
        if not run_local:
            wf_args.append(user_name)
            wf_args.append(rsa_key)
        wf_obj(*wf_args)
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
