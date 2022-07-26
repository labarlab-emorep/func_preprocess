r"""Title.

Desc.

Example
-------
code_dir=/hpc/home/nmm51/projects/func_preprocessing
log_dir=/hpc/group/labarlab/EmoRep_BIDS/logs
sbatch \
    --job-name=mainPP \
    --output=$log_dir \
    --mem-per-cpu=4000 \
    func_preprocessing \
        -c $code_dir \
        -s sub-ER0009
"""
import os
import sys
import glob
import textwrap
import subprocess
from argparse import ArgumentParser, RawTextHelpFormatter
from func_preprocessing import preprocess


def _schedule_subj(subj_deriv, subj):
    """Title.

    Desc.
    """
    sbatch_cmd = f"""\
        #!/bin/env {sys.executable}

        #SBATCH --job-name=p{subj[4:]}
        #SBATCH --output={subj_deriv}/parent_out.txt
        #SBATCH --time=20:00:00
        #SBATCH --mem=4000

        import os
        import sys
        # sys.path.append("{code_dir}")
        from func_preprocessing import preprocess

        fs_exists = preprocess.freesurfer(
            {deriv_dir},
            {subj_deriv},
            {subj_t1},
            {subj},
            {sess},
        )
        if not fs_exists:
            raise FileNotFoundError

        preprocess.fmriprep()
    """
    sbatch_cmd = textwrap.dedent(sbatch_cmd)
    sbatch_script = os.path.join(subj_deriv, "run_fs_fmriprep.py")
    with open(py_script, "w") as ps:
        ps.write(sbatch_cmd)
    h_sp = subprocess.Popen(
        f"sbatch {py_script}", shell=True, stdout=subprocess.PIPE
    )
    h_out, h_err = h_sp.communicate()
    return (h_out, h_err)


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
        "-c",
        "--code-dir",
        required=True,
        type=str,
        help="Path to clone of this repo",
    )
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


def main():
    """Title."""
    args = _get_args().parse_args()
    subj_list = args.sub_list
    code_dir = args.code_dir
    proj_dir = args.proj_dir

    # Make variables, lists
    deriv_dir = os.path.join(proj_dir, "derivatives")
    subj_raw = os.path.join(proj_dir, "rawdata", subj)
    sess_list = [x for x in os.listdir(subj_raw) if "ses*" in x]

    for subj in subj_list:
        for sess in sess_list:
            subj_t1 = glob.glob(f"{sess}/anat/*_T1w.nii.gz")
            subj_deriv = os.path.join(deriv_dir, "freesurfer", subj, sess)
            if not os.path.exists(subj_deriv):
                os.makedirs(subj_deriv)


if __name__ == "__main__":

    # Require proj env
    env_foud = [x for x in sys.path if "emorep" in x]
    if not env_found:
        print("\nERROR: missing required project environment 'emorep'")
        print("\tHint: $ labar_env emorep\n")
        sys.exit(1)
    main()
