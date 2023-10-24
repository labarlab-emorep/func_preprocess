import pytest
import os
import shutil
from func_preprocess import preprocess


@pytest.fixture(scope="session")
def fixt_setup():
    # Hardcode variables for specific testing
    subid = "sub-ER0009"
    sess = "ses-day2"
    # (derivs_path corresponds to work_deriv in preprocess.py module)
    derivs_path = (
        "/mnt/keoki/experiments2/EmoRep/Exp2_Compute_Emotion/"
        + "data_scanner_BIDS/derivatives/pre_processing"
    )
    proj_raw = (
        "/mnt/keoki/experiments2/EmoRep/Exp2_Compute_Emotion/"
        + "data_scanner_BIDS/rawdata"
    )

    yield {
        "subid": subid,
        "sess": sess,
        "derivs_path": derivs_path,
        "proj_raw": proj_raw,
    }


@pytest.fixture(scope="session")
def fixt_preprocess(fixt_setup):
    fs_object = preprocess.RunFreeSurfer(
        fixt_setup["subid"],
        fixt_setup["proj_raw"],
        fixt_setup["derivs_path"],
        str,
        True,
    )
    fp_object = preprocess.RunFmriprep(
        fixt_setup["subid"],
        fixt_setup["proj_raw"],
        fixt_setup["derivs_path"],
        "",
        "",
        "/mnt/keoki/experiments2/EmoRep/Exp2_Compute_Emotion/"
        + "code/func_preprocess/LICENSE",
        "",
        True,
        "",
        True,
    )
    yield {"fs_object": fs_object, "fp_object": fp_object}


@pytest.fixture(scope="session")
def fixt_get_output(fixt_setup):
    test_directory = os.path.join(
        "/mnt/keoki/experiments2/EmoRep"
        + "/Exp2_Compute_Emotion/code"
        + "/unit_test/func_preprocess"
    )

    ref_data_path = os.path.join(
        test_directory,
        "fmriprep",
        fixt_setup["sess"],
        fixt_setup["subid"],
        fixt_setup["sess"],
        "func",
    )

    # need to know wildcard syntax to change "movies" to something generic
    # same with lines 56 and 62 of test_preprocess
    test_preproc_bold_path = (
        f"{fixt_setup['derivs_path']}/fmriprep/"
        + f"{fixt_setup['subid']}/{fixt_setup['sess']}/"
        + f"func/{fixt_setup['subid']}_{fixt_setup['sess']}_task-movies"
        + "_run-01_space-MNI152NLin6Asym_res-2_desc-preproc_bold.nii.gz"
    )
    test_mask_bold_path = (
        f"{fixt_setup['derivs_path']}/fmriprep/"
        + f"{fixt_setup['subid']}/{fixt_setup['sess']}/"
        + f"func/{fixt_setup['subid']}_{fixt_setup['sess']}_task-movies"
        + "_run-01_space-MNI152NLin6Asym_res-2_desc-brain_mask.nii.gz"
    )

    specific_file = os.path.basename(test_preproc_bold_path)
    copied_file = os.path.join(ref_data_path, specific_file)

    # Avoid repeating work
    if not os.path.exists(test_directory):
        os.makedirs(test_directory)
    if not os.path.exists(ref_data_path):
        os.makedirs(ref_data_path)
    if not os.path.exists(copied_file):
        shutil.copy(test_preproc_bold_path, ref_data_path)
        shutil.copy(test_mask_bold_path, ref_data_path)

    yield {"test_directory": test_directory, "ref_data_path": ref_data_path}
