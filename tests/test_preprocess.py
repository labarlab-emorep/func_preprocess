import os
import shutil

# RunFreeSurfer Class:


def test_exec_fs(fixt_setup, fixt_preprocess):
    ref_subid = fixt_setup["subid"]
    ref_sess = fixt_setup["sess"]
    test_outfile = fixt_preprocess["fs_object"]._exec_fs(ref_sess)
    ref_outfile = os.path.join(
        fixt_setup["derivs_path"],
        "freesurfer",
        ref_sess,
        ref_subid,
        "mri/aparc+aseg.mgz",
    )
    assert test_outfile == ref_outfile


def test_setup(fixt_setup, fixt_preprocess):
    ref_mgz_path = os.path.join(
        fixt_setup["derivs_path"],
        "freesurfer",
        fixt_setup["sess"],
        fixt_setup["subid"],
        "mri/orig/001.mgz",
    )
    fixt_preprocess["fs_object"]._sess = fixt_setup["sess"]
    fixt_preprocess["fs_object"]._work_fs = os.path.join(
        fixt_setup["derivs_path"], "freesurfer", fixt_setup["sess"]
    )
    test_mgzpath = fixt_preprocess["fs_object"]._setup()
    assert ref_mgz_path == test_mgzpath


# RunFmriPrep Class:


def test_exec_fp(fixt_setup, fixt_preprocess):
    test_check_file = fixt_preprocess["fp_object"]._exec_fp(fixt_setup["sess"])
    ref_check_file = os.path.join(
        fixt_setup["derivs_path"],
        "fmriprep",
        fixt_setup["sess"],
        f"{fixt_setup['subid']}.html",
    )
    assert test_check_file == ref_check_file


def test_get_output(fixt_setup, fixt_preprocess, fixt_get_output):
    fixt_preprocess["fp_object"]._work_deriv = fixt_get_output[
        "test_directory"
    ]
    test_dictionary = fixt_preprocess["fp_object"].get_output()

    preproc_bold_item = (
        f"{fixt_get_output['ref_data_path']}/{fixt_setup['subid']}"
        + f"_{fixt_setup['sess']}_task-movies_run-01_space-"
        + "MNI152NLin6Asym_res-2_desc-preproc_bold.nii.gz"
    )

    mask_bold_item = (
        f"{fixt_get_output['ref_data_path']}/{fixt_setup['subid']}"
        + f"_{fixt_setup['sess']}_task-movies_run-01_space-"
        + "MNI152NLin6Asym_res-2_desc-brain_mask.nii.gz"
    )

    assert preproc_bold_item in test_dictionary["preproc_bold"]
    assert mask_bold_item in test_dictionary["mask_bold"]

    # Clean up
    shutil.rmtree(fixt_get_output["ref_data_path"])
