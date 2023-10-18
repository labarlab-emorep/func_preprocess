import os
from func_preprocess import preprocess


def test_exec_fs(fixt_setup):
    ref_subid = fixt_setup["subid"]
    ref_sess = fixt_setup["sess"]

    fs_object = preprocess.RunFreeSurfer(
        ref_subid, fixt_setup["proj_raw"], fixt_setup["fs_path"], str, True
    )
    test_outfile = fs_object._exec_fs(ref_sess)
    assert test_outfile == os.path.join(
        fixt_setup["fs_path"],
        "freesurfer",
        ref_sess,
        ref_subid,
        "mri/aparc+aseg.mgz",
    )
