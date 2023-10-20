import os


def test_exec_fs(fixt_setup):
    ref_subid = fixt_setup["subid"]
    ref_sess = fixt_setup["sess"]

    test_outfile = fixt_setup["fs_object"]._exec_fs(ref_sess)
    assert test_outfile == os.path.join(
        fixt_setup["derivs_path"],
        "freesurfer",
        ref_sess,
        ref_subid,
        "mri/aparc+aseg.mgz",
    )


def test_setup(fixt_setup):
    ref_mgz_path = os.path.join(
        fixt_setup["derivs_path"],
        "freesurfer",
        fixt_setup["sess"],
        fixt_setup["subid"],
        "mri/orig/001.mgz",
    )
    fixt_setup["fs_object"]._sess = fixt_setup["sess"]
    fixt_setup["fs_object"]._work_fs = os.path.join(
        fixt_setup["derivs_path"], "freesurfer", fixt_setup["sess"]
    )
    test_mgzpath = fixt_setup["fs_object"]._setup()
    assert ref_mgz_path == test_mgzpath
