import pytest
import os
import json


@pytest.mark.preproc
def test_RunFreeSurfer():
    pass


@pytest.mark.preproc
def test_RunFmriprep_write_filter(fixt_preproc):
    fixt_preproc.run_fp._write_filter()
    with open(fixt_preproc.run_fp._json_filt) as jf:
        filt_dict = json.load(jf)
    assert "day2" == filt_dict["bold"]["session"]
    assert "anat" == filt_dict["t1w"]["datatype"]
    assert "epi" == filt_dict["fmap"]["suffix"]


@pytest.mark.preproc
def test_RunFmriprep_write_fmriprep_bind(fixt_setup, fixt_preproc):
    # Test select binding opts
    fp_cmd = fixt_preproc.run_fp._write_fmriprep()
    assert "singularity run" in fp_cmd
    assert "--cleanenv" in fp_cmd
    assert f"--bind {fixt_setup.group_raw}:{fixt_setup.group_raw}" in fp_cmd
    assert f"--bind {fixt_setup.log_dir}:{fixt_setup.log_dir}" in fp_cmd
    assert f"--bind {fixt_setup.work_dir}:{fixt_setup.work_dir}" in fp_cmd
    assert (
        f"--bind {os.environ['SINGULARITYENV_TEMPLATEFLOW_HOME']}:"
        + f"{os.environ['SINGULARITYENV_TEMPLATEFLOW_HOME']}"
        in fp_cmd
    )
    fs_lic_dir = os.path.dirname(os.environ["FS_LICENSE"])
    assert f"--bind {fs_lic_dir}:{fs_lic_dir}" in fp_cmd
    assert f"--bind {fixt_setup.group_raw}:/data" in fp_cmd
    assert (
        f"--bind {fixt_setup.work_dir}/fmriprep/{fixt_setup.sess}:/out"
        in fp_cmd
    )
    assert f"{os.environ['SING_FMRIPREP']} /data /out participant" in fp_cmd


@pytest.mark.preproc
def test_RunFmriprep_write_fmriprep_opts(fixt_setup, fixt_preproc):
    # Test select options
    fp_cmd = fixt_preproc.run_fp._write_fmriprep()
    assert "--ignore fieldmaps" in fp_cmd
    assert f"--participant-label {fixt_setup.subj[4:]}" in fp_cmd
    assert "--skull-strip-template MNI152NLin6Asym" in fp_cmd
    assert "--output-spaces MNI152NLin6Asym:res-2" in fp_cmd
    assert f"--fs-license {os.environ['FS_LICENSE']}" in fp_cmd
    assert "--use-aroma" in fp_cmd
    assert "--fd-spike-threshold 0.5" in fp_cmd
    assert "--skip-bids-validation" in fp_cmd
    assert "--nthreads 10 --omp-nthreads 8" in fp_cmd


@pytest.mark.preproc
def test_RunFmriprep_get_output(fixt_preproc):
    print(fixt_preproc.fp_dict)
    assert ["preproc_bold", "mask_bold"] == list(fixt_preproc.fp_dict.keys())


@pytest.mark.preproc
def test_fsl_preproc():
    pass
