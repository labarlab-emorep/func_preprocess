import pytest
from func_preprocess import preprocess


@pytest.fixture(scope="session")
def fixt_setup():
    # Hardcode variables for specific testing
    subid = "sub-ER0009"
    sess = "ses-day2"
    derivs_path = (
        "/mnt/keoki/experiments2/EmoRep/Exp2_Compute_Emotion/"
        + "data_scanner_BIDS/derivatives/pre_processing"
    )
    proj_raw = (
        "/mnt/keoki/experiments2/EmoRep/Exp2_Compute_Emotion/"
        + "data_scanner_BIDS/rawdata"
    )
    fs_object = preprocess.RunFreeSurfer(
        subid, proj_raw, derivs_path, str, True
    )
    yield {
        "subid": subid,
        "sess": sess,
        "derivs_path": derivs_path,
        "proj_raw": proj_raw,
        "fs_object": fs_object,
    }
