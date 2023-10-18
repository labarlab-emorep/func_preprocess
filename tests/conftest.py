import pytest

# from func_preprocess import preprocess


@pytest.fixture(scope="session")
def fixt_setup():
    # Hardcode variables for specific testing
    subid = "ER0009"
    sess = "ses-day2"
    fs_path = (
        "/mnt/keoki/experiments2/EmoRep/Exp2_Compute_Emotion/"
        + "data_scanner_BIDS/derivatives/pre_processing/freesurfer"
    )
    proj_raw = (
        "/mnt/keoki/experiments2/EmoRep/Exp2_Compute_Emotion/"
        + "data_scanner_BIDS/rawdata"
    )
    yield {
        "subid": subid,
        "sess": sess,
        "fs_path": fs_path,
        "proj_raw": proj_raw,
    }
