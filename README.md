# func_preprocessing
This in-house python package is written for the EmoRep project, intended to be executed on the Duke Compute Cluster (DCC).

## Functionality
- Parallelizes jobs for batches of subjects
- Run FreeSurfer and fMRIPrep preprocessing
- Conduct FSL and AFNI extra preprocessing steps

## Usage
- Install into the project conda environment on the DCC `$ labar_env emorep && python setup.py install --record record.txt`
    - See [here](https://github.com/labarlab/conda_dcc) for labarlab conda details
    - (should already be done)
- Trigger package help and usage via entrypoint `$ func_preprocessing`

## Notes
- Data is processed in `/work/<user>/EmoRep/derivatives` and final files are copied to `/hpc/group/labarlab/EmoRep/derivatives`, then housekeeping is conducted in `/work`
- Run scripts (run_fmirprep_sub-ER0009.py), parent stdout/err (par009.txt), and subprocess stdout/err (out/err_009_<job_name>.log) for each subject are captured in `/work/<user>/EmoRep/derivatives/logs/func_pp_<timestamp>`

## Documentation
_TODO: take readthedocs live_
