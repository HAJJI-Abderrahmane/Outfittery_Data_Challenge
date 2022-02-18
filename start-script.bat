@ECHO OFF
set url="https://archive.org/download/stackexchange/beer.stackexchange.com.7z"
python Data_Staging\running_staging.py %url%
python Data_Processing\running_processing.py %url%

PAUSE