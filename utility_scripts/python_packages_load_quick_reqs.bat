@echo off
cd /d %~dp0
cd ..
for %%I in (.) do set CurrDirName=%%~nxI
set VirEnvName=%CurrDirName%_VirEnv
call "./%VirEnvName%/Scripts/activate.bat"
echo Loading all Python packages from quick_reqs.txt into Python virtual environment
set /p dummy=Press enter to continue or close to cancel
echo Loading Python packages
python -m pip install -r quick_reqs.txt
echo Loaded Python packages
set /p dummy=Press enter to close
