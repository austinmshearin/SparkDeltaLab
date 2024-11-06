@echo off
cd /d %~dp0
cd ..
for %%I in (.) do set CurrDirName=%%~nxI
set VirEnvName=%CurrDirName%_VirEnv
call "./%VirEnvName%/Scripts/activate.bat"
echo Python package will be installed locally as editable install according to setup.py
set /p dummy=Press enter to set up editable install of Python package or close to cancel
echo Installing local editable Python package
python -m pip install -e .
echo Python package installed
set /p dummy=Press enter to close
