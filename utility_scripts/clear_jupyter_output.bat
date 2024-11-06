@echo off
cd /d %~dp0
cd ..
for %%I in (.) do set CurrDirName=%%~nxI
set VirEnvName=%CurrDirName%_VirEnv
call "./%VirEnvName%/Scripts/activate.bat"
echo Clearing all Jupyter notebook output
set /p dummy=Press enter to clear all Jupyter notebook output from the workspace or close to cancel
python ./utility_scripts/clear_jupyter_output.py
echo All Jupyter notebook output cleared
set /p dummy=Press enter to close
