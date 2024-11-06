@echo off
cd /d %~dp0
cd ..
for %%I in (.) do set CurrDirName=%%~nxI
set VirEnvName=%CurrDirName%_VirEnv
call "./%VirEnvName%/Scripts/activate.bat"
echo Adding Python virtual environment to Jupyter notebooks: %VirEnvName%
set /p dummy=Press enter to add Python virtual environment or close to cancel
echo Adding Jupyter to Python virtual environment
python -m ipykernel install --user --name="%VirEnvName%"
echo Python virtual environment added
set /p dummy=Press enter to close
