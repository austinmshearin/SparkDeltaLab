@echo off
cd /d %~dp0
cd ..
for %%I in (.) do set CurrDirName=%%~nxI
set VirEnvName=%CurrDirName%_VirEnv
call "./%VirEnvName%/Scripts/activate.bat"
echo Python package will be built according to setup.py
set /p dummy=Press enter to build Python package or close to cancel
echo Building Python package
python setup.py bdist_wheel
echo Python package built
set /p dummy=Press enter to close
