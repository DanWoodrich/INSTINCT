@echo off
Set PREVDIR=%CD%

cd %~dp0

for /f "delims=" %%x in (../etc/INSTINCT.cfg) do (set "%%x")

cd ../

cd ./lib

python jobexec.py %*

cd %PREVDIR%