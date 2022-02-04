@echo off

cd ..\etc

copy /-y INSTINCT_ex.cfg INSTINCT.cfg 

pip install -r requirements.txt

ECHO INSTINCT config complete!
@Pause