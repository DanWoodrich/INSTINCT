@echo off

cd ..

git pull

cd .\etc

copy /-y INSTINCT_ex.cfg INSTINCT.cfg 

pip install -r requirements.txt

ECHO INSTINCT update complete!
@Pause