@echo off

set CONTRIBNAME=instinct_afsc_mml_acoustics

cd .. 

git submodule update --remote --merge ./lib/user/contrib/instinct_afsc_mml_acoustics

cd ./bin

call instinct pull_contrib

cd ..

Rscript ./lib/user/Installe.R

pip install -r ./lib/user/requirements.txt

ECHO instinct_afsc_mml_acoustics update complete!
@Pause