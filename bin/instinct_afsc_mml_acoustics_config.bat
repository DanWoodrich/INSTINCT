@echo off

set CONTRIBNAME=instinct_afsc_mml_acoustics

cd .. 

git submodule update --init --recursive ./lib/user/contrib/instinct_afsc_mml_acoustics

cd ./bin

call instinct pull_contrib

cd ..

Rscript ./lib/user/Installe.R

ECHO instinct_afsc_mml_acoustics config complete!
@Pause