@echo off

set CONTRIBNAME=instinct_afsc_mml_acoustics

cd .. 

git submodule update --remote --merge ./lib/user/contrib/instinct_afsc_mml_acoustics

call instinct pull_contrib

Rscript ./lib/user/Installe.R

ECHO instinct_afsc_mml_acoustics update complete!
@Pause