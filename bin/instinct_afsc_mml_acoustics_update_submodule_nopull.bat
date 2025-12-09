@echo off

set CONTRIBNAME=instinct_afsc_mml_acoustics

cd .. 

git submodule update --remote --merge ./lib/user/contrib/instinct_afsc_mml_acoustics

ECHO instinct_afsc_mml_acoustics ref update complete!
@Pause