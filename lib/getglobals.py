import os
import sys
from pathlib import Path
import nestedtext as nt

project_root= (Path(__file__).resolve().parents[1].as_posix() +"/")

params_root = project_root + 'lib/user/Projects/' + sys.argv[2] + '/' + sys.argv[3] + ".nt"

paramset_original = nt.load(params_root)
paramset_globals = paramset_original['Global']

PARAMSET_GLOBALS = {**paramset_globals,**{'project_root' :project_root}} #add to global dict

PROJECT = sys.argv[2]

