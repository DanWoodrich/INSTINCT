import os
import sys
import nestedtext as nt

app_path = os.getcwd()
app_path= app_path[:-4]
project_root=app_path.replace('\\', '/')+"/" #append this onto global params

params_root = project_root + 'etc/Projects/' + sys.argv[2] + '/' + sys.argv[3] + ".nt"

paramset_original = nt.load(params_root)
paramset_globals = paramset_original['Global']

PARAMSET_GLOBALS = {**paramset_globals,**{'project_root' :project_root}} #add to global dict

PROJECT = sys.argv[2]

