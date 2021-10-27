import sys
import os
import nestedtext as nt
import time
import shutil

app_path = os.getcwd()
app_path= app_path[:-4]
project_root=app_path.replace('\\', '/')+"/" #append this onto global params

if len(sys.argv)==1:
    os.system("start " + project_root) #just start the window
    exit()

if (sys.argv[1]=='to_contrib'): #add as desired
    #this will load all of the files in user to the contrib folder on path. 
    #exclude pycache, contrib, and readme.txt

    contribdir = project_root+"lib/user/contrib/" + os.environ.get('CONTRIBNAME')

    shutil.rmtree(contribdir)
    
    shutil.copytree(project_root+"lib/user", contribdir, ignore=shutil.ignore_patterns('__pycache__', 'README.txt','contrib'))
    exit()


editor = os.environ.get('TEXTEDITOR') #blank for default
    
#from classes import *
from misc import param_smoosh
from getglobals import PARAMSET_GLOBALS
from getnamespace import GLOBAL_NAMESPACE
from jobfxns import *



params_root = project_root + 'etc/Projects/' + sys.argv[2] + '/' + sys.argv[3] + ".nt"

paramset_original = nt.load(params_root)
paramset=paramset_original.copy()
paramset = param_smoosh(paramset,'Job')

#if all you want to do is view/edit params, will now do so
if sys.argv[1]=='params': #add as desired
    if os.name == 'nt': 
        os.system("start " + editor + " " + params_root) #will use the default text editor. 
        exit()

#start paramset further down

#arguments to build in: --param_drop , --print_tree , --novr

#defaults for arguments
print_tree=False
novr=False

if len(sys.argv)>4:

    xtra_args = sys.argv[4:len(sys.argv)]

    if "--params_drop" in xtra_args:
        pos = xtra_args.index("--params_drop")
        paramset = param_smoosh(paramset,xtra_args[pos+1])

    if "--print_tree" in xtra_args:
        print_tree = True
        
    if "--novr" in xtra_args:
        pos = xtra_args.index("--novr")
        novr = int(xtra_args[pos+1])
#initialize the job
deployJob(paramset,sys.argv,paramset_original,print_tree,novr,GLOBAL_NAMESPACE)

