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

elif (sys.argv[1]=='push_user'): 
    #this will push your user changes to a named contrib path (not within this repo)
    #exclude pycache, contrib, and readme.txt

    userdir = project_root+"lib/user"

    if len(sys.argv)==3:
        contribdir =sys.argv[2] #can manually specify, otherwise defaults to named path in INSTINCT.cfg
    else:
        contribdir = os.environ.get('CONTRIBPATH')
   

    #iteratively delete files and folders from contrib
    files_to_ignore_contrib = ['__pycache__', 'README.md','README.txt','LICENSE','.git','.gitignore']
    
    for item in os.listdir(contribdir):
        if item not in files_to_ignore_contrib:  # If it isn't in the list for retaining
            path_item = contribdir + '/' + item
            if os.path.isfile(path_item):
                os.remove(path_item) #Remove the item 
            else:
                shutil.rmtree(path_item)  # Remove the dir

    #iteratively copy files and folders from user
    files_to_ignore_user = ['__pycache__', 'README.md','README.txt','LICENSE','contrib']
    
    for item in os.listdir(userdir):
        if item not in files_to_ignore_user:
            path_item = userdir + '/' + item
            if os.path.isfile(path_item):
                shutil.copyfile(path_item, contribdir + "/" + item) #copy item
            else:
                shutil.copytree(path_item,contribdir + "/" + item) #copy tree
    #exit()

elif (sys.argv[1]=='pull_contrib'):
    #this will pull from your contib path, but as a submodule
    #this pattern encourages keeping your changes updated in a submodule repo so that others can access an up to date copy.

    ans = input("Warning: this will replace all contents of your /user directory with matching name to contrib. If not saved externally to this directory, recovery" +
                " will by impossible. Proceed? (y/n)")

    if ans == 'n':
        exit()

    userdir = project_root+"lib/user"

    if len(sys.argv)==3:
        contribdir =project_root+"lib/user/contrib/" + sys.argv[2]
    else:
        contribdir = project_root+"lib/user/contrib/" + os.environ.get('CONTRIBNAME')

    files_to_ignore_contrib = ['__pycache__', 'README.txt','README.md','LICENSE','.git']

    for root, dirs, files in os.walk(contribdir, topdown=False):

        dst_dir = root.replace(contribdir, userdir, 1)
        
        root = root.replace("\\","/")
        #print(root)
        root_short = root[(len(contribdir)+1):]
        #print(root_short)
        if len(root_short)==0:
            #import code
            #code.interact(local=dict(globals(), **locals()))
            filescomp = files
        else:
            filescomp = [root_short +'/'+ x for x in files]

        indeces = [i for i,val in enumerate(filescomp) if val not in files_to_ignore_contrib]

        files = [files[i] for i in indeces]

        if not os.path.exists(dst_dir):
            os.makedirs(dst_dir)
        for file_ in files:
            
            src_file = os.path.join(root, file_)
            dst_file = os.path.join(dst_dir, file_)
            if os.path.exists(dst_file):
                os.remove(dst_file)
            shutil.copy(src_file, dst_dir)

    #exit()

else: 
    editor = os.environ.get('TEXTEDITOR') #blank for default

    #from classes import *
    from misc import param_smoosh
    from getglobals import PARAMSET_GLOBALS
    from getnamespace import GLOBAL_NAMESPACE
    from jobfxns import *



    params_root = project_root + 'lib/user/Projects/' + sys.argv[2] + '/' + sys.argv[3] + ".nt"

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
    novr='default'

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

