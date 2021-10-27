The user directory is where you develop code for use in INSTINCT. A functional user project would contain ./processes.py, 
./pipe_shapes.py, ./definitions.py, ./art.py, and __init__.py . And also, the directories methods and pipelines (technically optional)

INSTINCT is a command line application, not a standalone python package. INSTINCT explicity refers to this location and 
files of predetermined name. Additionally, INSTINCT relies on some standard object definitions and imports within this 
directory. 

./contrib shows examples of projects from other labs. They can be imported into your script with import .contrib.[] 

Keep in mind that you can control namespace of contrib processes using your style of import. Methods and pipelines,
not being python files, must share a common namespace with your user directory if you plan on encorporating them. 

./contrib/template helps to start a new project. Copy in the files/directories from there into this dir and work off of 
the existing named files, imports, and objects. Please use the training materials (coming soon) and other examples in contrib to help 
define your needed objects. 

Recommended pattern is to define a submodule, update it with instinct push_user, and pull it using  git submodule update --remote --merge and
instinct pull_contrib. This will keep your own version control and allow for others to use your contrib. 
