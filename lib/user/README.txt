The user directory is where you develop code for use in INSTINCT. 

INSTINCT is a command line application, not a standalone python package. INSTINCT explicity refers to this location and 
files of predetermined name. Additionally, INSTINCT relies on some standard object definitions and imports within this 
directory. 

./Contrib shows examples of projects from other labs. They can be imported into your script with import .contrib.[] 

Keep in mind that you can control namespace of contrib processes using your style of import. Methods and pipelines,
not being python files, must share a common namespace with your user directory if you plan on encorporating them. 

./Contrib/template helps to start a new project. Copy in the files/directories from there into this dir and work off of 
the existing named files, imports, and objects. Please use the training materials and other examples in contrib to help 
define your needed objects. 

