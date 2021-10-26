This is where (in your local copy) you can store project specific params. Create a new directory corresponding to 
your project name, and place project job params in this directory. 
Project params are ignored from git for safer version control and individual use.

Access params in this folder using additional command line arguments:

instinct command arg1 arg2

arg1 indicates the parameter project folder. (ie, 'demo')

arg2 indicates the param file name within the project folder with no extension (ie, 'testproj')