This is where (in your local copy) you can store project specific params. Create a new directory corresponding to 
your project name, and place project job params in this directory. 
Project params are ignored from git for safer version control and individual use.

Access params in this folder using additional command line arguments:

command arg1 arg2

arg1 indicates the parameter source folder.  "." = defaults , "x" = Projects/x (used for project specific params)

arg2 indicates the Job name to pull parameters  "." = defaults , "y" = Job Name "y" (useful if experimenting viewing intermediate outputs) 