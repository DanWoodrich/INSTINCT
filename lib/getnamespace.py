#I might want to modify this with a environmental variable that will indicate
#if you want to use common or user namespace, and change the import style
#depending

from os import environ
from pipe_shapes import *

from user.processes import * 
from user.pipe_shapes import *

#if environ.get('USECONTRIB')=='True':
#    from contrib.processes import *
#    from contrib.pipe_shapes import * 

GLOBAL_NAMESPACE = globals() #this allows for exporting the complete namespace
