#miscellaneous functions
import pandas as pd
import numpy
import nestedtext as nt
from getglobals import PARAMSET_GLOBALS

#some fxns to handle .pipe syntax
def pipetest_pargs_pval_compdef(pipe_args,namespace):

    pipe_args = pipelink_unpack(pipe_args)

    if '.pipe' in pipe_args['pipe']:
        pipeshape = dotpipe_replace(pipe_args['pipe'],namespace)
        #import code
        #code.interact(local=dict(globals(), **locals()))
        #replace key if not yet replaced:
        if not pipeshape[0].__name__ in pipe_args:
            pipe_args[pipeshape[0].__name__]=pipe_args.pop(pipeshape[1].__name__+ '.pipe')
        compdef = namespace[pipeshape[1].__name__].upstreamdef
        
        #retrun first match, by definion there can only be one pipeline per level 
        return pipe_args,pipeshape[0],compdef,pipeshape[1].__name__+ '.pipe'
    #add to this original name so pipeID is useful info again. 
    else:
        return pipe_args,namespace[pipe_args['pipe']],namespace[pipe_args['pipe']].compdef,pipe_args['pipe']

def pipelink_unpack(pipe_args):
    if 'pipe_link' in pipe_args:
            
        link_dict = nt.load(PARAMSET_GLOBALS["project_root"] + "lib/user/pipelines/"+ pipe_args['pipe_link']['name'])
        del pipe_args['pipe_link']

        pipe_args.update(link_dict)
        return pipe_args
    else:
        return pipe_args

def dotpipe_replace(pipe,namespace):

    comp_string = pipe[0:len(pipe)-5]
    comp = namespace[comp_string]

    p_class = comp.pipeshape

    return p_class,comp


#####end this group

####
            
def get_param_names(parameters):
    param_names = list(parameters.keys())
    param_names.remove('methodID')
    param_names.remove('methodvers')

    param_names = ' '.join(sorted(param_names))

    return param_names

def keyassess(key,anydict):
    if anydict != None:
        keytest = key in anydict

        if keytest:
            value = anydict[key]
        else:
            value = None
    else:
        keytest = False
        value = 'base'
        
    return keytest,value

def paramcheck(params):
    slash_in_key=len([pos for pos, char in enumerate(','.join(params.keys())) if char == "-"])
    if slash_in_key != 0:
         raise ValueError("Slashes are a reserved character in parameter keys")

def param_smoosh(params,entry):

    #this function determines if
    #1. a drop should be performed
    #2. whether the drop is multi or single key

    #after each drop, parameters are combined and flattened. Lower level
    #params supercede higher level params of the same key. 
    
    #this function returns a params dict.
    
    #check keys to make sure a dash doesn't exist (not allowed, messes this all up)
    paramcheck(params)

    #check to see if there are slashes in the entry    
    entry_slashes = [pos for pos, char in enumerate(entry) if char == "-"]

    #if no slashes present, just drop the specified level
    if len(entry_slashes)==0:
        params_add = params[entry].copy()
        #fill in params, overwriting same keys with dropped methods
        params.update(params_add)
    else:
        nested_levels= len(entry_slashes)+1 #add 1 to match # of levels, not dashes
        #add length to index string correctly
        entry_slashes = [-1] + entry_slashes +[len(entry)] #make start -1 so will work with both
        #original value and further subsets

        for q in range(nested_levels):
            params_add = params[entry[(entry_slashes[q]+1):entry_slashes[q+1]]].copy()
            params.update(params_add)
    return params
        
        
