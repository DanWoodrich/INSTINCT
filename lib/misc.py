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
        #import code
        #code.interact(local=dict(globals(), **locals()))
        if hasattr(namespace[pipe_args['pipe']],"compdef"):
            return pipe_args,namespace[pipe_args['pipe']],namespace[pipe_args['pipe']].compdef,pipe_args['pipe']
        else:
            return pipe_args,namespace[pipe_args['pipe']],None,pipe_args['pipe']
            #import code
            #code.interact(local=dict(globals(), **locals()))

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

        #if entry =="Job-TrainModel_paramset-seperate_ds_train":
            #import code
            #code.interact(local=dict(globals(), **locals()))
    return params
        
#these two are part of the AFSC submodule, but they are used by the combine
#loop class which relies on AFSC intermediate datatypes. Can be ignored, and
#should be fixed eventually. 

def get_difftime(data,cap_consectutive=None):

    

    data['TrueStart'] = data['StartTime']+pd.to_timedelta(data['SegStart'], unit='s')
    data['TrueEnd'] = data['TrueStart']+pd.to_timedelta(data['SegDur'], unit='s')
    data['DiffTime']=pd.to_timedelta(0)
    data['DiffTime'][0:(len(data)-1)] = pd.to_timedelta(abs(data['TrueEnd'][0:(len(data['TrueEnd'])-1)] - data['TrueStart'][1:len(data['TrueStart'])].values)) #changes 7/12/21, fix bug where difftime was assigned improperly
    data['DiffTime'] = (data['DiffTime']>pd.to_timedelta(2,unit='s'))==False #makes the assumption that if sound files are 1 second apart they are actually consecutive (deals with rounding differences)

    consecutive = numpy.empty(len(data['DiffTime']), dtype=int)
    consecutive[0] = 1
    iterator = 1

    if cap_consectutive != None:
        #loop through each difftime, and if
        for n in range(0,(len(data['DiffTime'])-1)):
            if data['DiffTime'].values[n] != True:
                iterator = iterator+1
                consecutive[n+1] = iterator
            else:
                consecutive[n+1] = iterator

        #data['DiffTime'] = consecutive

        #temporary to use for loop
        data['DiffTime_con'] = consecutive

        dpatch_list = [None] * len(data['DiffTime_con'].unique().tolist())

        for x in range(len(data["DiffTime_con"].unique().tolist())):

            xIn = [data['DiffTime_con'].unique().tolist()[x]]

            #if x ==38:
            #    import code
            #    code.interact(local=dict(globals(), **locals()))

            dataIn = data.loc[data['DiffTime_con'].isin(xIn),]

            dataIn = dataIn.reset_index()

            #hardcoded as 40 minutes
            _cumsum = dataIn["SegDur"].cumsum()//(cap_consectutive) #hardcode this to be 40 minutes- reason is for backwards compatible.

            #import code
            #code.interact(local=dict(globals(), **locals()))
            
            _cumsum = pd.Series.tolist(_cumsum)
            indexes = [_cumsum.index(x) for x in set(_cumsum)]
            indexes2 = list(numpy.where(dataIn["SegDur"]>=3600))[0].tolist()
            #print(x)

            if len(dataIn) != 1 and indexes!=[0]:

                #Not sure if below was ever right for any case- I should test it further.
                #if x !=2:
                #import code
                #code.interact(local=dict(globals(), **locals()))
                
                dataIn.loc[indexes,"DiffTime"] = False #set first value to false. 
                dataIn.loc[0,"DiffTime"] = True #except for the first one

                #final change- for each index which is equal to or greater than cap_consectutive, change to false
                dataIn.loc[indexes2,"DiffTime"] = False

            dataIn = dataIn.drop('index', axis=1)

            dpatch_list[x]=dataIn


        
        data = pd.concat(dpatch_list,ignore_index=True)

        data = data.drop('DiffTime_con', axis=1)
        
    #do again with cap consecutive breaks added

    #import code
    #code.interact(local=dict(globals(), **locals()))

    consecutive = numpy.empty(len(data['DiffTime']), dtype=int)
    consecutive[0] = 1
    iterator = 1
    
    for n in range(0,(len(data['DiffTime'])-1)):
        if data['DiffTime'].values[n] != True:
            iterator = iterator+1
            consecutive[n+1] = iterator
        else:
            consecutive[n+1] = iterator

    data['DiffTime'] = consecutive
    data = data.drop(columns='TrueStart')
    data = data.drop(columns='TrueEnd')

    #import code
    #code.interact(local=dict(globals(), **locals()))
    
    return(data)

def file_peek(file,fn_type,fp_type,st_type,dur_type,comp_type=0):#this is out of date- don't think I need to have fxn variables for how I load in the standard metadata.
        if comp_type != 0:
            heads = pd.read_csv(file, nrows=1,compression=comp_type)
        else:
            heads = pd.read_csv(file, nrows=1)
        heads = heads.columns.tolist()
        heads.remove('FileName')
        heads.remove('StartTime')
        heads.remove('Duration')
        heads.remove('SegStart')
        heads.remove('SegDur')
        hdict = {'FileName': fn_type, 'FullPath': fp_type, 'StartTime': st_type, 'Duration': dur_type, 'SegStart': 'float64', 'SegDur': 'float64'}
        if len(heads) != 0:
            metadict = dict.fromkeys(heads , 'category')
            hdict.update(metadict)
        return hdict

def pipeline_n_extract(n,n_ref,component_name):

    #print(n)
    #print(n_ref)
    #print(component_name)

    if n_ref == None or n_ref ==[None]:
        return 'default'
        
    if type(n_ref)==list:
        if component_name in n_ref:
            if not isinstance(n, int):
                ind =n_ref.index(component_name)
                #print(n[ind])
                #print(n_ref[ind])
                return n[ind]
            else:
                return n
        else:
            return 'default'
        
    else:
        if component_name in n_ref:
            return n
        else:
            return 'default'

    

