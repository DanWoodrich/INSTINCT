import luigi
import os
import nestedtext as nt
import shutil
import pandas as pd
import hashlib
import subprocess
import gzip
import tarfile
from misc import * 
from getglobals import PARAMSET_GLOBALS

class INSTINCT_pipeline:

    #run() defined in specific pipeline

    def __init__(self,params,n,pipe_args,upstream,namespace,compdef,dag):

        self.params=params
        self.n=n
        self.namespace = namespace

        if pipe_args!={}:

            pipe_args = pipelink_unpack(pipe_args) #contains a hidden 'if' to check for pipe_link

            if 'params_drop' in pipe_args:
                #print("******did params drop!*******")
                #import code
                #code.interact(local=dict(globals(), **locals()))
                #print(self.pipeID)
                #print(pipe_args['params_drop'])
                #print("******did params drop!*******")
                #print(pipe_args['params_drop'])
                self.params = param_smoosh(self.params,pipe_args['params_drop'])
                #print(self.params)

            #test if .pipe is referenced in pipeline#
            pipe_args,pipe_val,compdef,pipeID =pipetest_pargs_pval_compdef(pipe_args,namespace)
            
            #print(pipe_args)
            self.pipe_args=pipe_args[self.__class__.__name__] #drop to name of pipeline in pipe_args

        #print(self.pipe_args)
        self.upstream = upstream
            
        self.pipeID = pipeID

        #pass copies, so the originals aren't modified up the pipe
        self.params_copy = self.params.copy()
        self.pipe_args_copy = self.pipe_args.copy()

        #compdef will be specified as an attribute for defined pipelines (the not .pipe syntax version)

        self.compdef=compdef
        self.dag = dag

        #import code
        #code.interact(local=dict(globals(), **locals()))

    def run_component(self,component=None,final_process=False,upstream=[None]):

        if final_process==True:
            #print(self.namespace[self.pipe_args['process']])
                    #if cls.__name__ == "DLmodel_Test":
                        #import code
                        #code.interact(local=dict(globals(), **locals()))
            #

            #I changed the params passing from self.params_copy to self.params... keep an eye on other ramifications of this change.
            process = self.namespace[self.pipe_args['process']].invoke(self.params,n=self.n,pipe_args=self.pipe_args_copy,compdef=None,\
                                                                            upstream=upstream,pipeID=self.pipeID,namespace = self.namespace,dag=self.dag)
            #print(self.pipe_args_copy)
            #print(self.params_copy["Job"]["DLmodel*"])
            #if self.pipe_args_copy['process'] == "DLmodel_Test":
            #    import code
            #    code.interact(local=dict(globals(), **locals()))
            return process
        elif component in self.pipe_args:

            #here, test if pipe or process
            #if component =="GetDETx":
            

            #assign temporary pipe args so any modifications will not be retained
            pipe_args = self.pipe_args_copy.copy()

            #print(pipe_args[component])
            
            
            #component will never have both pipe and process in the same level. (?)
            if 'process' in pipe_args[component]:

                return self.namespace[pipe_args[component]['process']].invoke(self.params_copy,n=self.n,pipe_args=pipe_args[component],upstream=upstream,\
                                                                       pipeID=self.pipeID,namespace = self.namespace,dag=self.dag,compdef=None)
            elif 'pipe' in pipe_args[component]:
                
                pipe_args,pipe_val,compdef,pipeID =pipetest_pargs_pval_compdef(self.pipe_args[component],self.namespace)
                
                    #here  is where I'd test for the loop conditional
                 
                if 'loop_on' in pipe_args: #and self.n==False: #not sure if this does anything..?
                    
                    #check to see if there is an novr

                    #print(component)

                    #if component== "GetPE1_S1":
                    #    import code
                    #    code.interact(local=dict(globals(), **locals()))
                    
                    return CombineExtLoop.invoke(params=self.params_copy,component=pipe_val,pipe_args=pipe_args,pipeID=pipeID,\
                                                 namespace = self.namespace,compdef=compdef,process_peek = pipe_args['loop_on'],dag=self.dag)
                else:
                    return pipe_val.invoke(self.params_copy,n=self.n,pipe_args=pipe_args,upstream=upstream,\
                                           pipeID=pipeID,compdef=compdef,namespace = self.namespace,dag=self.dag)
        else:

            #import code
            #code.interact(local=locals())
            if component in self.namespace:
                #this means that there is a hardcoded process or pipeline as the component. 
                #When hardcoding compdef, use a string to identify process or pipeline in namespace.
                #once you hardcode a component, you cannot go back to pipeargs (everything upstream also must be hardcoded
                self.namespace[component].invoke(self.params_copy,n=self.n,pipe_args={},upstream=upstream,\
                                               pipeID=pipeID,compdef=None,namespace = self.namespace,dag=self.dag)
            else:
                #this means that the component is not assigned. So, we return None. This can be done intentionally or unintentionally, be careful
                return None

            #what this also means, is that components cannot share namespace with pipelines/processes!
            
    def loop_component(self,component=None,final_process=False,upstream=[None],process_peek=None):

        return CombineExtLoop.invoke(params=self.params_copy,component=component,pipe_args={},pipeID=self.pipeID,\
                                             namespace = self.namespace,compdef=None,process_peek = process_peek,dag=self.dag)
        
    @classmethod
    def invoke(cls,params,pipe_args={},upstream=[None],n='default',pipeID=None,namespace = None,compdef = None,dag=[]):

        #drop to pipeline name param level if present in keys.
        if cls.__name__ in params:
            params = param_smoosh(params,cls.__name__)

        #print(params["DLmodel*"])
            
        return cls(params,n,pipe_args,upstream,namespace,compdef,dag=dag).run()

class INSTINCT_task(luigi.Task):

    ports = luigi.Parameter(significant=False) #these are the upstream ports , in order.
    ports_hash = luigi.ListParameter() #these are included to be hashed.
    
    #internal variable definitions
    hash_length = int(os.environ.get('HASHLEN'))

    hash_file = False #for processes which need to do this, specify this as true

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._Task__hash = int(str(self._Task__hash)[1:(1+self.hash_length)])

    def requires(self):
        if self.ports[0]!=None:
            for L in range(len(self.ports)):
                yield self.ports[L]

    def outfilegen(self):

        return self.outpath() + "/" + self.outfile

    def outpath(self):
        
        if self.ports[0]==None:
            #default to CacheRoot
            path = PARAMSET_GLOBALS['cache_root'] +'/'+ str(self._Task__hash)
             
        else:
            path = self.ports[0].outpath() +'/'+ str(self._Task__hash)

        if not os.path.exists(path): #might be redundant, make sure not slowing anything down
            os.makedirs(path)

        return path

    #convenience
    def getports(upstream):
        #print(upstream)
        #print(len(upstream))
        if upstream[0]!=None:
            ports = [upstream[L] for L in range(len(upstream))]
            ports_hash = [upstream[L]._Task__hash for L in range(len(upstream))]
        else:
            ports = [None]
            ports_hash = [None]
        return [ports,ports_hash]

class INSTINCT_process(INSTINCT_task):
    
    parameters = luigi.DictParameter(significant=False) #this is referenced within task
    param_string = luigi.Parameter() #this is only hashed

    param_string2 = luigi.Parameter(significant=False) #this is passed to run_cmd 

    rerun_key = luigi.IntParameter() #all this does is change the hash to a unique value
    
    arguments = luigi.DictParameter(significant=False)
    descriptors = luigi.DictParameter(significant=False)

    processID = luigi.Parameter(significant=False)

    pipeID= luigi.Parameter(significant=False) #reference the pipeline that called the process

    def output(self):
        return luigi.LocalTarget(self.outfilegen())
    def run_cmd(self):
        #1st iteration of this, might modify

        methodjoin = '-'

        if self.descriptors['language']=='R':
            executable1 = 'Rscript '
            executable2 = '.R'
        elif self.descriptors['language']=='Python':
            executable1 = 'python '
            executable2 = '.py'
        elif self.descriptors['language']=='MATLAB':
            executable1 = ''
            executable2 = '.mat'
        elif self.descriptors['language']=='batch':
            executable1 = ''
            executable2 = '.bat'
            
        if self.descriptors['runtype']=='bin':
            executable2 = '.exe'
            methodjoin = '' #matlab does not allow for '-' in name

            
        
        wrapper,wrapper_value = keyassess('wrapper',self.descriptors)
        if wrapper_value!='True':
            #import code
            #code.interact(local=locals())
            command1 = executable1 + PARAMSET_GLOBALS['project_root'] + 'lib/user/methods/' + self.processID +\
                       '/' + self.parameters['methodID'] + '/' + self.parameters['methodID'] + methodjoin + self.parameters['methodvers'] + executable2

            command2 = os.environ["INS_ARG_SEP"].join(self.cmd_args)
            
        else:
            command1 = executable1 + PARAMSET_GLOBALS['project_root'] + 'lib/user/methods/' + self.processID + '/'+\
            self.processID  +"Wrapper" + executable2

            command2 = PARAMSET_GLOBALS['project_root'] + os.environ["INS_ARG_SEP"]+ os.environ["INS_ARG_SEP"].join(self.cmd_args)
                
        


        command = command1 + ' '+ command2

        #import code
        #code.interact(local=dict(globals(), **locals()))

        if 'venv' in self.descriptors:
            print("******************************\nActivating virtual environment " + self.descriptors['venv_name'] + " with " + self.descriptors['venv'] + "\n******************************")
            if(self.descriptors['venv']=='Conda'):
                command_venv = self.descriptors['venv'] + ' activate ' + self.descriptors['venv_name'] + ' & '
                command = command_venv + command #append venv call to start of command. This assumes conda venv is set up to work on command line.
            else:
                print("VENV NOT YET CONFIGURED TO WORK ON BASE VENV")
        
        print("******************************\nRunning " + self.descriptors['language'] + " method " + self.parameters['methodID'] + methodjoin + self.parameters['methodvers'] +\
              " for process " + self.processID + "\n******************************")

        #temp:
        #print(command)

        print("******************************\nCommand params (can copy and paste): " + command2 +"\n******************************")

        #import code
        #code.interact(local=locals())
        subprocess.run(command,shell=True)

        
    #pipe_args is not refered to here since it is a placeholder to allow for dynamic calling between pipelines/processes
    @classmethod 
    def invoke(cls,params,upstream=[None],n='default',pipe_args={},pipeID=None,namespace=None,dag=None,compdef=None):
        
        keytest,value = keyassess('drop',pipe_args)

        if keytest and value!=False:

            params = param_smoosh(params,value)

        paramssave = params

        processID = cls.__name__ #overwrite this if have to drop further to find params
        #print(cls.__name__)
        
        #if process name present, drop to process
        if cls.__name__ in params:
            params = params[cls.__name__] #always drop to processes available in params: control overrides in param file
        #if not present, try to use character matching.
        else:
            for f in range(len(list(params.keys()))):
                string_candidate = list(params.keys())[f]
                if '*' in string_candidate:

                    string_candidate_test = string_candidate[0:string_candidate.find('*')]

                    if string_candidate_test in cls.__name__:
                        params = params[string_candidate] #if candidate contains string and wildcard
                        processID = string_candidate_test
                        break
            
        if 'parameters' in params:

            #if cls.__name__!="FormatGT":

            parameters = params['parameters'].copy()

            if len(parameters)>0:

                #if n==1:
                #import code
                #code.interact(local=locals())

                #extract methodID and method vers
                if params['descriptors']['runtype']!= "no_method":
                    methodparams = { k:v for k,v in parameters.items() if 'methodID' in k}
                    methodparams.update({ k:v for k,v in parameters.items() if 'methodvers' in k})
                    
                    del parameters['methodID']
                    del parameters['methodvers']

                prm_tlist= sorted(parameters.items())
                
                prm_vals = [prm_tlist[x][1] for x in range(len(prm_tlist))]
                if n != 'default':
                    prm_vals_sort = [sorted(prm_vals[x])[n] if isinstance(prm_vals[x],list) else prm_vals[x] for x in range(len(prm_vals))]

                else:
                    #or resort the list so that it hashes in a standard way. 
                    prm_vals_sort = [','.join(sorted(prm_vals[x])) if isinstance(prm_vals[x],list) else prm_vals[x] for x in range(len(prm_tlist))]

                #here, check for global parameters and inject in case of match
                for i in range(len(prm_vals_sort)):
                    if prm_vals_sort[i].startswith("[") and prm_vals_sort[i].endswith("]"):
                        prm_vals_sort[i] = PARAMSET_GLOBALS['parameters'][prm_vals_sort[i][1:len(prm_vals_sort[i])-1]]
                    #also, check for leading and trailing whitespace
                    #prm_vals_sort[i] = prm_vals_sort[i].strip() 
                    

                #import code
                #code.interact(local=dict(globals(), **locals()))

                param_string = ' '.join(prm_vals_sort)
                param_string2 = os.environ["INS_ARG_SEP"].join(prm_vals_sort)


                #if cls.__name__=="EventDetector":
                

                #append on the values for methodID and method to param string
                if params['descriptors']['runtype']!= "no_method":
                #squish together methodID and methodvers to be more consistent with existing R methods etc. 
                    param_string = param_string + ' '+ methodparams['methodID'] + "-" + methodparams['methodvers']
                    param_string2 = param_string2 + os.environ["INS_ARG_SEP"]+ methodparams['methodID'] + "-" + methodparams['methodvers']

                    parameters.update(methodparams)
                
            else:
                param_string = ''
                param_string2 = ''

            

            #change the n value within parameters
            if n!= 'default':
                #import code
                #code.interact(local=locals())
                keys = list(parameters.keys())
                any_list = False
                for i in range(len(keys)):
                    if type(parameters[keys[i]])==list:
                        any_list=True
                        parameters_n = parameters.copy()
                        parameters_n[keys[i]]=sorted(parameters[keys[i]])[n]
                if any_list==False:
                    parameters_n = parameters
            else:
                parameters_n = parameters

        else:
            parameters_n = None
            param_string =""
            param_string2 =""

        if 'arguments' in params:
            arguments = params['arguments'].copy()
            if 'splits' not in arguments:
                arguments.update({'splits':'1'})#   
            
            #test for presence of global parameters and populate. 
            arg_vlist= list(arguments.values())
            arg_klist= list(arguments.keys())
            for i in range(len(arg_vlist)):
                val = arg_vlist[i]
                if val.startswith("[") and val.endswith("]"):
                    #import code
                    #code.interact(local=dict(globals(), **locals()))
                    arguments[arg_klist[i]] = PARAMSET_GLOBALS['parameters'][val[1:len(val)-1]]
        else:
            arguments = {'splits':'1'}
        if 'descriptors' in params:
            descriptors = params['descriptors'].copy()
        else:
            descriptors = None
        #get outpath and hash from ports

        #print(param_string)

        portsout = cls.getports(upstream)

        #if has an argument to rerun process, test for completion, and delete output if complete.
        #this is useful for tasks where the state of the precursor is intentionally not address (a db for example)
        #and use cases arise where remaining consistent is more important than using the most up to date info

        rerun_key = 0
        if 'rerun_key' in arguments:
            rerun_key = int(arguments['rerun_key'])

        process = cls(parameters=parameters_n,param_string=param_string,param_string2=param_string2,arguments=arguments,descriptors=descriptors,\
                   ports=portsout[0],ports_hash=portsout[1],processID=processID,pipeID=pipeID,rerun_key=rerun_key)

        #downstream will be set to empty to when just populating hashes for combextloop. 
        if 'hard_rerun' in arguments:
            if arguments['hard_rerun']=='y' and dag!=None:
                #delete all of the downstream outputs 
                dag._clean(process,'downstream')

        if 'trim_tree' in arguments:
            if arguments['trim_tree']=='y' and dag!=None:
                #delete all of the downstream outputs 
                dag._clean(process,'upstream')

        return(process)

class INSTINCT_userprocess(INSTINCT_process):

    #instinct userprocess has the same run, but calls a specific instructions
    def get_modules_Art(self):
        
        from getglobals import PROJECT
        from art import getArt
        import random
        
        art = getArt(PROJECT,True,random.randint(1,10))

        return art

    def input_w_art(self,message):

        mlen = len(message)
        spaceslen = 40
        messagebuffer = " "* (spaceslen-round(mlen/2))
        
        out = input("\n " + r"""                  '.'.,'; '. .'.,',; ,' :','..'.,', '. ; ,'""" + "\n" +\
                    messagebuffer + message + "\n" + self.get_modules_Art() + "\n(y/n):")
        return out

    def get_userfile(self,only_file=False):

        file = self.outfile
        outf1 = file[0:file.find(".")]
        outf2 = file[self.outfile.find("."):]
        outmod = outf1 + self.userfile_append + outf2

        if only_file ==False:
            out =self.outpath() + "/" +outmod 
        elif only_file==True:
            out = outmod
        return out

    def user_cancelation(self):
        os.environ["USERREVIEW"] = "False"
        os.environ["CANCELREVIEW"] = "True"
    def print_manifest(self):

        os.system("start " + self.outpath().replace('/','\\')) #windows friendly for copy paste (matters on NAS drive) 

        text = 'INPUT: file named: "' + self.outfile + '" in opened directory: "' + self.outpath().replace('/','\\') + '"\n\n' +\
               'INFO:' + self.get_info()+ '"\n\n' +\
                "INSTRUCTIONS: " + self.parameters['instructions'] + '\n\nOUTPUT: save a new copy of the file in the opened directory as: "' + self.get_userfile(True) + '" \n'+\
                "When task is completed, enter y to Proceed, or n to abort." + "\n\n" +\
                "Proceed?"

        return text

    def file_modify(self,file): #if you want to modify the file prior to user working on it, override this in specific implementation 
        print("")
        #for example, this could take a RAVENx file and add a column for analyst (based on user ID) 
    def run(self):


        if os.environ.get('USERREVIEW')!="False":
            #this is the default value
            
            userreview = self.input_w_art("A userprocess task awaits! Perfom now?") #A userprocess task awaits! Perfom now?

            if userreview == 'y':
                os.environ["USERREVIEW"] = "True"

                #make a change: before calling this, move files into this task output folder- will be much more clear if running different versions, otherwise have already populated
                #files and folders which will be confusing to the user.


                shutil.copy(self.ports[0].outfilegen(), self.outfilegen())

                self.file_modify(self.outfilegen()) #in the specific usage of this class, you can change file_modify to prepare file for analysis. 
                
                taskcomplete = self.input_w_art(self.print_manifest())

                if taskcomplete=='y':
                    #in this case, rename and move the file to correct output format.
                    #import code
                    #code.interact(local=dict(globals(), **locals()))

                    shutil.copy(self.get_userfile(), self.outfilegen())
                    
                    
                elif taskcomplete=='n':
                    self.user_cancelation()
                    os.remove(self.outfilegen())
                else:
                    raise ValueError("Invalid input to (y/n) prompt")
                
                
            elif userreview == 'n':
                self.user_cancelation() #this signifies when printing job output that a review was not performed
            else:
                raise ValueError("Invalid input to (y/n) prompt")
        #otherwise, do nothing and let the task fail
        #import code
        #code.interact(local=dict(globals(), **locals()))
        
        
        
        #idea: prompt user whether they want to perform tasks now, save preference as a temporary env variable if not. 
        
class Split_process:

    #reason these are loaded in this way instead of appended to arguments is because arguments is a 'frozendict' 
    splits = luigi.IntParameter()
    split_ID = luigi.IntParameter()
    process_ID = luigi.Parameter(significant=False)

    outpath_str = luigi.Parameter(significant=False)
    
    def outpath(self):
        return self.outpath_str
    def outfilegen(self,only_file=False):
    
        #import code
        #code.interact(local=locals())
        file = self.outfile
        outf1 = file[0:file.find(".")]
        outf2 = file[self.outfile.find("."):]
        outmod = outf1 + str(self.split_ID) + '_' + str(self.splits) + outf2
        #adding in splitnum here keeps names unique and does not cause partial outputs
        #to be interpreted as different size chunk between runs. 
        if only_file ==False:
            out =self.outpath() + "/" +outmod #testing
        elif only_file==True:
            out = outmod
        return out

class SplitRun_process:
    def requires(self):
        return self.SplitInitial(parameters=self.parameters,param_string=self.param_string,param_string2=self.param_string2,arguments=self.arguments,\
                                 descriptors=self.descriptors,ports=self.ports,ports_hash=self.ports_hash,splits=self.splits,\
                                 split_ID=self.split_ID,process_ID=self.process_ID,outpath_str = self.outpath(),processID=self.processID,\
                                 pipeID=self.pipeID,rerun_key=self.rerun_key)

class Unify_process:
    
    def requires(self):
        for k in range(int(self.arguments['splits'])):
            yield self.SplitRun(parameters=self.parameters,param_string=self.param_string,param_string2=self.param_string2,arguments=self.arguments,\
                                descriptors=self.descriptors,ports=self.ports,ports_hash=self.ports_hash,splits=int(self.arguments['splits']),\
                                split_ID=k+1,process_ID=self.__class__.__name__,outpath_str = self.outpath(),processID=self.processID,\
                                pipeID=self.pipeID,rerun_key=self.rerun_key)
#pipelines:

#note: the looping function currently relies on some AFSC submodule file formats, and supporting functions, to correctly combine outputs. 
class CombineExtLoop(INSTINCT_task):

    outfile = luigi.Parameter(significant=False)

    pipeID= luigi.Parameter(significant=False) #reference the pipeline that called the process
    
    def output(self):
        return luigi.LocalTarget(self.outfilegen())
    def run(self):

        if '.txt' in self.outfile: #this assumes it's going to be a raven format- can get more specific here
            #if you need other .txt formats. 
            
            dataframes = [None] * len(self.ports)
            for L in range(len(self.ports)):
                dataframes[L] = pd.read_csv(self.ports[L].outfilegen(), delimiter = "\t") #
            Dat = pd.concat(dataframes,ignore_index=True)

            Dat["Selection"] = Dat.index + 1

            #import code
            #code.interact(local=locals())
            Dat.to_csv(self.outfilegen(),index=False, sep = "\t")
        elif self.outfile=="no_comb" and ('.tgz' in self.ports[0].outfilegen() or '.tar.gz' in self.ports[0].outfilegen()):

            os.mkdir(self.outfilegen()) 

            for L in range(len(self.ports)):
                outpath_name = self.outfilegen() +"/" + str(L+1) + "_" + os.path.basename(self.ports[L].outfilegen()[:self.ports[L].outfilegen().index(".")])
                os.mkdir(outpath_name)
                with tarfile.open(self.ports[L].outfilegen(), "r:gz") as tar:
                    
                    #outdir =tarfile.open(self.outfile)
                    for member in tar:
                        if member.isdir():
                            continue
                        fname = member.name.rsplit('/',1)[1]
                        tar.makefile(member, outpath_name + '/' + fname)

                tar.close()

            #import code #unpack before combining into folder! Use commented out code at bottom of elifs
            #code.interact(local=dict(globals(), **locals()))

                    
                    #tar.extract(outpath_name)
                    
                    

        
            #create a directory called no_comb with all of the files
            #os.mkdir(self.outfilegen()) 
            #for L in range(len(self.ports)):
            #    shutil.copy(self.ports[L].outfilegen(), self.outfilegen() +"/" + str(L+1) + "_" + self.ports[L].outfile)
     
        elif self.outfile=="no_comb":
            #create a directory called no_comb with all of the files
            os.mkdir(self.outfilegen()) 
            for L in range(len(self.ports)):
                shutil.copy(self.ports[L].outfilegen(), self.outfilegen() +"/" + str(L+1) + "_" + self.ports[L].outfile)

        elif 'FileGroupFormat.csv.gz' in self.outfile:

            #FG_dict = file_peek(self.ports[0].outfilegen(),fn_type = object,fp_type = object,st_type = object,dur_type = 'float64')
            
            dataframes = [None] * len(self.ports)
            for L in range(len(self.ports)):
                dataframes[L] = pd.read_csv(self.ports[L].outfilegen())#, dtype=FG_dict)
            Dat = pd.concat(dataframes,ignore_index=True)
            Dat['StartTime'] = pd.to_datetime(Dat['StartTime'])
            #import code
            #code.interact(local=locals())

            Dat.drop(columns=['DiffTime'])

            Dat=get_difftime(Dat) #this is currently broken- difftime doesn't work between original format and format FG
        
            Dat.to_csv(self.outfilegen(),index=False)
        elif '.csv.gz' or '.csv.' in self.outfile:
            
            dataframes = [None] * len(self.ports)
            for L in range(len(self.ports)):
                dataframes[L] = pd.read_csv(self.ports[L].outfilegen()) #
            Dat = pd.concat(dataframes,ignore_index=True)

            Dat.to_csv(self.outfilegen(),index=False)
        #elif ".tgz" in self.outfile or ".tar.gz" in self.outfile:

            #pseudo: unpack all files into single directory, then tar that directory.

        #    import code
        #    code.interact(local=dict(globals(), **locals()))

        #    for L in range(len(self.ports)):
        #        outnamepath = self.outfile[:self.ports[L].outfilegen().index(".")]
        #        outdir =tarfile.open(self.outfile)

        #        outdir.extractall(outnamepath)            
            

       
        
            
    @classmethod
    def invoke(cls,params,component,pipe_args={},upstream=[None],process_peek=None,novr=False,pipeID=None,namespace = None,compdef=None,dag=None):
        #params in this case, just means a level above processes/subroutines

        paramssave=params
        params = params[process_peek] #defaults to formatFG, since usually looping through FG

        parameters = params["parameters"]

        #print(component)


        #figure out which parameter is the list

        keys = list(parameters.keys())

        n_len = None
        for i in range(len(keys)):
            if type(parameters[keys[i]])==list:
                n_len = len(parameters[keys[i]])

        #import code
        #code.interact(local=locals())

        if novr!=False:
            n_len = 1
            nval = novr
        else:
            nval = 0

        #print(pipe_args)

        upstreamNew=[component.invoke(paramssave,n=L,pipe_args=pipe_args.copy(),upstream=upstream,pipeID=pipeID,namespace = namespace,compdef=compdef,dag=dag) for L in range(n_len)]

        #import code
        #code.interact(local=dict(globals(), **locals()))
        #this makes sure only unique inputs are retained. If not unique, why run it? 
        upstreamNew = list(set(upstreamNew))
        n_len = len(upstreamNew)

        if n_len!=1:

            portsout = cls.getports(upstreamNew)

            noncombinable_formats = [".png","test",".tgz"]
            
            if any([(i in upstreamNew[0].outfile) for i in noncombinable_formats]):
                outmod = "no_comb"
            else:
                outmod = upstreamNew[0].outfile

            comb_process = cls(ports=portsout[0],ports_hash=portsout[1],outfile=outmod,pipeID=pipeID)

            return(comb_process)
        else:
            #if it isn't a loop, just return the component
            return component.invoke(paramssave,n=nval,pipe_args=pipe_args,upstream=upstream,pipeID=pipeID,namespace = namespace,compdef=compdef,dag=dag)

        #pass values into luigi task as parameters: outpath, hash, file type, etc. 

class INSTINCT_job(INSTINCT_task):

    job_out=luigi.Parameter(significant=False) #means if output folder is changed, will keep hash but rerun
    #in new folder
    param_file_name=luigi.Parameter(significant=False) #for saving params. duplication helps with tracking. 

    paramset_original=luigi.DictParameter(significant=False)

    pipenames=luigi.ListParameter(significant=False)
    
    def outpath(self):

        path = PARAMSET_GLOBALS['project_root'] +'Outputs/' + self.job_out + '/' + str(self._Task__hash)

        if not os.path.exists(path): #might be redundant, make sure not slowing anything down
            os.makedirs(path)

        return path
        
    def output(self):
        for n in range(len(self.ports)):
            filedest =self.outpath() + "/" + self.pipenames[n] + "_" + self.ports[n].outfile
            if '.gz' in filedest:
                filedest = filedest[0:-3]
            elif '.tgz' in filedest or '.tar.gz' in filedest:
                    
                filedest = filedest[:filedest.index(".")]
            yield luigi.LocalTarget(filedest)
        
    def run(self):

        #find the outputs and dump them in common folder. 

        nt.dump(self.paramset_original, self.outpath() +"/" +self.param_file_name +"_archive.nt")

        for n in range(len(self.ports)):

            if self.ports[n].outfile!="no_comb":

                #here, look for .gz and unzip before transfering! Make output not outfile but a non .gz modification of outfile. 
                #import code
                #code.interact(local=locals())
                
                filepath = self.ports[n].outfilegen()

                filedest =self.outpath() + "/" + self.pipenames[n] + "_" + self.ports[n].outfile 

                if '.gz' in filepath:
                    outnamepath = filedest[0:-3]
                    with gzip.open(filepath, 'rb') as f_in:
                        with open(outnamepath, 'wb') as f_out:
                            shutil.copyfileobj(f_in, f_out)
                elif '.tgz' in filepath or '.tar.gz' in filepath:
                    
                    outnamepath = filedest[:filedest.index(".")]
                    outdir =tarfile.open(filepath)

                    outdir.extractall(outnamepath)
                    
                    #import code
                    #code.interact(local=dict(globals(), **locals()))
                else:
                    shutil.copy(filepath, filedest)
                
            else:

                shutil.copytree(self.ports[n].outfilegen(), self.outpath() + "/" + self.pipenames[n] + "_" +  self.ports[n].outfile)
                
                #import code
                #code.interact(local=locals())
            

            #add in functionality here to unzip files in outputs!

        
    @classmethod
    def invoke(cls,job,paramset_original,param_file_name,pipenames,pipeID=None): #testing out change- params was 2nd arg here 

        #import code
        #code.interact(local=locals())
    
        job_out=list(job.keys())[0]
        upstream=list(job.values())[0]        

        portsout = cls.getports(upstream)

        return(cls(ports=portsout[0],ports_hash=portsout[1],job_out=job_out,pipenames=pipenames,\
                   paramset_original=paramset_original,param_file_name=param_file_name))

class INSTINCT_job_wrapper(luigi.WrapperTask,INSTINCT_job): #may not work, as both inherit from luigi.task...
    pass



##############################################

    

