#functions related to running jobs 
import time
import luigi
import random
import os
import nestedtext as nt
from classes import *
from getglobals import PARAMSET_GLOBALS
import luigi.tools.deps_tree as deps_tree
import numpy
from google.cloud import storage
from urllib.parse import urlparse

#need for namespace
from user.processes import *
from pipe_shapes import *

import user.definitions

from art import getArt

################this section by github: diogoffmelo
#I changed some naming upstream/downstream over backward/forward

from collections import defaultdict, OrderedDict

from luigi.task import flatten, getpaths

def topological_sorting(struct, outnodes_funct, transform_funct):
    struct = flatten(struct.keys()) if isinstance(struct, dict) else flatten(struct)
    visited = OrderedDict()
    def dvisit(root):
        if root in visited.keys():
            return

        outnodes = flatten(outnodes_funct(root))
        for o in outnodes:
            dvisit(o)

        visited.update({root: transform_funct(root)})

    for root in struct:
        dvisit(root)

    return OrderedDict(reversed(visited.items()))


def to_dag(struct, outnodes_funct):
    inv_dag = defaultdict(list)
    def inv_visit_function(root):
        outnodes = flatten(outnodes_funct(root))
        for o in outnodes:
            inv_dag[o].append(root)

        return outnodes

    dag = topological_sorting(struct, 
                              outnodes_funct, 
                              inv_visit_function)


    return dag, inv_dag


def clear_task_output(task):
    for output in flatten(task.output()):
        # This works for LocalTargetOutput
        # Add here your per class notion of 'clear'

        #note to me: I could add getting rid of folders here too 
        if output.exists():
            output.remove()

def clear_task_dag_output(struct, dag):
    def outnodes_funct(root):
        return dag[root]

    for root in flatten(struct):
        topological_sorting(root, outnodes_funct, clear_task_output)


def task_outnodes_funct(task):
    return flatten(task.requires())


class Dag(object):
    def __init__(self, lasttask):
        # lasttask(s) should be the last task to be executed (no task depends on it)
        self.struct = lasttask
        self._build()

    def _build(self):
        self.dag, self.inv_dag = to_dag(self.struct, task_outnodes_funct)

    def clean_upstream(self, tasks):
        # Clean (recursively) all dependencies of tasks
        return self._clean(tasks, direction='upstream')

    def clean_downstream(self, tasks):
        # Clean (recursively) all tasks that depend on those
        return self._clean(tasks, direction='downstream')

    def clean_all(self, tasks):
        return self._clean(tasks, direction='all')

    def _clean(self, tasks, direction=None):
        if direction in ['all', 'upstream']:
            clear_task_dag_output(tasks, self.dag)

        if direction in ['all', 'downstream']:
            clear_task_dag_output(tasks, self.inv_dag)


################end test section
    

def secToDHMS(time):
    day = time // (24 * 3600)
    time = time % (24 * 3600)
    hour = time // 3600
    time %= 3600
    minutes = time // 60
    time %= 60
    seconds = time
    return("%d:%d:%d:%d" % (day, hour, minutes, seconds))

def deployJob(paramset,args,paramset_original,print_tree,novr,GLOBAL_NAMESPACE):

    #I should have this also output params to outpath, to refer to later!
    
    start = time.time()
    if PARAMSET_GLOBALS['Wrapper'] == "False":
        job = INSTINCT_job
    else:
        job = INSTINCT_job_wrapper

    os.environ["CANCELREVIEW"]="False" #set default env value

    #import code
    #code.interact(local=dict(globals(), **locals()))

    #declare env variable and remove quotes.
    if 'custom_argument_seperator' in paramset['Global']:
        sepvar= paramset['Global']['custom_argument_seperator']
        assert sepvar[0] == "'" and sepvar[len(sepvar)-1] == "'", "Custom argument seperator must be bounded by single quotes"
        os.environ["INS_ARG_SEP"] = sepvar[1:len(sepvar)-1]
    else:
        os.environ["INS_ARG_SEP"] = " "
    
    job_dets,pipenames = StagedJob(args[1],paramset,novr,GLOBAL_NAMESPACE,None).getJob()
    inv = job.invoke(job_dets,paramset_original,args[3],pipenames=pipenames)

    dag = Dag(inv)

    #this is repetitive, but can't think of any other way to do this... enables hard rerun
    job_dets,pipenames = StagedJob(args[1],paramset,novr,GLOBAL_NAMESPACE,dag).getJob()
    inv = job.invoke(job_dets,paramset_original,args[3],pipenames=pipenames)

    
    result=luigi.build([inv], local_scheduler=True) and inv.complete()
    randNum=random.randint(1,10)
    end = time.time()

    #import code
    #code.interact(local=dict(globals(), **locals()))

    if os.environ["CANCELREVIEW"] =="True":
        failmessage = "                                === Job failed ===:\n" +\
                      "                              'userprocess canceled'"
    else:
        failmessage = "                               === Job failed ==="
    if print_tree:
        print(deps_tree.print_tree(inv)) 
        print(r"""                 . ,'..'.,'; '. .'.,',; ,' :','..'.,', '. ; ,','""")

    if result:
        if bool(PARAMSET_GLOBALS.get('GCS_output')):

            print(f"saving job output to GCS: {PARAMSET_GLOBALS['GCS_path']}")
            #save to cloud storage. Use specified name, save cache outpath for possible future use, otherwise
            #just save file contents in outpath.

            storage_client = storage.Client()

            parsed_uri = urlparse(PARAMSET_GLOBALS['GCS_path'])

            bucket = storage_client.bucket(parsed_uri.netloc)
            prefix = parsed_uri.path.lstrip('/')

            #recurse and upload all files
            for root, _, files in os.walk(inv.outpath()):
                for filename in files:
                    # Construct the full local path of the file to upload
                    local_file_path = os.path.join(root, filename)
                    
                    # Construct the destination blob name by preserving the folder structure
                    relative_path = os.path.relpath(local_file_path, inv.outpath())
                    # GCS uses forward slashes as separators
                    gcs_path = os.path.join(prefix, relative_path).replace(os.sep, '/')

                    blob = bucket.blob(gcs_path)
                    
                    blob.upload_from_filename(local_file_path)
            #write out what was the local path
            blob = bucket.blob(os.path.join(prefix, "local_path.txt").replace(os.sep, '/'))
            blob.upload_from_string(inv.outpath(), content_type="text/plain")
        if(PARAMSET_GLOBALS['Wrapper'] == "False"):
            print("                     ,'..'. Output file location path: ':',,''\n" + "                   " +inv.outpath())
            if os.name == 'nt': #if system is windows- explorer not a thing on linux
                os.system("start " + inv.outpath())
        elif(bool(PARAMSET_GLOBALS['Wrapper'])):
            print("                             Wrapper Job Successful!                   ")
        print("                        elapsed time (d:h:m:s): " + str(secToDHMS(round(end-start,0))))    
    else:
        print(failmessage)
    print(getArt(args[2],result,num=randNum))

class StagedJob:

    def __init__(self,job,params,novr,namespace,dag):
        self.job = job
        self.params = params
        self.novr = novr
        self.namespace = namespace
        self.dag = dag

    def pipe_stage(self,pipe_args):

        #return {self.job:[}

        #import code
        #code.interact(local=locals())

        #test for a params_drop here!

        if 'params_drop' in pipe_args:
            params_copy = self.params.copy()
            #import code
            #code.interact(local=locals())
            #print("******did params drop!*******")
            #print(pipe_args['params_drop'])
            #print("******did params drop!*******")
            params_copy = param_smoosh(params_copy,pipe_args['params_drop'])
            #del pipe_args['params_drop']
        else:
            #might be able to remove this copy
            params_copy = self.params.copy()

        #test for .pipe notation
        if 'pipe' in pipe_args:
            pipe_args,pipe_val,compdef,pipeID =pipetest_pargs_pval_compdef(pipe_args,self.namespace)

            #import code
            #code.interact(local=dict(globals(), **locals())) 
            #import code
            #code.interact(local=dict(globals(), **locals()))
            #see if it's a loop:
            if 'loop_on' in pipe_args:
                #print('didit')
                return  CombineExtLoop.invoke(params=params_copy,component=pipe_val,pipe_args=pipe_args,\
                                             pipeID=pipeID,namespace = self.namespace,compdef=compdef,\
                                             process_peek = pipe_args['loop_on'],dag = self.dag)
                
            else:
                return pipe_val.invoke(params=params_copy,pipe_args=pipe_args,n=self.novr,namespace=self.namespace,compdef=compdef,dag = self.dag)
        else: #must be a process
            return self.namespace[pipe_args['process']].invoke(params=params,pipe_args={},n=self.novr,namespace=self.namespace,compdef=None,dag = self.dag)
    def getJob(self):

        #1st, expand jobs into pipelines (search if jobs are nested).
        pipelist = []

        

        jobsnames = self.search_namesp(self.job,pipelist,ret_type="pipenames")

        #import code
        #code.interact(local=dict(globals(), **locals()))

        jobslist = self.search_namesp(self.job,pipelist)
        
        #might be able to remove this copy

        jobsnames_sort = jobsnames.copy()
        jobsnames_sort.sort()

        #find index of alphabetical sort
        sort_index = [jobsnames.index(i) for i in jobsnames_sort]
        jobsnames = jobsnames_sort

        #resort jobslist by index
        jobslist = [jobslist[i] for i in sort_index]
                
        #this line will recursively look through namespace to find all pipelines that need to be run. 
        return {self.job:jobslist},jobsnames

    def search_namesp(self,job,pipelist,ret_type = 'pipeargs'):

        if job in user.definitions.jobs:  
            jobout = user.definitions.jobs[job]
            pipelist = pipelist + sum([self.search_namesp(jobout[i],pipelist,ret_type=ret_type) for i in range(len(jobout))],[])#recursion
        else: #this means that job is just a pipeline

            if ret_type == 'pipeargs':  
                pipelist = pipelist + [self.pipe_stage(pipe_search(job))]
            elif ret_type=='pipenames':
                pipelist = pipelist + [job]

        return pipelist
    
def pa_fromfile(job,source):

    filepath =PARAMSET_GLOBALS["project_root"] + "lib/" + source + "/pipelines/"+ job + ".nt"

    if os.path.isfile(filepath):
       
        return True,nt.load(filepath)
    else:
        return False,None

def pipe_search(job):

    #search code defined pipelines
    if job in user.definitions.pipelines:
        return user.definitions.pipelines[job]
    elif pa_fromfile(job,'user')[0]:
        return pa_fromfile(job,'user')[1]
    else:
         raise NameError('Pipeline name not found')
    


        

