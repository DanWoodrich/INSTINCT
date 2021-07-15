import hashlib
import configparser
from collections import namedtuple

#ID='AssignLabels' #this can be modified to distinguish reused processes in same pipeline

def ProcessParams(self,ID):    
    self.ID = ID
    self.ID.process = 'AssignLabels'
    self.ID.methodID = 'methodID-v1-0.R' #self.MasterINI[ID]['MethodID']
    return self

#this is just pseudocode for now to help visualize. 
class RunDetector():
    def getParams(master,runtype):
        if runtype == 'Original':
            paramsStruct = namedtuple('params', 'EventDetector FeatureExtraction ApplyModel')
            #what you'd really do, is instead of declaring dictionary here, call .getParams for each process, which will export dictionary
#           #get params will use method ID specified in master for given process.
            #EventDetector=EventDetector.getParams(master) #will read EventDetectorMethod from master, use it to load params as a dict.
            #might be worth thinking harder about param organization, standard loading, and distinctions for hashing/passing to methods. 
            params = paramsStruct(EventDetector={'Splits':1,'ParamString':'test 5 78 param2 param3val','method':'contour_xtra_crazy-v1-0.R','process':'EventDetector'}\
                                  ,FeatureExtraction={'Splits':5,'ParamString':'bogideebeeboo 23 11','method':'coolFE.R','process':'FeatureExtraction'}\
                                  ,ApplyModel={'you':'get','the':'idea'})
        elif runtype == 'DL':
            paramsStruct = namedtuple('params', 'ServeModel')
            params = paramsStruct(ServeModel={'ParamString':'testtensorflow x2 param90 2','method':'serve-tenborflow-v1-0.py','process':'ServeModel'})
        return params
            
    #self will contain args for each process
    #def pipelineMap(self,params,runtype,upstream1):

    #    if runtype == 'Original':
    #        task0=ED(params.ED,upstream1)
    #        task1=FE(params.FE,task0)
    #        task2=AM(params.AM,task1) #produce detwprobs

    #        return task2
        
    #    elif runtype == 'DL':
    #        task0=SM(params.SM,upstream1) #produce detwprobs 

    #        return task0


#this is the empty object to append more params onto. This will really be the
#job params, and will include masterINI (as in job_fxns.py)
#class Params():
#    pass

#named tuple does not support overrides. So, would need to first declare and object for master, then use
#contents of master to build a new struct (also containing master) for a single params structure. 

paramsStructMaster = namedtuple('params', 'master')

paramsMaster = paramsStructMaster(master = {'RDruntype':'Original','EventDetectorMethod':'contour_xtra_crazy-v1-0.R'}) #this will normally be output of getParams, which will be dictionary 

paramsStruct = namedtuple('params', 'master RunDetector')

#nice, this works!
params = paramsStruct(master = paramsMaster.master,RunDetector=RunDetector.getParams(paramsMaster.master,params.master['RDruntype']))

#issue I will likely run into later- this prevents me from cleanly pulling out vars from global, even other methods, for
#use in different processes. I can probably append dictionaries as necessary, but that gets messy.... unsure what is best to do here. 


#later:
task0=FormatFG()
task1=RunDetector.pipelineMap(params.RD,params.master.RDruntype,task0) #will return last process object in pipeline
task2=RavenFromDETx(task1)



#next thing I want to do here is create a task template, inherit it, and make
#a dummy pipeline to test out if the parameter passing works. 
