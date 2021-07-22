import luigi
import os
import hashlib
import configparser
import pandas as pd
import sys
import numpy
import subprocess
import shlex
import shutil

from supporting.instinct import *
from supporting.getParams import *
from supporting.job_fxns import * 

#this runs one RFN to view in Raven. If wanting to generate in loop, or use outputs, use the other job, RunFullNovel.  

class ExternalPerfEval(FormatFG,ServeModel,FormatGT,AssignLabels,): #,AssignLabels,PerfEval2

    JobName=luigi.Parameter()

    GT_signal_code=luigi.Parameter()
    #nullify some inherited parameters:
    PE2datType=None

    upstream_task1=None
    upstream_task2=None
    upstream_task3=None

    def pipelineMap(self):
        
            task0 = FormatFG.invoke(self,n=0,src="GT")
            task1 = FormatGT.invoke(self,task0,n=0)
            task2 = ServeModel.invoke(self,task0)

            task3 = AssignLabels.invoke(self,task2,task1,task0)
            #task4 = PerfEval2.invoke(self,task3,None,"NoPE1")

            return [task3]
    def hashProcess(self):
        taskStr = []
        tasks = self.pipelineMap()
        for f in range(len(tasks)):
            taskStr.extend([tasks[f].hashProcess()])
            
        hashStrings = ' '.join(taskStr)

        return Helper.getParamHash2(' '.join(hashStrings),6)
    def requires(self):
        tasks = self.pipelineMap()
        return tasks[0]
    def outpath(self):
        return self.ProjectRoot +'Outputs/' + self.JobName + '/' + self.hashProcess()
    def output(self):
        return luigi.LocalTarget(self.outpath() + '/DETx.csv.gz')
    def run(self):

        tasks = self.pipelineMap()
        filepath = tasks[0].outpath() + '/DETx.csv.gz'
        filedest = self.outpath() + '/DETx.csv.gz'

        if not os.path.exists(self.ProjectRoot +'Outputs/' + self.JobName):
            os.mkdir(self.ProjectRoot +'Outputs/' + self.JobName)

        if not os.path.exists(self.outpath()):
            os.mkdir(self.outpath())

        shutil.copy(filepath, filedest)
        
    def invoke(obj):
        return(ExternalPerfEval(JobName=obj.JobName,ProjectRoot=obj.ProjectRoot,SoundFileRootDir_Host_Raw=obj.SoundFileRootDir_Host_Raw,\
                            FGfile=obj.FGfile,SoundFileRootDir_Host_Dec=obj.SoundFileRootDir_Host_Dec,FGparamString = obj.FGparamString,FGmethodID = obj.FGmethodID,\
                            decimatedata = obj.decimatedata,SMprocess=obj.SMprocess,SMmethodID=obj.SMmethodID,SMvenv_type=obj.SMvenv_type,SMvenv_name=obj.SMvenv_name,\
                            ALprocess=obj.ALprocess,ALmethodID=obj.ALmethodID,ALparamString=obj.ALparamString,\
                            SMparamString=obj.SMparamString,GTfile=obj.GTfile,GT_signal_code=obj.GT_signal_code,CacheRoot=obj.CacheRoot,system=obj.system))
    def getParams(args):

        #thought- in here I could use parameters to declare which kind of model I was applying, so I could make pipelines compositional. But, the problem is in how I am defining parameters in .invoke. This
        #is something I could take a look at- I am not really explicity using the Luigi parameter functionality, I could just try to pass the parameter object itself, and not define luigi parameters. Maybe
        #the classess will just have these parameters self object? Maybe they need a self object? I am not sure, I could use some help here. 
        
        params = Load_Job('ExternalPerfEval',args)

        params = FG(params,'FormatFG')
        params = GT(params,'FormatGT')
        params = SM(params,'ServeModel')
        params = GT(params,'FormatGT')
        
        params = AL(params,'AssignLabels')
        #params = AL(params,'PerfEval2')

        return params
    
if __name__ == '__main__':
    deployJob(ExternalPerfEval,sys.argv)

