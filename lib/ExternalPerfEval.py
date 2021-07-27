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

class ExternalPerfEval(FormatFG,ServeModel,FormatGT,AssignLabels,PerfEval2,PerfEval1_s1,ApplyCutoff,RavenViewDETx): #,AssignLabels,PerfEval2

    JobName=luigi.Parameter()

    GT_signal_code=luigi.Parameter()
    #nullify some inherited parameters:
    PE2datType=None

    RavenFill=None


    upstream_task1=None
    upstream_task2=None
    upstream_task3=None

    def pipelineMap(self):
        
            task0 = FormatFG.invoke(self,n=0,src="GT")
            task1 = FormatGT.invoke(self,task0,n=0)
            task2 = ServeModel.invoke(self,task0)

            task3 = AssignLabels.invoke(self,task2,task1,task0)
            task4 = PerfEval1_s1.invoke(self,task3,task0,task3,n=0,src="GT")
            task5 = PerfEval2.invoke(self,task3,task4,"FG")

            #add in processes to view various components of this

            task6 = ApplyCutoff.invoke(self,task3)
            task7 = RavenViewDETx.invoke(self,task6,task0)


            return [task4,task5,task7]
    def hashProcess(self):
        taskStr = []
        tasks = self.pipelineMap()
        for f in range(len(tasks)):
            taskStr.extend([tasks[f].hashProcess()])
            
        hashStrings = ' '.join(taskStr)+"a" #TEMP, DELETE THIS

        return Helper.getParamHash2(' '.join(hashStrings),6)
    def requires(self):
        tasks = self.pipelineMap()
        yield tasks[0]
        yield tasks[1]
        yield tasks[2]
    def outpath(self):
        return self.ProjectRoot +'Outputs/' + self.JobName + '/' + self.hashProcess()
    def output(self):
        return luigi.LocalTarget(self.outpath() + '/RAVENx.txt')
    def run(self):
        #move file
        tasks = self.pipelineMap()
        filepath = tasks[2].outpath() + '/RAVENx.txt'
        filedest = self.outpath() + '/RAVENx.txt'

        if not os.path.exists(self.ProjectRoot +'Outputs/' + self.JobName):
            os.mkdir(self.ProjectRoot +'Outputs/' + self.JobName)

        if not os.path.exists(self.outpath()):
            os.mkdir(self.outpath())

        shutil.copy(filepath, filedest)

        
    def invoke(obj):
        return(ExternalPerfEval(JobName=obj.JobName,ProjectRoot=obj.ProjectRoot,SoundFileRootDir_Host_Raw=obj.SoundFileRootDir_Host_Raw,\
                            FGfile=obj.FGfile,SoundFileRootDir_Host_Dec=obj.SoundFileRootDir_Host_Dec,FGparamString = obj.FGparamString,FGmethodID = obj.FGmethodID,\
                            decimatedata = obj.decimatedata,SMprocess=obj.SMprocess,SMmethodID=obj.SMmethodID,SMvenv_type=obj.SMvenv_type,SMvenv_name=obj.SMvenv_name,\
                            ALprocess=obj.ALprocess,ALmethodID=obj.ALmethodID,ALparamString=obj.ALparamString,PE1process=obj.PE1process,PE1methodID=obj.PE1methodID,\
                            PE2process=obj.PE2process,PE2methodID=obj.PE2methodID,FileGroupID=obj.FileGroupID,ACcutoffString=obj.ACcutoffString,RVmethodID=obj.RVmethodID,\
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
        params = PE2(params,'PerfEval2')
        params = PE1(params,'PerfEval1')
        params = RV(params,'RavenViewDETx')
        params = AC(params,'ApplyCutoff')

        return params
    
if __name__ == '__main__':
    deployJob(ExternalPerfEval,sys.argv)

