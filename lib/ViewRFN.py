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
from supporting.Comb4FeatureTrain import *
from supporting.getParams import *
from supporting.job_fxns import * 

#this runs one RFN to view in Raven. If wanting to generate in loop, or use outputs, use the other job, RunFullNovel.  

class ViewRFN(ApplyModel,Comb4FeatureTrain,TrainModel,ApplyCutoff,RavenViewDETx):

    JobName=luigi.Parameter()
    
    n_FGfile = luigi.Parameter()
    n_FileGroupID = luigi.Parameter()
    n_IDlength= luigi.IntParameter()

    #nullify some inherited parameters:
    PE2datType=None

    upstream_task1=None
    upstream_task2=None
    upstream_task3=None

    loopVar = None

    RavenFill = None

    fileName = None

    def pipelineMap(self):
            task0 = Comb4FeatureTrain.invoke(self)
            task1 = TrainModel.invoke(self,task0)
            
            task2 = FormatFG.invoke(self,n=0,src="n_") 
            task3 = UnifyED.invoke(self,task2)
            task4 = UnifyFE.invoke(self,task3,task2)
            task5 = ApplyModel.invoke(self,task4,task1,task2)
            task6 = ApplyCutoff.invoke(self,task5)
            task7 = RavenViewDETx.invoke(self,task6,task2,"F")

            return [task0,task1,task2,task3,task4,task5,task6,task7]
    def hashProcess(self):
        taskStr = []
        tasks = self.pipelineMap()
        for f in range(len(tasks)):
            taskStr.extend([tasks[f].hashProcess()])
            
        hashStrings = ' '.join(taskStr)

        return Helper.getParamHash2(' '.join(hashStrings),6)
    def requires(self):
        tasks = self.pipelineMap()
        return tasks[7]
    def outpath(self):
        return self.ProjectRoot +'Outputs/' + self.JobName + '/' + self.hashProcess()
    def output(self):
        return luigi.LocalTarget(self.outpath() + '/RAVENx.txt')
    def run(self):

        tasks = self.pipelineMap()
        filepath = tasks[7].outpath() + '/RAVENx.txt'
        filedest = self.outpath() + '/RAVENx.txt'

        if not os.path.exists(self.ProjectRoot +'Outputs/' + self.JobName):
            os.mkdir(self.ProjectRoot +'Outputs/' + self.JobName)

        if not os.path.exists(self.outpath()):
            os.mkdir(self.outpath())

        shutil.copy(filepath, filedest)
        
    def invoke(obj):
        return(ViewRFN(JobName=obj.JobName,ProjectRoot=obj.ProjectRoot,SoundFileRootDir_Host_Raw=obj.SoundFileRootDir_Host_Raw,\
                            IDlength=obj.IDlength,FGfile=obj.FGfile,FileGroupID=obj.FileGroupID,SoundFileRootDir_Host_Dec=obj.SoundFileRootDir_Host_Dec,\
                            GTfile=obj.GTfile,EDprocess=obj.EDprocess,EDsplits=obj.EDsplits,EDcpu=obj.EDcpu,\
                            EDchunk=obj.EDchunk,EDmethodID=obj.EDmethodID,EDparamString=obj.EDparamString,\
                            EDparamNames=obj.EDparamNames,ALprocess=obj.ALprocess,ALmethodID=obj.ALmethodID,\
                            ALparamString=obj.ALparamString,FEprocess=obj.FEprocess,FEmethodID=obj.FEmethodID,\
                            FEparamString=obj.FEparamString,FEparamNames=obj.FEparamNames,FEsplits=obj.FEsplits,\
                            FEcpu=obj.FEcpu,MFAprocess=obj.MFAprocess,MFAmethodID=obj.MFAmethodID,FGparamString=obj.FGparamString,\
                            FGmethodID=obj.FGmethodID,decimatedata = obj.decimatedata,RVmethodID=obj.RVmethodID,\
                            TMprocess=obj.TMprocess,TMmethodID=obj.TMmethodID,TMparamString=obj.TMparamString,TMstage=obj.TMstage,\
                            TM_outName=obj.TM_outName,TMcpu=obj.TMcpu,ACcutoffString=obj.ACcutoffString,n_FileGroupID=obj.n_FileGroupID,\
                            n_IDlength=obj.n_IDlength,n_FGfile=obj.n_FGfile,system=obj.system,CacheRoot=obj.CacheRoot))
    def getParams(args):
        
        params = Load_Job('runFullNovel',args)

        params = FG(params,'FormatFG')
        params = GT(params,'FormatGT')
        params = ED(params,'EventDetector')
        params = FE(params,'FeatureExtraction')
        params = AL(params,'AssignLabels')
        params = MFA(params,'MergeFE_AL')
        params = TM(params,'TrainModel','train')
        params = AC(params,'ApplyCutoff')
        params = RV(params,'RavenViewDETx')

        #FG for novel data
        n_params = Load_Job('runFullNovel',args)
        if len(args)==4:
            n_params = FG(n_params,'FormatFGapply',FGovr=args[3])
        else:
            n_params = FG(n_params,'FormatFGapply') #will just do the 1st one by default 

        #only retain these ones. 
        params.n_FileGroupID = n_params.FileGroupID
        params.n_FGfile = n_params.FGfile
        params.n_IDlength = n_params.IDlength

        return params
    
if __name__ == '__main__':
    deployJob(ViewRFN,sys.argv)

