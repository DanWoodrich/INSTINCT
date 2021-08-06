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
from ViewRFN import *

from supporting.instinct import *
from supporting.Comb4FeatureTrain import *
from supporting.getParams import *
from supporting.job_fxns import * 

#this runs one RFN to view in Raven. If wanting to generate in loop, or use outputs, use the other job, RunFullNovel.  

class ViewRFN_reduce(ApplyModel,Comb4FeatureTrain,TrainModel,ApplyCutoff,RavenViewDETx,RavenToDETx,ReduceByGT):

    JobName=luigi.Parameter()
    
    n_FGfile = luigi.Parameter()
    n_FileGroupID = luigi.Parameter()
    n_IDlength= luigi.IntParameter()

    GT_signal_code= luigi.Parameter()
    #nullify some inherited parameters:
    PE2datType=None

    upstream_task1=None
    upstream_task2=None
    upstream_task3=None

    loopVar = None

    RavenFill = None

    fileName = None

    def pipelineMap(self):
            task0 = FormatFG.invoke(self,n=0,src="n_")
            task1 = ViewRFN.invoke(self)
            task2 = RavenToDETx.invoke(self,task1,task0)
            task3 = ReduceByGT.invoke(self,task2,task0)

            return [task0,task1,task2,task3]
    def hashProcess(self):
        taskStr = []
        tasks = self.pipelineMap()
        for f in range(len(tasks)):
            taskStr.extend([tasks[f].hashProcess()])
            
        hashStrings = ' '.join(taskStr)

        return Helper.getParamHash2(' '.join(hashStrings),6)
    def requires(self):
        tasks = self.pipelineMap()
        return tasks[3]
    def outpath(self):
        return self.ProjectRoot +'Outputs/' + self.JobName + '/' + self.hashProcess()
    def output(self):
        yield luigi.LocalTarget(self.outpath() + '/' + self.FileGroupID[0][0:(len(self.FileGroupID[0])-4)] + '_reduce.csv')
        return luigi.LocalTarget(self.outpath() + '/' + self.GT_signal_code + '_' + self.FileGroupID[0][0:(len(self.FileGroupID[0])-4)] + '_reduce.csv')
    def run(self):

        tasks = self.pipelineMap()
        filepath1 = tasks[3].outpath() + '/DETx.csv.gz'
        filepath2 = tasks[3].outpath() + '/FileGroupFormat.csv.gz'

        GT = pd.read_csv(filepath1)
        if not os.path.exists(self.ProjectRoot +'Outputs/' + self.JobName):
            os.mkdir(self.ProjectRoot +'Outputs/' + self.JobName)

        if not os.path.exists(self.outpath()):
            os.mkdir(self.outpath())

        GT.to_csv(self.outpath() + '/' + self.GT_signal_code + '_' + self.n_FileGroupID[0][0:(len(self.n_FileGroupID[0])-4)] + '_reduce.csv',index=False)

        FG = pd.read_csv(filepath2)

        FG.to_csv(self.outpath() + '/' + self.n_FileGroupID[0][0:(len(self.n_FileGroupID[0])-4)] + '_reduce.csv',index=False)
        
    def invoke(obj):
        return(ViewRFN_reduce(JobName=obj.JobName,ProjectRoot=obj.ProjectRoot,SoundFileRootDir_Host_Raw=obj.SoundFileRootDir_Host_Raw,\
                            IDlength=obj.IDlength,FGfile=obj.FGfile,FileGroupID=obj.FileGroupID,SoundFileRootDir_Host_Dec=obj.SoundFileRootDir_Host_Dec,\
                            GTfile=obj.GTfile,EDprocess=obj.EDprocess,EDsplits=obj.EDsplits,EDcpu=obj.EDcpu,\
                            EDchunk=obj.EDchunk,EDmethodID=obj.EDmethodID,EDparamString=obj.EDparamString,GT_signal_code=obj.GT_signal_code,\
                            EDparamNames=obj.EDparamNames,ALprocess=obj.ALprocess,ALmethodID=obj.ALmethodID,\
                            ALparamString=obj.ALparamString,FEprocess=obj.FEprocess,FEmethodID=obj.FEmethodID,\
                            FEparamString=obj.FEparamString,FEparamNames=obj.FEparamNames,FEsplits=obj.FEsplits,\
                            FEcpu=obj.FEcpu,MFAprocess=obj.MFAprocess,MFAmethodID=obj.MFAmethodID,FGparamString=obj.FGparamString,\
                            FGmethodID=obj.FGmethodID,decimatedata = obj.decimatedata,RVmethodID=obj.RVmethodID,\
                            RGmethodID=obj.RGmethodID,RGparamString=obj.RGparamString,RDmethodID=obj.RDmethodID,\
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
        params = RD(params,'RavenToDETx')
        params = RG(params,'ReduceByGT')

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
    deployJob(ViewRFN_reduce,sys.argv)

