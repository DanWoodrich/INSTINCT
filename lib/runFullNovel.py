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

#this is a wrapper job to run a lot of RFN. Places in Cache, not in outputs. 

class RunFullNovel(luigi.WrapperTask,ApplyModel,Comb4FeatureTrain,TrainModel,ApplyCutoff):

    JobName=luigi.Parameter()
    
    n_FGfile = luigi.Parameter()
    n_FileGroupID = luigi.Parameter()
    n_IDlength= luigi.IntParameter()

    #nullify some inherited parameters:

    upstream_task1=None
    upstream_task2=None
    upstream_task3=None

    def pipelineMap(self,l):
            task0 = Comb4FeatureTrain.invoke(self)
            task1 = TrainModel.invoke(self,task0)
            
            task2 = FormatFG.invoke(self,n=l,src="n_") 
            task3 = UnifyED.invoke(self,task2)
            task4 = UnifyFE.invoke(self,task3,task2)
            task5 = ApplyModel.invoke(self,task4,task1,task2)
            task6 = ApplyCutoff.invoke(self,task5)

            return [task0,task1,task2,task3,task4,task5,task6]
    def requires(self):
        for l in range(self.loopVar):
            tasks = self.pipelineMap(l)
            yield tasks[6]
    #def complete(self): #this is the luigi wrapper logic, just says the job is complete if requirements are satisfied.
    #    return all(r.complete() for r in flatten(self.requires()))
    def invoke(obj):
        return(RunFullNovel(JobName=obj.JobName,ProjectRoot=obj.ProjectRoot,SoundFileRootDir_Host_Raw=obj.SoundFileRootDir_Host_Raw,\
                            IDlength=obj.IDlength,FGfile=obj.FGfile,FileGroupID=obj.FileGroupID,SoundFileRootDir_Host_Dec=obj.SoundFileRootDir_Host_Dec,\
                            GTfile=obj.GTfile,EDprocess=obj.EDprocess,EDsplits=obj.EDsplits,EDcpu=obj.EDcpu,\
                            EDchunk=obj.EDchunk,EDmethodID=obj.EDmethodID,EDparamString=obj.EDparamString,\
                            EDparamNames=obj.EDparamNames,ALprocess=obj.ALprocess,ALmethodID=obj.ALmethodID,\
                            ALparamString=obj.ALparamString,FEprocess=obj.FEprocess,FEmethodID=obj.FEmethodID,\
                            FEparamString=obj.FEparamString,FEparamNames=obj.FEparamNames,FEsplits=obj.FEsplits,\
                            FEcpu=obj.FEcpu,MFAprocess=obj.MFAprocess,MFAmethodID=obj.MFAmethodID,FGparamString=obj.FGparamString,\
                            FGmethodID=obj.FGmethodID,decimatedata = obj.decimatedata,loopVar=obj.n_IDlength,\
                            TMprocess=obj.TMprocess,TMmethodID=obj.TMmethodID,TMparamString=obj.TMparamString,TMstage=obj.TMstage,\
                            TM_outName=obj.TM_outName,TMcpu=obj.TMcpu,ACcutoffString=obj.ACcutoffString,n_FileGroupID=obj.n_FileGroupID,\
                            n_IDlength=obj.n_IDlength,n_FGfile=obj.n_FGfile,system=obj.system,r_version=obj.r_version))
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

        #FG for novel data
        n_params = Load_Job('runFullNovel',args)
        n_params = FG(n_params,'FormatFGapply')

        #only retain these ones. 
        params.n_FileGroupID = n_params.FileGroupID
        params.n_FGfile = n_params.FGfile
        params.n_IDlength = n_params.IDlength

        return params
    
if __name__ == '__main__':
    deployJob(RunFullNovel,sys.argv,"Wrapper")

