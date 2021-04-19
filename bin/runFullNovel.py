import luigi
import os
import hashlib
import configparser
import pandas as pd
import sys
import numpy
import subprocess
import shlex
from instinct import *
from Comb4FeatureTrain import Comb4FeatureTrain
from getParams import *

#Run a model on data that does not need to have labels. 

RFN_params = Load_Job('runFullNovel')

RFN_params = FG(RFN_params,'FormatFG')
RFN_params = GT(RFN_params,'FormatGT')
RFN_params = ED(RFN_params,'EventDetector')
RFN_params = FE(RFN_params,'FeatureExtraction')
RFN_params = AL(RFN_params,'AssignLabels')
RFN_params = MFA(RFN_params,'MergeFE_AL')
RFN_params = TM(RFN_params,'TrainModel','train')
RFN_params = AC(RFN_params,'ApplyCutoff')

#novel data params

#FG for novel data
n_RFN_params = Load_Job('runFullNovel')
n_FGparams = FG(n_RFN_params,'FormatFGapply')

#if other args are present, load it in as FGID instead of what is on params.
#note that this is copy pasted from getParams, and is kind of hacky. Potential for conflicts if I'm still using this and change FG()
if len(sys.argv)>1:
    n_FGparams.FileGroupID=sys.argv[1]
    n_FGparams.FileGroupID = sorted(n_FGparams.FileGroupID.split(','))
    n_FGparams.IDlength = len(n_FGparams.FileGroupID)
    n_FGparams.FGfile = [None] * n_FGparams.IDlength

    for l in range(n_FGparams.IDlength):
        n_FGparams.FGfile[l] = n_FGparams.ProjectRoot +'Data/' + 'FileGroups/' + n_FGparams.FileGroupID[l]

#only retain these ones. 
RFN_params.n_FileGroupID = n_FGparams.FileGroupID
RFN_params.n_FGfile = n_FGparams.FGfile
RFN_params.n_IDlength = n_FGparams.IDlength

#don't think I need this, at least not right now? 
#n_EDparams = ED(MasterINI,'n_EventDetector',ParamsRoot).getParams()
#n_FEparams = ED(MasterINI,'n_EventDetector',ParamsRoot).getParams()

class runFullNovel(ApplyModel,Comb4FeatureTrain,TrainModel,ApplyCutoff):

    n_FGfile = luigi.Parameter()
    n_FileGroupID = luigi.Parameter()
    n_IDlength= luigi.IntParameter()

    #nullify some inherited parameters:
    PE2datType=None

    upstream_task1=None
    upstream_task2=None
    upstream_task3=None
    uTask1path=None
    uTask2path=None
    uTask3path=None

    def pipelineMap(self,l):
            task0 = Comb4FeatureTrain.invoke(self)
            task1 = TrainModel.invoke(self,task0)
            
            task2 = FormatFG.invoke(self,n="l",src="n_") 
            task3 = UnifyED.invoke(self,task2)
            task4 = UnifyFE.invoke(self,task3,task2)
            task5 = ApplyModel.invoke(self,task4,task1,task2)
            task6 = ApplyCutoff.invoke(self,task5)

            return task6
    def requires(self):
        for l in range(self.n_IDlength):
            yield self.pipelineMap(l)
    def output(self):
        return None
    def run(self):

        return None
    def invoke(obj):
        return(runFullNovel(JobName=obj.JobName,ProjectRoot=obj.ProjectRoot,SoundFileRootDir_Host_Raw=obj.SoundFileRootDir_Host_Raw,\
                            IDlength=obj.IDlength,FGfile=obj.FGfile,FileGroupID=obj.FileGroupID,SoundFileRootDir_Host_Dec=obj.SoundFileRootDir_Host_Dec,\
                            GTfile=obj.GTfile,EDprocess=obj.EDprocess,EDsplits=obj.EDsplits,EDcpu=obj.EDcpu,\
                            EDchunk=obj.EDchunk,EDmethodID=obj.EDmethodID,EDparamString=obj.EDparamString,\
                            EDparamNames=obj.EDparamNames,ALprocess=obj.ALprocess,ALmethodID=obj.ALmethodID,\
                            ALparamString=obj.ALparamString,FEprocess=obj.FEprocess,FEmethodID=obj.FEmethodID,\
                            FEparamString=obj.FEparamString,FEparamNames=obj.FEparamNames,FEsplits=obj.FEsplits,\
                            FEcpu=obj.FEcpu,MFAprocess=obj.MFAprocess,MFAmethodID=obj.MFAmethodID,decimatedata = obj.decimatedata,\
                            TMprocess=obj.TMprocess,TMmethodID=obj.TMmethodID,TMparamString=obj.TMparamString,TMstage=obj.TMstage,\
                            TM_outName=obj.TM_outName,TMcpu=obj.TMcpu,ACcutoffString=obj.ACcutoffString,n_FileGroupID=obj.n_FileGroupID,\
                            n_IDlength=obj.n_IDlength,n_FGfile=obj.n_FGfile,system=obj.system,r_version=obj.r_version))


if __name__ == '__main__':
    luigi.build([runFullNovel.invoke(RFN_params)], local_scheduler=True)

