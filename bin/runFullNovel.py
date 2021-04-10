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
from getParams import *

#Run a model on data that does not need to have labels. 

RFN_params = RFN('runFullNovel')

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
n_RFN_params = RFN('runFullNovel')
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

class runFullNovel(TrainModel,ApplyModel,ApplyCutoff):

    RFN_JobName=luigi.Parameter()
    n_FGfile = luigi.Parameter()
    n_FileGroupID = luigi.Parameter()
    n_IDlength= luigi.IntParameter()

    #nullify some inherited parameters:
    PE2datType=None
    TM_JobName=None

    def pipelineMap(self,l):
            task0 = TrainModel.invoke(self)
            task1 = FormatFG(FGfile = self.n_FGfile[l],ProjectRoot=self.ProjectRoot) #long form here: do I need return
            task2 = UnifyED.invoke(self,task1)
            task3 = UnifyFE.invoke(self,task2,task1)
            task4 = ApplyModel.invoke(self,task3,task0,task1)
            task5 = ApplyCutoff.invoke(self,task4)

            return [task0,task1,task2,task3,task4,task5]
    def requires(self):
        for l in range(self.n_IDlength):
            tasks = self.pipelineMap(l)
            yield tasks[5] 
    def output(self):
        return None
    def run(self):

        return None
    def invoke(obj):
        return(runFullNovel(RFN_JobName=obj.RFN_JobName,ProjectRoot=obj.ProjectRoot,SoundFileRootDir_Host=obj.SoundFileRootDir_Host,\
                            IDlength=obj.IDlength,FGfile=obj.FGfile,FileGroupID=obj.FileGroupID,\
                            GTfile=obj.GTfile,EDprocess=obj.EDprocess,EDsplits=obj.EDsplits,EDcpu=obj.EDcpu,\
                            EDchunk=obj.EDchunk,EDmethodID=obj.EDmethodID,EDparamString=obj.EDparamString,\
                            EDparamNames=obj.EDparamNames,ALprocess=obj.ALprocess,ALmethodID=obj.ALmethodID,\
                            ALparamString=obj.ALparamString,FEprocess=obj.FEprocess,FEmethodID=obj.FEmethodID,\
                            FEparamString=obj.FEparamString,FEparamNames=obj.FEparamNames,FEsplits=obj.FEsplits,\
                            FEcpu=obj.FEcpu,MFAprocess=obj.MFAprocess,MFAmethodID=obj.MFAmethodID,\
                            TMprocess=obj.TMprocess,TMmethodID=obj.TMmethodID,TMparamString=obj.TMparamString,TMstage=obj.TMstage,\
                            TM_outName=obj.TM_outName,TMcpu=obj.TMcpu,ACcutoffString=obj.ACcutoffString,n_FileGroupID=obj.n_FileGroupID,\
                            n_IDlength=obj.n_IDlength,n_FGfile=obj.n_FGfile,system=obj.system,r_version=obj.r_version))


if __name__ == '__main__':
    luigi.build([runFullNovel.invoke(RFN_params)], local_scheduler=True)

