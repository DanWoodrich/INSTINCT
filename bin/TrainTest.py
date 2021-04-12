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
from runFullNovel import *
from getParams import *

#Run a model on data that does not need to have labels. 

TT_params = Load_Job('TrainTest')

TT_params = FG(TT_params,'FormatFG')
TT_params = GT(TT_params,'FormatGT')
TT_params = ED(TT_params,'EventDetector')
TT_params = FE(TT_params,'FeatureExtraction')
TT_params = AL(TT_params,'AssignLabels')
TT_params = MFA(TT_params,'MergeFE_AL')
TT_params = TM(TT_params,'TrainModel','train')
TT_params = AC(TT_params,'ApplyCutoff')
TT_params = PE1(TT_params,'PerfEval1')
TT_params = PE2(TT_params,'PerfEval2')


#novel data params

#FG for novel data
n_TT_params = TT('TrainTest')
n_FGparams = FG(n_TT_params,'FormatFGapply')

n_FGparams = GT(n_FGparams,'FormatGTapply')

#only retain these ones. 
TT_params.n_FileGroupID = n_FGparams.FileGroupID
TT_params.n_FGfile = n_FGparams.FGfile
TT_params.n_IDlength = n_FGparams.IDlength
TT_params.n_GTfile = n_FGparams.GTfile

class TrainTest(runFullNovel,PerfEval1,PerfEval2):

    JobName=luigi.Parameter()
    n_GTfile = luigi.Parameter()
    #nullify some inherited parameters:

    def pipelineMap(self):
            task0 = TrainModel.invoke(self)
            task1 = FormatFG(FGfile = self.n_FGfile[0],ProjectRoot=self.ProjectRoot)
            task2 = UnifyED.invoke(self,task1)
            task3 = UnifyFE.invoke(self,task2,task1)
            task4 = ApplyModel.invoke(self,task3,task0,task1)
            task5 = ApplyCutoff.invoke(self,task4)
            task6 = FormatGT(upstream_task1=task1,uTask1path=task1.outpath(),GTfile=self.n_GTfile[0],ProjectRoot=self.ProjectRoot) #I should remove uTaskpath param before too long
            task7 = AssignLabels.invoke(self,task5,task6,task1)
            task8 = PerfEval1.invoke(self,task7,task1,n=0)
            task9 = PerfEval2.invoke(self,task0,task8,"FG") #this will not work currently, for a couple
            #reasons. Assign labels incorrectly drops the prob column (have it retain by default).
            #the name of the read file is also different. I should rename all detection files to be
            #detx.csv.gz, and make R methods robust to unnecessary columns.

            #Next to do: fix the R methods, think about splitting up TM and EDPE1 run methods into a process
            #and 1. keeping their merge function (merge for EDPE1, merge for TM, merge for TT)?
            #2. eliminating their pipeline structure entirely?.

            #maybe could have a general merge class which just automerges data?

            return [task0,task1,task2,task3,task4,task5,task6,task7,task8,task9]
    def requires(self):
        tasks=self.pipelineMap()
        yield tasks[1]
        yield tasks[6]
        yield tasks[8]
        yield tasks[9]
    def output(self):
        return None
    def run(self):

        #dummy just to make sure it runs and find output easier
        tasks = self.pipelineMap
        print(tasks[1].outpath())
    def invoke(obj):
        return(TrainTest(JobName=obj.JobName,ProjectRoot=obj.ProjectRoot,SoundFileRootDir_Host=obj.SoundFileRootDir_Host,\
                            IDlength=obj.IDlength,FGfile=obj.FGfile,FileGroupID=obj.FileGroupID,\
                            GTfile=obj.GTfile,EDprocess=obj.EDprocess,EDsplits=obj.EDsplits,EDcpu=obj.EDcpu,\
                            EDchunk=obj.EDchunk,EDmethodID=obj.EDmethodID,EDparamString=obj.EDparamString,\
                            EDparamNames=obj.EDparamNames,ALprocess=obj.ALprocess,ALmethodID=obj.ALmethodID,\
                            ALparamString=obj.ALparamString,FEprocess=obj.FEprocess,FEmethodID=obj.FEmethodID,\
                            FEparamString=obj.FEparamString,FEparamNames=obj.FEparamNames,FEsplits=obj.FEsplits,\
                            FEcpu=obj.FEcpu,MFAprocess=obj.MFAprocess,MFAmethodID=obj.MFAmethodID,\
                            TMprocess=obj.TMprocess,TMmethodID=obj.TMmethodID,TMparamString=obj.TMparamString,TMstage=obj.TMstage,\
                            TM_outName=obj.TM_outName,TMcpu=obj.TMcpu,ACcutoffString=obj.ACcutoffString,n_FileGroupID=obj.n_FileGroupID,\
                            PE1process=obj.PE1process,PE1methodID=obj.PE1methodID,PE2process=obj.PE2process,PE2methodID=obj.PE2methodID,\
                            n_IDlength=obj.n_IDlength,n_FGfile=obj.n_FGfile,n_GTfile=obj.n_GTfile,system=obj.system,r_version=obj.r_version))


if __name__ == '__main__':
    luigi.build([TrainTest.invoke(TT_params)], local_scheduler=True)

