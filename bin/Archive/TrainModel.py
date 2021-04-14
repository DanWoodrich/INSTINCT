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

#Combine FG data to prepare for model training 
# C4FT = combine for feature train. Used to train RF/SVM models etc, not for DL.
#for DL, would want to load detections w no features extracted. 

#load params

C4FT_params=Load_Job("C4FT")
C4FT_params = FG(C4FT_params,'FormatFG')
C4FT_params = GT(C4FT_params,'FormatGT')
C4FT_params = ED(C4FT_params,'EventDetector')
C4FT_params = FE(C4FT_params,'FeatureExtraction')
C4FT_params = AL(C4FT_params,'AssignLabels')
C4FT_params = MFA(C4FT_params,'MergeFE_AL')


C4FT_params.C4FT_WriteToOutputs='y'

class Comb4FeatureTrain(FormatFG,FormatGT,UnifyED,AssignLabels,UnifyFE,MergeFE_AL):

    JobName=luigi.Parameter()
    IDlength = luigi.IntParameter()
    FileGroupID = luigi.Parameter()

    #TMprocess = luigi.Parameter()
    #TMmethodID = luigi.Parameter()
    #TMparamString = luigi.Parameter()

    #TMstage=luigi.Parameter()
    #TM_outName=luigi.Parameter()

    #TMcpu=luigi.Parameter()

    C4FT_WriteToOutputs=luigi.Parameter()

    #nullify some inherited parameters:
    upstream_task1=None
    upstream_task2=None
    upstream_task3=None
    uTask1path=None
    uTask2path=None
    uTask3path=None

    def pipelineMap(self,l): #here is where you define pipeline structure
        task0 = FormatFG.invoke(self,l) 
        task1 = FormatGT.invoke(self,task0,l)
        task2 = UnifyED.invoke(self,task0)
        task3 = AssignLabels.invoke(self,task2,task1,task0)
        task4 = UnifyFE.invoke(self,task2,task0)
        task5 = MergeFE_AL.invoke(self,task3,task4)
        return [task0,task1,task2,task3,task4,task5]
    def hashProcess(self):
        hashStrings = [None] * self.IDlength
        for l in range(self.IDlength):
            tasks = self.pipelineMap(l)
            hashStrings[l] = ' '.join([tasks[0].hashProcess(),tasks[1].hashProcess(),tasks[2].hashProcess(),tasks[3].hashProcess(),
                                  tasks[4].hashProcess(),tasks[5].hashProcess()])
    
        TM_JobHash = Helper.getParamHash2(self.TMparamString + ' ' +self.TMmethodID + ' ' + ' '.join(hashStrings),12)
        return(TM_JobHash)
        #Method + pipeline  hashes
    def outpath(self):
        if self.TM_WriteToOutputs=='y':
            return self.ProjectRoot +'Outputs/' + self.JobName 
        elif self.TM_WriteToOutputs=='n':
            return self.ProjectRoot + 'Cache/' + self.hashProcess()
    def requires(self):
        for l in range(self.IDlength):
            tasks = self.pipelineMap(l)

            yield tasks[len(tasks)-1]
    def output(self):
        return luigi.LocalTarget(self.outpath() + '/' + self.TM_outName)
    def run(self):

        #concatenate outputs and summarize

        #load in 
        dataframes = [None] * self.IDlength
        FGdf = [None] * self.IDlength
        for k in range(self.IDlength):
            tasks = self.pipelineMap(k)
            
            dataframes[k] = pd.read_csv(tasks[5].outpath()+ '/DETwFEwAL.csv.gz') # 
            dataframes[k]['FGID'] = pd.Series(self.FileGroupID[k], index=dataframes[k].index)
            
            FGdf[k] = pd.read_csv(tasks[0].outpath() + '/FileGroupFormat.csv.gz')
        TMdat = pd.concat(dataframes,ignore_index=True)
        FGdf = pd.concat(FGdf,ignore_index=True)

        resultPath =self.outpath()
        if not os.path.exists(resultPath):
            os.mkdir(resultPath)

        TMdat.to_csv(resultPath + '/TM_Intermediate1.csv.gz',index=False,compression='gzip')
        FGdf.to_csv(resultPath + '/FG_Intermediate2.csv.gz',index=False,compression='gzip')

        #
        TMpath = resultPath + '/TM_Intermediate1.csv.gz'
        FGpath = resultPath + '/FG_Intermediate2.csv.gz'
        Mpath = 'NULL'

        Paths = [TMpath,FGpath,resultPath,Mpath]
        Args = [self.TMstage,self.TMcpu]

        argParse.run(Program='R',rVers=self.r_version,cmdType=self.system,ProjectRoot=self.ProjectRoot,ProcessID=self.TMprocess,MethodID=self.TMmethodID,Paths=Paths,Args=Args,Params=self.TMparamString)

        os.remove(resultPath + '/TM_Intermediate1.csv.gz')
        os.remove(resultPath + '/FG_Intermediate2.csv.gz')

        #copy params to output folder
    def invoke(self):
        return(TrainModel(JobName=self.JobName,SoundFileRootDir_Host=self.SoundFileRootDir_Host,C4FT_WriteToOutputs=self.C4FT_WriteToOutputs,\
                           IDlength=self.IDlength,GTfile=self.GTfile,FGfile=self.FGfile,FileGroupID=self.FileGroupID,EDprocess=self.EDprocess,EDsplits=self.EDsplits,\
                           EDcpu=self.EDcpu,EDchunk=self.EDchunk,EDmethodID=self.EDmethodID,EDparamString=self.EDparamString,EDparamNames=self.EDparamNames,ALprocess=self.ALprocess,\
                           ALmethodID=self.ALmethodID,ALparamString=self.ALparamString,\
                           FEprocess=self.FEprocess,FEmethodID=self.FEmethodID,FEparamString=self.FEparamString,FEparamNames=self.FEparamNames,\
                           FEsplits=self.FEsplits,FEcpu=self.FEcpu,MFAprocess=self.MFAprocess,MFAmethodID=self.MFAmethodID,\
                           TMprocess=self.TMprocess,TMmethodID=self.TMmethodID,TMparamString=self.TMparamString,\
                           TMstage=self.TMstage,TM_outName=self.TM_outName,TMcpu=self.TMcpu,system=self.system,ProjectRoot=self.ProjectRoot,r_version=self.r_version))

if __name__ == '__main__':
    luigi.build([C4FT_params.invoke(params)], local_scheduler=True)


