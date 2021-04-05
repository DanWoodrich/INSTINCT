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

#Performance evaluation from CV models,
#run PE2 stage 1 from instinct.py, and run PE2 stage 2 from def run(self):.
#home/daniel.woodrich/Projects/instinct_dt/
#C:/Apps/instinct_dt

ProjectRoot=Helper.getProjRoot()

#####parse config for parameters#######

MPE_JobName='ModelPerfEval'
    
ParamsRoot=ProjectRoot + 'etc/' + MPE_JobName + '/'

MasterINI = configparser.ConfigParser()
MasterINI.read(ParamsRoot + 'Master.ini')

#get global params
system=MasterINI['Global']['system']
r_version=MasterINI['Global']['r_version']

FGparams = FG(MasterINI,'FormatFG',ProjectRoot).getParams()
GTparams = GT(MasterINI,'FormatGT',ProjectRoot,FGparams.FileGroupID).getParams()

EDparams = ED(MasterINI,'EventDetector',ParamsRoot).getParams()

#includes a target hash to the upstream 
FEparams = FE(MasterINI,'FeatureExtraction',ParamsRoot,EDparams.paramHash).getParams()

#includes two target hashes to the upstream, 1st is were branch goes and 2nd is represented in hash 
ALparams = AL(MasterINI,'AssignLabels',ParamsRoot,EDparams.paramHash,GTparams.paramHash,GTparams.paramHash).getParams()

PE1params = PE1(MasterINI,'PerfEval1',str(ALparams.uTask1path + '/' + ALparams.paramHash)).getParams()

#includes two target hashes to the upstream, 1st represented in hash, 2nd is where branch continues. 
MFAparams = MFA(MasterINI,'MergeFE_AL',str(FEparams.uTaskpath + '/' + FEparams.paramHash),str(ALparams.uTask1path + '/' + ALparams.paramHash),FEparams.paramHash).getParams()

TMparams = TM(MasterINI,'TrainModel',ParamsRoot,'CV').getParams()

#other params not loaded from .ini to specify

#EDpe1 params
EDpe1_WriteToOutputs='n'
EDpe1_JobName='EDperfeval'

EDpe1_processes = [ALparams.paramHash,EDparams.paramHash,PE1params.paramHash] #alphabetical order

EDpe1_JobHash =hashJob(FGparams.FileGroupHashes,GTparams.GTHashes,EDpe1_processes)

#TM params
TM_processes = [ALparams.paramHash,EDparams.paramHash,FEparams.paramHash,MFAparams.paramHash,TMparams.paramHash] #alphabetical order

TM_JobName='TrainModel'
TM_JobHash = hashJob(FGparams.FileGroupHashes,GTparams.GTHashes,TM_processes)

#PE2 works off of two jobs 

PE2params = PE2(MasterINI,'PerfEval2','All',TM_JobHash,EDpe1_JobHash,EDpe1_JobHash).getParams()

#split FE: branch off of ALparams hash

#very minor process w no method or params, don't use getParams. 
SFPEspecialPath = EDparams.paramHash + '/' + ALparams.paramHash

PE2_params_Split = PE2(MasterINI,'PerfEval2','FG',str(SFPEspecialPath + '/' + TM_JobHash),EDpe1_JobHash,EDpe1_JobHash).getParams() 

#apply cutoff
ACparams = AC(MasterINI,'ApplyCutoff',PE2_params_Split.uTask1path).getParams() 

#AL_AM: uses same params as 1st invocation
AL_AMparams = AL(MasterINI,'AssignLabels',ParamsRoot,str(ACparams.uTaskpath + '/' + ACparams.paramHash),GTparams.paramHash,GTparams.paramHash).getParams()

#PE1_AM: uses same params as 1st invocation
PE1_AMparams = PE1(MasterINI,'PerfEval1',str(ACparams.uTaskpath + '/' + ACparams.paramHash)).getParams()

#preformance report
PRparams = PR(MasterINI,'PerformanceReport').getParams() #assumes a lot of different paths, so not a modular task

MPE_processes = [ACparams.paramHash, ALparams.paramHash,EDparams.paramHash,FEparams.paramHash,MFAparams.paramHash,PE1params.paramHash,PE2params.paramHash,\
                 PRparams.paramHash,TMparams.paramHash] #alphabetical order
MPE_JobHash = hashJob(FGparams.FileGroupHashes,GTparams.GTHashes,MPE_processes)

MPE_WriteToOutputs = 'y'

class ModelPerfEval(EDperfEval,TrainModel,SplitForPE,ApplyCutoff,PerfEval2):
    
    #macro job
    MPE_JobName=luigi.Parameter()
    MPE_JobHash=luigi.Parameter()
    MPE_WriteToOutputs = luigi.Parameter()

    IDlength = luigi.IntParameter()

    #PR
    PRprocess=luigi.Parameter()
    PRmethodID=luigi.Parameter()
    PRparamsHash=luigi.Parameter()

    #nullify some inherited parameters:
    PE2datType=None

    def outpath(self):
        if self.MPE_WriteToOutputs=='y':
            return self.ProjectRoot +'Outputs/' + self.MPE_JobName + '/' + self.MPE_JobHash 
        elif self.MPE_WriteToOutputs=='n':
            return self.ProjectRoot + 'Cache/' + self.MPE_JobHash 
    def requires(self):
        task1 = EDperfEval.invoke(self)
        task2 = TrainModel.invoke(self)
        task3 = PerfEval2.invoke(self,task2,task1,"All")
        for l in range(self.IDlength):
            
            task4 = SplitForPE.invoke(self,task2,l) 
            task5 = PerfEval2.invoke(self,task4,task1,"FG",l)
            task6 = ApplyCutoff.invoke(self,task4,l)
            task7 = FormatGT.invoke(self,l)
            task8 = AssignLabels.invoke(self,task6,task7,ALstageDef='2',n=l)
            task9 = PerfEval1.invoke(self,task8,PE1ContPathDef='n',n=l)

            yield task9
    def output(self):
        #this is full performance report
        #return luigi.LocalTarget(OutputsRoot + self.JobName + '/' + self.JobHash + '/RFmodel.rds')
        return luigi.LocalTarget(self.outpath() + '/FullStats.csv')
    def run(self):
        #this is a mega job. Do PervEval1 stage 2 here (similar to run section of EDperfeval). Collect all artifacts and summarize into report: for now, this is going to be a static printout from R.
        #Don't worry about adding map yet, but when want to do that should draw lat long from FG.
        #if really tricking this section out, keep in python and publish web dashboard with python dash module. 
        
         #concatenate outputs and summarize

        dataframes = [None] * self.IDlength
        for k in range(self.IDlength):
            dataframes[k] = pd.read_csv(self.ProjectRoot + 'Cache/' + self.FileGroupHashes[k] + '/' + self.EDparamsHash + '/' + self.ALparamsHash + '/' +\
                                        self.TM_JobHash + '/' + self.ACcutoffHash + '/Stats.csv.gz',compression='gzip')
        Modeleval = pd.concat(dataframes,ignore_index=True)

        #trying to get resultPath formated like normal, messed up paths a little bit need to fix!!! Pick up here. 
        resultPath= self.outpath() 
        if not os.path.exists(resultPath):
            os.mkdir(resultPath)

        Modeleval.to_csv(resultPath + '/Stats_Intermediate.csv',index=False)
        #send back in to PE1

        FGpath = 'NULL'
        LABpath = 'NULL'
        INTpath = resultPath + '/Stats_Intermediate.csv'
        resultPath2 =  resultPath + '/Stats.csv.gz'
        FGID = 'NULL'

        Paths = [FGpath,LABpath,INTpath,resultPath2]
        Args = [FGID,'2']
        Params = ''

        argParse.run(Program='R',rVers=self.r_version,cmdType=self.system,ProjectRoot=self.ProjectRoot,ProcessID=self.PE1process,MethodID=self.PE1methodID,Paths=Paths,Args=Args,Params=Params)

        os.remove(resultPath + '/Stats_Intermediate.csv')

        #now send the paths for all of the artifacts into the performance report R script.

        #list of paths to collect:
            #for each FG (n paths, comma seperated and parse in R) 
                #PRcurve
                #PRauc
        #EDperfeval
        #Modelperfeval
        #full model PR curve
        #full model AUC

        EDstatPath= self.ProjectRoot + 'Cache/' + self.EDpe1_JobHash
        MDstatPath= self.ProjectRoot + 'Cache/' + self.MPE_JobHash
        MDvisPath= self.ProjectRoot + 'Cache/' + self.TM_JobHash + '/' + self.PE2paramsHash
        
        FGvis_paths = [None] * self.IDlength
        for k in range(self.IDlength):
            FGvis_paths[k] = self.ProjectRoot + 'Cache/' + self.FileGroupHashes[k] + '/' + self.EDparamsHash + '/' + self.ALparamsHash + '/' + self.TM_JobHash + '/' + self.PE2paramsHash
        FGvis_paths = ','.join(FGvis_paths)
        FGIDs=','.join(self.FileGroupID)
        
        if not os.path.exists(resultPath):
            os.mkdir(resultPath)

        Paths = [EDstatPath,MDstatPath,MDvisPath,resultPath]
        Args = [FGvis_paths,FGIDs]

        argParse.run(Program='R',rVers=self.r_version,cmdType=self.system,ProjectRoot=self.ProjectRoot,ProcessID=self.PRprocess,MethodID=self.PRmethodID,Paths=Paths,Args=Args,Params='')
        
if __name__ == '__main__':
    luigi.build([ModelPerfEval(MPE_JobName=MPE_JobName,MPE_JobHash=MPE_JobHash,MPE_WriteToOutputs=MPE_WriteToOutputs,SoundFileRootDir_Host=FGparams.SoundFileRootDir_Host,\
                               IDlength=FGparams.IDlength,FGfile=FGparams.FGfile,FileGroupHashes=FGparams.FileGroupHashes,FileGroupID=FGparams.FileGroupID,\
                               GTfile=GTparams.GTfile,GTparamsHash=GTparams.paramHash,EDprocess=EDparams.process,\
                               EDsplits=EDparams.Splits,EDcpu=EDparams.CPUNeed,EDchunk=EDparams.sf_chunk_size,EDmethodID=EDparams.methodID,\
                               EDparamString=EDparams.paramString,EDparamsHash=EDparams.paramHash,EDparamsNames=EDparams.paramNames,ALprocess=ALparams.process,ALmethodID=ALparams.methodID,\
                               ALparamString=ALparams.paramString,ALparamsHash=ALparams.paramHash,\
                               FEprocess=FEparams.process,FEmethodID=FEparams.methodID,FEparamString=FEparams.paramString,FEparamsHash=FEparams.paramHash,FEparamsNames=FEparams.paramNames,\
                               FEsplits=FEparams.Splits,FEcpu=FEparams.CPUNeed,MFAprocess=MFAparams.process,\
                               MFAmethodID=MFAparams.methodID,MFAparamsHash=MFAparams.paramHash,SFPEspecialPath=SFPEspecialPath,\
                               TM_JobName=TM_JobName,TM_JobHash=TM_JobHash,TMprocess=TMparams.process,TMmethodID=TMparams.methodID,TMparams=TMparams.paramString,\
                               TMstage=TMparams.stage,TM_outName=TMparams.outName,TMcpu=TMparams.CPUNeed,EDpe1_WriteToOutputs=EDpe1_WriteToOutputs,\
                               EDpe1_JobName=EDpe1_JobName,EDpe1_JobHash=EDpe1_JobHash,PE1process=PE1params.process,PE1methodID=PE1params.methodID,\
                               PE1paramsHash=PE1params.paramHash,PE2process=PE2params.process,PE2methodID=PE2params.methodID,\
                               PE2paramsHash=PE2params.paramHash,ACcutoffString=ACparams.cutoff,ACcutoffHash=ACparams.paramHash,\
                               PRprocess=PRparams.process,PRmethodID=PRparams.methodID,PRparamsHash=PRparams.paramHash,\
                               ProjectRoot=ProjectRoot,system=system,r_version=r_version)], local_scheduler=True)    

