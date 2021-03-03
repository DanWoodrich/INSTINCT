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

JobName='ModelPerfEval'
    
ParamsRoot=ProjectRoot + 'etc/' + JobName + '/'

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

PE2params = PE2(MasterINI,'PerfEval2','Job',TM_JobHash,EDpe1_JobHash,EDpe1_JobHash).getParams()

#split FE: branch off of ALparams hash

#very minor process w no method or params, don't use getParams. 
SFPuTask2path = EDparams.paramHash + '/' + ALparams.paramHash

PE2_params_Split = PE2(MasterINI,'PerfEval2','FG',str(SFPuTask2path + '/' + TM_JobHash),EDpe1_JobHash,EDpe1_JobHash).getParams() 

#apply cutoff
ACparams = AC(MasterINI,'ApplyCutoff',PE2_params_Split.uTask1path).getParams() 

#AL_AM: uses same params as 1st invocation
AL_AMparams = AL(MasterINI,'AssignLabels',ParamsRoot,str(ACparams.uTaskpath + '/' + ACparams.paramHash),GTparams.paramHash,GTparams.paramHash).getParams()

#PE1_AM: uses same params as 1st invocation
PE1_AMparams = PE1(MasterINI,'PerfEval1',str(ACparams.uTaskpath + '/' + ACparams.paramHash)).getParams()

#preformance report
PRparams = PR(MasterINI,'PerformanceReport').getParams() #assumes a lot of different paths, so not a modular task

MPE_processes = [ACparams.paramHash, ALparams.paramHash,EDparams.paramHash,FEparams.paramHash,MFAparams.paramHash,PE1params.paramHash,PE2params.paramHash,PRparams.paramHash,TMparams.paramHash] #alphabetical order
JobHash = hashJob(FGparams.FileGroupHashes,GTparams.GTHashes,MPE_processes)

WriteToOutputs = 'y'

class ModelPerfEval(luigi.Task):

    ProjectRoot=luigi.Parameter()
    system=luigi.Parameter()
    r_version=luigi.Parameter()
    
    #macro job
    JobName=luigi.Parameter()
    JobHash=luigi.Parameter()
    
    GTparamsHash =luigi.Parameter()
    GT_signal_code = luigi.Parameter()

    SoundFileRootDir_Host = luigi.Parameter()

    IDlength = luigi.IntParameter()
    GTfile = luigi.Parameter()
    FGfile = luigi.Parameter()
    FileGroupHashes = luigi.Parameter()
    FileGroupID = luigi.Parameter()

    EDprocess = luigi.Parameter()
    EDsplits = luigi.IntParameter()
    EDcpu = luigi.Parameter()
    EDchunk = luigi.Parameter()
    EDmethodID = luigi.Parameter()
    EDparamString=luigi.Parameter()
    EDparamsHash=luigi.Parameter()

    FEprocess = luigi.Parameter()
    FEmethodID = luigi.Parameter()
    FEcpu = luigi.Parameter()
    FEsplits = luigi.IntParameter()
    FEparamString =luigi.Parameter()
    FEparamsHash = luigi.Parameter()

    FEuTaskpath = luigi.Parameter()
        
    ALprocess = luigi.Parameter()
    ALmethodID = luigi.Parameter()
    ALparamString=luigi.Parameter()
    ALparamsHash=luigi.Parameter()

    ALuTask1path=luigi.Parameter()
    ALuTask2path=luigi.Parameter()


    PE1process = luigi.Parameter()
    PE1methodID = luigi.Parameter()
    PE1paramsHash= luigi.Parameter()

    PE1uTaskpath = luigi.Parameter()

    PE1ContPath='y'

    MFAprocess = luigi.Parameter()
    MFAmethodID = luigi.Parameter()
    MFAparamsHash = luigi.Parameter()

    MFAuTask1path = luigi.Parameter()
    MFAuTask2path = luigi.Parameter()
    
    #EDpe1 job 
    EDpe1_JobName=luigi.Parameter()
    EDpe1_JobHash=luigi.Parameter()
    EDpe1_WriteToOutputs=luigi.Parameter()

    #TM job
    TM_JobName=luigi.Parameter()
    TM_JobHash=luigi.Parameter()

    TMprocess = luigi.Parameter()
    TMmethodID = luigi.Parameter()
    TMparams = luigi.Parameter()

    TMstage=luigi.Parameter()
    TM_outName=luigi.Parameter()
    CVcpu=luigi.Parameter()

    PE2process = luigi.Parameter()
    PE2methodID = luigi.Parameter()
    PE2paramsHash= luigi.Parameter()

    #pre split
    PE2uTask1path = luigi.Parameter() #DETwProbs
    PE2uTask2path = luigi.Parameter() #Stats.csv

    PE2rp= luigi.Parameter()

    SFPuTask2path= luigi.Parameter() #

    #post split
    PE2uTask1pathSplit=luigi.Parameter()
    PE2rpSplit= luigi.Parameter()

    #AC
    ACcutoffString=luigi.Parameter()
    ACcutoffHash=luigi.Parameter()

    #AL_AM

    AL_AMuTask1path =  luigi.Parameter()
    AL_AMuTask2path = luigi.Parameter()
    AL_AMstage = '2'

    #PE1 AM 
    PE1uTaskpathAM =luigi.Parameter()
    PE1_AMContPath = 'n'
    PE1_AMstage = '2'

    #PR
    PRprocess=luigi.Parameter()
    PRmethodID=luigi.Parameter()
    PRparamsHash=luigi.Parameter()

    WriteToOutputs = luigi.Parameter()
    def rootpath(self):
        if self.WriteToOutputs=='y':
            return self.ProjectRoot +'Outputs/' + self.JobName + '/'
        elif self.WriteToOutputs=='n':
            return self.ProjectRoot + 'Cache/'
    def requires(self):
        task1=EDperfeval(JobName=self.EDpe1_JobName,JobHash=self.EDpe1_JobHash,GTparamsHash=self.GTparamsHash,SoundFileRootDir_Host=self.SoundFileRootDir_Host,IDlength=self.IDlength,\
                   GTfile=self.GTfile,FGfile=self.FGfile,FileGroupHashes=self.FileGroupHashes,FileGroupID=self.FileGroupID,EDprocess=self.EDprocess,EDsplits=self.EDsplits,EDcpu=self.EDcpu,\
                   EDchunk=self.EDchunk,EDmethodID=self.EDmethodID,EDparamString=self.EDparamString,EDparamsHash=self.EDparamsHash,ALprocess=self.ALprocess,\
                   ALmethodID=self.ALmethodID,ALparamString=self.ALparamString,ALparamsHash=self.ALparamsHash,ALuTask1path=self.ALuTask1path,ALuTask2path=self.ALuTask2path,\
                   PE1ContPath=self.PE1ContPath,PE1process=self.PE1process,PE1methodID=self.PE1methodID,PE1paramsHash=self.PE1paramsHash,PE1uTaskpath=self.PE1uTaskpath,WriteToOutputs=self.EDpe1_WriteToOutputs,\
                   ProjectRoot=self.ProjectRoot,system=self.system,r_version=self.r_version)
        yield(task1)
        task2=TrainModel(JobName=self.TM_JobName,JobHash=self.TM_JobHash,GTparamsHash=self.GTparamsHash,SoundFileRootDir_Host=self.SoundFileRootDir_Host,\
                           IDlength=self.IDlength,GTfile=self.GTfile,FGfile=self.FGfile,FileGroupHashes=self.FileGroupHashes,FileGroupID=self.FileGroupID,EDprocess=self.EDprocess,EDsplits=self.EDsplits,\
                           EDcpu=self.EDcpu,EDchunk=self.EDchunk,EDmethodID=self.EDmethodID,EDparamString=self.EDparamString,EDparamsHash=self.EDparamsHash,ALprocess=self.ALprocess,\
                           ALmethodID=self.ALmethodID,ALparamString=self.ALparamString,ALparamsHash=self.ALparamsHash,ALuTask1path=self.ALuTask1path,ALuTask2path=self.ALuTask2path,\
                           FEprocess=self.FEprocess,FEmethodID=self.FEmethodID,FEparamString=self.FEparamString,FEparamsHash=self.FEparamsHash,FEuTaskpath=self.FEuTaskpath,\
                           FEsplits=self.FEsplits,FEcpu=self.FEcpu,MFAprocess=self.MFAprocess,MFAmethodID=self.MFAmethodID,MFAparamsHash=self.MFAparamsHash,\
                           MFAuTask1path=self.MFAuTask1path,MFAuTask2path=self.MFAuTask2path,TMprocess=self.TMprocess,TMmethodID=self.TMmethodID,TMparams=self.TMparams,\
                           stage=self.TMstage,TM_outName=self.TM_outName,CVcpu=self.CVcpu,system=self.system,ProjectRoot=self.ProjectRoot,r_version=self.r_version)
        task3 = PerfEval2(upstream_task1=task2,upstream_task2=task1,uTask1path=self.PE2uTask1path,uTask2path=self.PE2uTask2path,ProcessID=self.PE2process,MethodID=self.PE2methodID,\
                          PE2paramsHash=self.PE2paramsHash,rootPath=self.PE2rp,FGhash=None,system=self.system,ProjectRoot=self.ProjectRoot,r_version=self.r_version)
        yield(task3)
        for l in range(self.IDlength):
            task4 = SplitForPE(upstream_task1=task2,uTask1path=self.TM_JobHash,uTask2path=self.SFPuTask2path,FileGroupID=self.FileGroupID[l],FGhash=self.FileGroupHashes[l],ProjectRoot=self.ProjectRoot) #task which splits the combined data based on file IDs back into FG. 
            task5 = PerfEval2(upstream_task1=task4,upstream_task2=task1,uTask1path=self.PE2uTask1pathSplit,uTask2path=self.PE2uTask2path,ProcessID=self.PE2process,MethodID=self.PE2methodID,\
                          PE2paramsHash=self.PE2paramsHash,rootPath=self.PE2rpSplit,FGhash=self.FileGroupHashes[l],system=self.system,ProjectRoot=self.ProjectRoot,r_version=self.r_version)
            yield(task5)
            task6 = ApplyCutoff(upstream_task=task4,uTaskpath=self.PE2uTask1pathSplit,FGhash=self.FileGroupHashes[l],CutoffHash=self.ACcutoffHash,Cutoff=self.ACcutoffString,ProjectRoot=self.ProjectRoot)
            task7 = FormatGT(GTfile=self.GTfile[l],FGhash = self.FileGroupHashes[l],FGfile = self.FGfile[l],GThash=self.GTparamsHash,ProjectRoot=self.ProjectRoot)
            task8 = AssignLabels(upstream_task1=task6,uTask1path=self.AL_AMuTask1path,upstream_task2=task7,uTask2path=self.AL_AMuTask2path,FGhash=self.FileGroupHashes[l],ProcessID=self.ALprocess,\
                   MethodID=self.ALmethodID,Params=self.ALparamString,ALparamsHash=self.ALparamsHash,stage=self.AL_AMstage,system=self.system,ProjectRoot=self.ProjectRoot,r_version=self.r_version)
            task9 = PerfEval1(upstream_task=task8,FGhash=self.FileGroupHashes[l],uTaskpath=self.AL_AMuTask1path, PE1paramsHash=self.PE1paramsHash,\
                               FileGroupID=self.FileGroupID[l],MethodID=self.PE1methodID,ProcessID=self.PE1process,ContPath=self.PE1_AMContPath,system=self.system,ProjectRoot=self.ProjectRoot,r_version=self.r_version)
            yield task9
    def output(self):
        #this is full performance report
        #return luigi.LocalTarget(OutputsRoot + self.JobName + '/' + self.JobHash + '/RFmodel.rds')
        return luigi.LocalTarget(self.rootpath() + '/' + self.JobHash + '/FullStats.csv')
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

        resultPath2= self.ProjectRoot + 'Cache/' + self.JobHash
        if not os.path.exists(resultPath2):
            os.mkdir(resultPath2)

        Modeleval.to_csv(resultPath2 + '/Stats_Intermediate.csv',index=False)
        #send back in to PE1

        FGpath = 'NULL'
        LABpath = 'NULL'
        INTpath = resultPath2 + '/Stats_Intermediate.csv'
        resultPath =  resultPath2 + '/Stats.csv.gz'
        FGID = 'NULL'

        Paths = [FGpath,LABpath,INTpath,resultPath]
        Args = [FGID,self.PE1_AMstage]
        Params = ''

        argParse.run(Program='R',rVers=self.r_version,cmdType=self.system,ProjectRoot=self.ProjectRoot,ProcessID=self.PE1process,MethodID=self.PE1methodID,Paths=Paths,Args=Args,Params=Params)

        os.remove(resultPath2 + '/Stats_Intermediate.csv')

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
        MDstatPath= self.ProjectRoot + 'Cache/' + self.JobHash
        MDvisPath= self.ProjectRoot + 'Cache/' + self.TM_JobHash + '/' + self.PE2paramsHash
        
        FGvis_paths = [None] * self.IDlength
        for k in range(self.IDlength):
            FGvis_paths[k] = self.ProjectRoot + 'Cache/' + self.FileGroupHashes[k] + '/' + self.EDparamsHash + '/' + self.ALparamsHash + '/' + self.TM_JobHash + '/' + self.PE2paramsHash
        FGvis_paths = ','.join(FGvis_paths)
        FGIDs=','.join(self.FileGroupID)
        
        if not os.path.exists(self.rootpath()):
            os.mkdir(self.rootpath())

        resultPath = self.rootpath() + '/' + self.JobHash
        if not os.path.exists(resultPath):
            os.mkdir(resultPath)

        Paths = [EDstatPath,MDstatPath,MDvisPath,resultPath]
        Args = [FGvis_paths,FGIDs]
        Params = ''

        argParse.run(Program='R',rVers=self.r_version,cmdType=self.system,ProjectRoot=self.ProjectRoot,ProcessID=self.PRprocess,MethodID=self.PRmethodID,Paths=Paths,Args=Args,Params=Params)
        
        #Copy params

        
if __name__ == '__main__':
    luigi.build([ModelPerfEval(JobName=JobName,JobHash=JobHash,WriteToOutputs=WriteToOutputs,SoundFileRootDir_Host=FGparams.SoundFileRootDir_Host,\
                               IDlength=FGparams.IDlength,FGfile=FGparams.FGfile,FileGroupHashes=FGparams.FileGroupHashes,FileGroupID=FGparams.FileGroupID,\
                               GTfile=GTparams.GTfile,GTparamsHash=GTparams.paramHash,GT_signal_code=GTparams.GT_signal_code,EDprocess=EDparams.process,\
                               EDsplits=EDparams.Splits,EDcpu=EDparams.CPUNeed,EDchunk=EDparams.sf_chunk_size,EDmethodID=EDparams.methodID,\
                               EDparamString=EDparams.paramString,EDparamsHash=EDparams.paramHash,ALprocess=ALparams.process,ALmethodID=ALparams.methodID,\
                               ALparamString=ALparams.paramString,ALparamsHash=ALparams.paramHash,ALuTask1path=ALparams.uTask1path,ALuTask2path=ALparams.uTask2path,\
                               FEprocess=FEparams.process,FEmethodID=FEparams.methodID,FEparamString=FEparams.paramString,FEparamsHash=FEparams.paramHash,\
                               FEuTaskpath=FEparams.uTaskpath,FEsplits=FEparams.Splits,FEcpu=FEparams.CPUNeed,MFAprocess=MFAparams.process,\
                               MFAmethodID=MFAparams.methodID,MFAparamsHash=MFAparams.paramHash,MFAuTask1path=MFAparams.uTask1path,MFAuTask2path=MFAparams.uTask2path,\
                               TM_JobName=TM_JobName,TM_JobHash=TM_JobHash,TMprocess=TMparams.process,TMmethodID=TMparams.methodID,TMparams=TMparams.paramString,\
                               TMstage=TMparams.stage,TM_outName=TMparams.outName,CVcpu=TMparams.CPUNeed,EDpe1_WriteToOutputs=EDpe1_WriteToOutputs,\
                               EDpe1_JobName=EDpe1_JobName,EDpe1_JobHash=EDpe1_JobHash,PE1process=PE1params.process,PE1methodID=PE1params.methodID,\
                               PE1paramsHash=PE1params.paramHash,PE1uTaskpath=PE1params.uTaskpath,PE2process=PE2params.process,PE2methodID=PE2params.methodID,\
                               PE2paramsHash=PE2params.paramHash,PE2uTask1path=PE2params.uTask1path,PE2uTask2path=PE2params.uTask2path,PE2rp=PE2params.rp,\
                               SFPuTask2path=SFPuTask2path,PE2rpSplit=PE2_params_Split.rp,PE2uTask1pathSplit=PE2_params_Split.uTask1path,ACcutoffString=ACparams.cutoff,\
                               ACcutoffHash=ACparams.paramHash,AL_AMuTask1path=AL_AMparams.uTask1path,AL_AMuTask2path=AL_AMparams.uTask2path,\
                               PE1uTaskpathAM=PE1_AMparams.uTaskpath,PRprocess=PRparams.process,PRmethodID=PRparams.methodID,PRparamsHash=PRparams.paramHash,\
                               ProjectRoot=ProjectRoot,system=system,r_version=r_version)], local_scheduler=True)    

