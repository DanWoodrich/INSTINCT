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

ProjectRoot=Helper.getProjRoot()

#####parse config for parameters#######

JobName='runFullNovel'
    
ParamsRoot=ProjectRoot + 'etc/' + JobName + '/'

MasterINI = configparser.ConfigParser()
MasterINI.read(ParamsRoot + 'Master.ini')

system=MasterINI['Global']['system']
r_version=MasterINI['Global']['r_version']

FGparams = FG(MasterINI,'FormatFG',ProjectRoot).getParams()
GTparams = GT(MasterINI,'FormatGT',ProjectRoot,FGparams.FileGroupID).getParams()

EDparams = ED(MasterINI,'EventDetector',ParamsRoot).getParams()

FEparams = FE(MasterINI,'FeatureExtraction',ParamsRoot,EDparams.paramHash).getParams()

ALparams = AL(MasterINI,'AssignLabels',ParamsRoot,EDparams.paramHash,GTparams.paramHash,GTparams.paramHash).getParams()

MFAparams = MFA(MasterINI,'MergeFE_AL',str(FEparams.uTaskpath + '/' + FEparams.paramHash),str(ALparams.uTask1path + '/' + ALparams.paramHash),FEparams.paramHash).getParams()

TMparams = TM(MasterINI,'TrainModel',ParamsRoot,'train').getParams()

TM_processes = [ALparams.paramHash,EDparams.paramHash,FEparams.paramHash,MFAparams.paramHash,TMparams.paramHash] #alphabetical order

TM_JobName='TrainModel'
TM_JobHash = hashJob(FGparams.FileGroupHashes,GTparams.GTHashes,TM_processes)

#novel data params

#FG for novel data
n_FGparams = FG(MasterINI,'FormatFGapply',ProjectRoot).getParams()

#if other args are present, load it in as FGID instead of what is on params.
#note that this is copy pasted from getParams, and is kind of hacky. Potential for conflicts if I'm still using this and change FG()
if len(sys.argv)>1:
    n_FGparams.FileGroupID=sys.argv[1]
    n_FGparams.SoundFileRootDir_Host = getM_Param(n_FGparams,'SoundFileRootDir_Host')
    n_FGparams.FileGroupID = sorted(n_FGparams.FileGroupID.split(','))
    n_FGparams.IDlength = len(n_FGparams.FileGroupID)
    n_FGparams.FGfile = [None] * n_FGparams.IDlength
    n_FGparams.FileGroupHashes = [None] * n_FGparams.IDlength

    for l in range(n_FGparams.IDlength):
        n_FGparams.FGfile[l] = n_FGparams.ProjectRoot +'Data/' + 'FileGroups/' + n_FGparams.FileGroupID[l]
        n_FGparams.FileGroupHashes[l] = Helper.hashfile(n_FGparams.FGfile[l])

n_EDparams = ED(MasterINI,'n_EventDetector',ParamsRoot).getParams()
n_FEparams = ED(MasterINI,'n_EventDetector',ParamsRoot).getParams()

#apply model

APM_Filename = "DETwFeatures.csv.gz"
APMuTask1path = EDparams.paramHash + '/' + FEparams.paramHash 

#apply cutoff

ACparams = AC(MasterINI,'ApplyCutoff',str(APMuTask1path + '/' + TM_JobHash)).getParams() 

#hash the full job
JobHash="placeholder for now" 

class runFullNovel(luigi.Task):

    ProjectRoot=luigi.Parameter()
    system=luigi.Parameter()
    r_version=luigi.Parameter()

    JobName=luigi.Parameter()
    JobHash=luigi.Parameter()
    
    #train model job params: 
    TM_JobName=luigi.Parameter()
    TM_JobHash=luigi.Parameter()
    
    ParamsRoot=luigi.Parameter()

    GTparamsHash =luigi.Parameter()

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
    EDparamsNames = luigi.Parameter()

    FEprocess = luigi.Parameter()
    FEmethodID = luigi.Parameter()
    FEcpu = luigi.Parameter()
    FEsplits = luigi.IntParameter()
    FEparamString =luigi.Parameter()
    FEparamsHash = luigi.Parameter()
    FEparamsNames = luigi.Parameter()
    
    FEuTaskpath = luigi.Parameter()
        
    ALprocess = luigi.Parameter()
    ALmethodID = luigi.Parameter()
    ALparamString=luigi.Parameter()
    ALparamsHash=luigi.Parameter()

    ALuTask1path=luigi.Parameter()
    ALuTask2path=luigi.Parameter()

    MFAprocess = luigi.Parameter()
    MFAmethodID = luigi.Parameter()
    MFAparamsHash = luigi.Parameter()

    MFAuTask1path = luigi.Parameter()
    MFAuTask2path = luigi.Parameter()

    TMprocess = luigi.Parameter()
    TMmethodID = luigi.Parameter()
    TMparams = luigi.Parameter()
    TMstage= luigi.Parameter()
    TM_outName= luigi.Parameter()

    CVcpu= luigi.Parameter()

    #run full params:
    
    n_FGfile = luigi.Parameter()
    n_FileGroupID = luigi.Parameter()
    n_FileGroupHashes = luigi.Parameter()
    n_SoundFileRootDir_Host= luigi.Parameter()
    n_IDlength= luigi.IntParameter()

    n_EDsplits= luigi.IntParameter()
    n_EDcpu= luigi.Parameter()
    n_EDchunk= luigi.Parameter()

    n_FEsplits= luigi.IntParameter()
    n_FEcpu= luigi.Parameter()

    APMuTask1path= luigi.Parameter()
    APM_Filename=luigi.Parameter()

    ACcutoff= luigi.Parameter()
    ACcutoffHash= luigi.Parameter()
    ACuTaskpath= luigi.Parameter()

    def requires(self):
        task1 = TrainModel(JobName=self.TM_JobName,JobHash=self.TM_JobHash,GTparamsHash=self.GTparamsHash,SoundFileRootDir_Host=self.SoundFileRootDir_Host,\
                           IDlength=self.IDlength,GTfile=self.GTfile,FGfile=self.FGfile,FileGroupHashes=self.FileGroupHashes,FileGroupID=self.FileGroupID,EDprocess=self.EDprocess,EDsplits=self.EDsplits,\
                           EDcpu=self.EDcpu,EDchunk=self.EDchunk,EDmethodID=self.EDmethodID,EDparamString=self.EDparamString,EDparamsHash=self.EDparamsHash,EDparamsNames=self.EDparamNames,\
                           ALprocess=self.ALprocess,ALmethodID=self.ALmethodID,ALparamString=self.ALparamString,ALparamsHash=self.ALparamsHash,ALuTask1path=self.ALuTask1path,ALuTask2path=self.ALuTask2path,\
                           FEprocess=self.FEprocess,FEmethodID=self.FEmethodID,FEparamString=self.FEparamString,FEparamsHash=self.FEparamsHash,FEparamsNames=FEparams.paramNames,FEuTaskpath=self.FEuTaskpath,\
                           FEsplits=self.FEsplits,FEcpu=self.FEcpu,MFAprocess=self.MFAprocess,MFAmethodID=self.MFAmethodID,MFAparamsHash=self.MFAparamsHash,\
                           MFAuTask1path=self.MFAuTask1path,MFAuTask2path=self.MFAuTask2path,TMprocess=self.TMprocess,TMmethodID=self.TMmethodID,TMparams=self.TMparams,\
                           stage=self.TMstage,TM_outName=self.TM_outName,CVcpu=self.CVcpu,system=self.system,r_version=self.r_version,ProjectRoot=self.ProjectRoot)
        for l in range(self.n_IDlength):
            task2 = FormatFG(FGfile = self.n_FGfile[l],FGhash = self.n_FileGroupHashes[l],ProjectRoot=self.ProjectRoot)
            task3 = UnifyED(upstream_task = task2,splits = self.n_EDsplits,FGhash=self.n_FileGroupHashes[l],SoundFileRootDir_Host=self.n_SoundFileRootDir_Host,\
                            EDparamsHash=self.EDparamsHash,Params=self.EDparamString,MethodID=self.EDmethodID,ProcessID=self.EDprocess,CPU=self.n_EDcpu,\
                            Chunk=self.n_EDchunk,system=self.system,r_version=self.r_version,ProjectRoot=self.ProjectRoot)
            task4 = UnifyFE(upstream_task = task3,FGhash=self.n_FileGroupHashes[l],uTaskpath=self.FEuTaskpath,FEparamsHash=self.FEparamsHash,MethodID=self.FEmethodID,\
                            ProcessID=self.FEprocess,Params=self.FEparamString,splits=self.n_FEsplits,CPU=self.n_FEcpu,SoundFileRootDir_Host=self.n_SoundFileRootDir_Host,r_version=self.r_version,\
                            ProjectRoot=self.ProjectRoot,system=self.system)
            task5 = ApplyModel(upstream_task1=task4,upstream_task2=task1,uTask1path=self.APMuTask1path,uTask2path=self.TM_JobHash,uTask1FileName=self.APM_Filename,\
                               FGhash=self.n_FileGroupHashes[l],ProcessID=self.TMprocess,MethodID=self.TMmethodID,system=self.system,r_version=self.r_version,ProjectRoot=self.ProjectRoot)
            task6 = ApplyCutoff(upstream_task=task5,uTaskpath=self.ACuTaskpath,FGhash=self.n_FileGroupHashes[l],CutoffHash=self.ACcutoffHash,Cutoff=self.ACcutoff,ProjectRoot=self.ProjectRoot)
            yield task6

    def output(self):
        return None
    def run(self):

        return None


if __name__ == '__main__':
    luigi.build([runFullNovel(JobName=JobName,JobHash=JobHash,ProjectRoot=ProjectRoot,SoundFileRootDir_Host=FGparams.SoundFileRootDir_Host,\
                              IDlength=FGparams.IDlength,FGfile=FGparams.FGfile,FileGroupHashes=FGparams.FileGroupHashes,FileGroupID=FGparams.FileGroupID,\
                              GTfile=GTparams.GTfile,GTparamsHash=GTparams.paramHash,EDprocess=EDparams.process,\
                              EDsplits=EDparams.Splits,EDcpu=EDparams.CPUNeed,EDchunk=EDparams.sf_chunk_size,EDmethodID=EDparams.methodID,\
                              EDparamString=EDparams.paramString,EDparamsHash=EDparams.paramHash,EDparamsNames=EDparams.paramNames,ALprocess=ALparams.process,ALmethodID=ALparams.methodID,\
                              ALparamString=ALparams.paramString,ALparamsHash=ALparams.paramHash,ALuTask1path=ALparams.uTask1path,ALuTask2path=ALparams.uTask2path,\
                              FEprocess=FEparams.process,FEmethodID=FEparams.methodID,FEparamString=FEparams.paramString,FEparamsHash=FEparams.paramHash,FEparamsNames=FEparams.paramNames\
                              FEuTaskpath=FEparams.uTaskpath,FEsplits=FEparams.Splits,FEcpu=FEparams.CPUNeed,MFAprocess=MFAparams.process,\
                              MFAmethodID=MFAparams.methodID,MFAparamsHash=MFAparams.paramHash,MFAuTask1path=MFAparams.uTask1path,MFAuTask2path=MFAparams.uTask2path,\
                              TM_JobName=TM_JobName,TM_JobHash=TM_JobHash,TMprocess=TMparams.process,TMmethodID=TMparams.methodID,TMparams=TMparams.paramString,\
                              TMstage=TMparams.stage,TM_outName=TMparams.outName,CVcpu=TMparams.CPUNeed,APM_Filename=APM_Filename,APMuTask1path=APMuTask1path,\
                              ACcutoff=ACparams.cutoff,ACcutoffHash=ACparams.paramHash,ACuTaskpath=ACparams.uTaskpath,n_SoundFileRootDir_Host=n_FGparams.SoundFileRootDir_Host,\
                              n_FileGroupID=n_FGparams.FileGroupID,n_IDlength=n_FGparams.IDlength,n_FileGroupHashes=n_FGparams.FileGroupHashes,n_FGfile=n_FGparams.FGfile,\
                              n_EDsplits=n_EDparams.Splits,n_EDcpu=n_EDparams.CPUNeed,n_EDchunk=n_EDparams.sf_chunk_size,n_FEsplits=n_FEparams.Splits,n_FEcpu=n_FEparams.CPUNeed,\
                              system=system,r_version=r_version,ParamsRoot=ParamsRoot)], local_scheduler=True)

