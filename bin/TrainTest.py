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

#Run a model on data that does not need to have labels. 

ProjectRoot='C:/instinct_dt/' #linux install
#ProjectRoot='C:/test/' #windows

CacheRoot=ProjectRoot + 'Cache/'
DataRoot=ProjectRoot +'Data/'
OutputsRoot=ProjectRoot +'Outputs/'

#####parse config for parameters#######

JobName='TrainTest'
    
ParamsRoot=ProjectRoot + 'etc/' + JobName + '/'

MasterINI = configparser.ConfigParser()
MasterINI.read(ParamsRoot + 'Master.ini')

FileGroupID = MasterINI['FormatFG']['FileGroupID']
GT_signal_code = MasterINI['FormatGT']['GT_signal_code']
GTparams = sorted(MasterINI.items('FormatGT')).__str__()
GTparamsHash= GTparams.encode('utf8')
GTparamsHash = str(hashlib.sha1(GTparamsHash).hexdigest())

SoundFileRootDir_Host = MasterINI['FormatFG']['SoundFileRootDir_Host']

#FG for training data
FileGroupID = sorted(FileGroupID.split(','))
IDlength = len(FileGroupID)
FGfile = [None] * IDlength
GTfile = [None] * IDlength

FileGroupHashes = [None] * IDlength
GTHashes        = [None] * IDlength

for l in range(IDlength):
    GTfile[l] = DataRoot + 'GroundTruth/' + GT_signal_code + '_' +FileGroupID[l]
    FGfile[l] = DataRoot + 'FileGroups/' + FileGroupID[l]
    FileGroupHashes[l] = Helper.hashfile(FGfile[l])
    GTHashes[l] = Helper.hashfile(GTfile[l])

EDprocess = 'EventDetector'
EDmethodID = MasterINI[EDprocess]['MethodID']
EDsplits = int(MasterINI[EDprocess]['Splits'])
EDcpu = MasterINI[EDprocess]['CPUneed']
EDchunk = MasterINI[EDprocess]['sf_chunk_size']
ED_INI = configparser.ConfigParser()
ED_INI.read(ParamsRoot + EDmethodID + '.ini')
EDparamList=sorted(ED_INI.items(EDprocess))
EDparamList = Helper.paramList(EDparamList)
EDparamString =' '.join(EDparamList)+ ' ' +EDmethodID
EDparamsHash= EDparamString.encode('utf8')
EDparamsHash = str(hashlib.sha1(EDparamsHash).hexdigest())

FEprocess = 'FeatureExtraction'
FEmethodID = MasterINI[FEprocess]['MethodID']
FEcpu = MasterINI[FEprocess]['CPUneed']
FEsplits = int(MasterINI[FEprocess]['Splits'])
FE_INI = configparser.ConfigParser()
FE_INI.read(ParamsRoot + FEmethodID + '.ini')
FEparamList=sorted(FE_INI.items(FEprocess))
FEparamList = Helper.paramList(FEparamList)
FEparamString =' '.join(FEparamList)+ ' ' +FEmethodID
FEparamsHash= FEparamString.encode('utf8')
FEparamsHash = str(hashlib.sha1(FEparamsHash).hexdigest())

FE1uTaskpath = EDparamsHash

ALprocess = 'AssignLabels'
ALmethodID = MasterINI[ALprocess]['MethodID']
AL_INI = configparser.ConfigParser()
AL_INI.read(ParamsRoot + ALmethodID +'.ini')
ALparamList = sorted(AL_INI.items(ALprocess))
ALparamList = Helper.paramList(ALparamList)
ALparamString =' '.join(ALparamList)+ ' ' + ALmethodID
ALparamString2hash = ALparamString + GTparamsHash
ALparamsHash= ALparamString2hash.encode('utf8') 
ALparamsHash = str(hashlib.sha1(ALparamsHash).hexdigest())

ALuTask1path = EDparamsHash 
ALuTask2path =GTparamsHash

MFAprocess = 'MergeFE_AL'
MFAmethodID = MasterINI[MFAprocess]['MethodID']
MFAmethodID2hash = MFAmethodID + FEparamsHash
MFAparamsHash= MFAmethodID.encode('utf8')
MFAparamsHash = str(hashlib.sha1(MFAparamsHash).hexdigest())

MFAuTask1path = EDparamsHash + '/' + FEparamsHash
MFAuTask2path = EDparamsHash + '/' + ALparamsHash

TMprocess = 'TrainModel'
TMstage = 'train'
TM_outName='RFmodel.rds' 
TMmethodID = MasterINI[TMprocess]['MethodID']
TM_INI = configparser.ConfigParser()
TM_INI.read(ParamsRoot + TMmethodID +'.ini')
if(TMstage=='CV'):
    TMparamList = sorted(TM_INI.items(TMprocess)+ TM_INI.items(TMstage))
else:
    TMparamList = sorted(TM_INI.items(TMprocess))
TMparamList = Helper.paramList(TMparamList)
TMparamString =' '.join(TMparamList)+ ' ' + TMmethodID
TMparamsHash= TMparamString.encode('utf8') 
TMparamsHash = str(hashlib.sha1(TMparamsHash).hexdigest())

CVcpu=MasterINI[TMprocess]['CPUneed']

#

strings = [None] * IDlength

#need to add GT info in here. Instead of a path, all relevant sections in alphabetical order. 
for k in range(IDlength):
    strings[k]= ALparamsHash + EDparamsHash + FileGroupHashes[k] + GTHashes[k] + MFAparamsHash + TMparamsHash 

#hash of all the parameters
strings = sorted(strings).__str__()
TM_JobHash= strings.encode('utf8')
TM_JobHash = str(hashlib.sha1(TM_JobHash).hexdigest())

TM_JobName='TrainModel'


#novel data params

#FG for novel data
n_FileGroupID = MasterINI['FormatFGapply']['FileGroupID']
n_SoundFileRootDir_Host= MasterINI['FormatFGapply']['SoundFileRootDir_Host']
n_FileGroupID = sorted(n_FileGroupID.split(','))
n_IDlength = len(n_FileGroupID)
n_FGfile = [None] * n_IDlength


n_FileGroupHashes = [None] * n_IDlength

for l in range(n_IDlength):
    n_FGfile[l] = DataRoot + 'FileGroups/' + n_FileGroupID[l]
    n_FileGroupHashes[l] = Helper.hashfile(n_FGfile[l])

n_EDsplits= int(MasterINI[EDprocess]['n_Splits'])
n_EDcpu= int(MasterINI[EDprocess]['n_CPUneed'])
n_EDchunk= MasterINI[EDprocess]['n_sf_chunk_size']

n_FEcpu = MasterINI[FEprocess]['n_CPUneed']
n_FEsplits = int(MasterINI[FEprocess]['n_Splits'])

#apply model

APM_Filename = "DETwFeatures.csv.gz"

APMuTask1path = EDparamsHash + '/' + FEparamsHash 

#apply cutoff

ACcutoff = MasterINI['ApplyCutoff']['cutoff']
ACcutoffString = str(ACcutoff)
ACcutoffString2hash=ACcutoffString.encode('utf8')
ACcutoffHash = str(hashlib.sha1(ACcutoffString2hash).hexdigest())

ACuTaskpath = APMuTask1path + '/' + TM_JobHash

#hash the full job
JobHash="placeholder for now" 

class runFullNovel(luigi.Task):

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

    FEprocess = luigi.Parameter()
    FEmethodID = luigi.Parameter()
    FEcpu = luigi.Parameter()
    FEsplits = luigi.IntParameter()
    FEparamString =luigi.Parameter()
    FEparamsHash = luigi.Parameter()

    FE1uTaskpath = luigi.Parameter()
        
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

    n_EDsplits= luigi.Parameter()
    n_EDcpu= luigi.Parameter()
    n_EDchunk= luigi.Parameter()

    n_FEsplits= luigi.Parameter()
    n_FEcpu= luigi.Parameter()

    APMuTask1path= luigi.Parameter()
    APM_Filename=luigi.Parameter()

    ACcutoff= luigi.Parameter()
    ACcutoffHash= luigi.Parameter()
    ACuTaskpath= luigi.Parameter()

    def requires(self):
        task1 = TrainModel(JobName=self.TM_JobName,JobHash=self.TM_JobHash,GTparamsHash=self.GTparamsHash,SoundFileRootDir_Host=self.SoundFileRootDir_Host,\
                           IDlength=self.IDlength,GTfile=self.GTfile,FGfile=self.FGfile,FileGroupHashes=self.FileGroupHashes,FileGroupID=self.FileGroupID,EDprocess=self.EDprocess,EDsplits=self.EDsplits,\
                           EDcpu=self.EDcpu,EDchunk=self.EDchunk,EDmethodID=self.EDmethodID,EDparamString=self.EDparamString,EDparamsHash=self.EDparamsHash,ALprocess=self.ALprocess,\
                           ALmethodID=self.ALmethodID,ALparamString=self.ALparamString,ALparamsHash=self.ALparamsHash,ALuTask1path=self.ALuTask1path,ALuTask2path=self.ALuTask2path,\
                           FEprocess=self.FEprocess,FEmethodID=self.FEmethodID,FEparamString=self.FEparamString,FEparamsHash=self.FEparamsHash,FE1uTaskpath=self.FE1uTaskpath,\
                           FEsplits=self.FEsplits,FEcpu=self.FEcpu,MFAprocess=self.MFAprocess,MFAmethodID=self.MFAmethodID,MFAparamsHash=self.MFAparamsHash,\
                           MFAuTask1path=self.MFAuTask1path,MFAuTask2path=self.MFAuTask2path,TMprocess=self.TMprocess,TMmethodID=self.TMmethodID,TMparams=self.TMparams,\
                           stage=self.TMstage,TM_outName=self.TM_outName,CVcpu=self.CVcpu)
        for l in range(self.IDlength):
            task2 = FormatFG(FGfile = self.n_FGfile[l],FGhash = self.n_FileGroupHashes[l])
            task3 = UnifyED(upstream_task = task1,splits = self.n_EDsplits,FGhash=self.n_FileGroupHashes[l],SoundFileRootDir_Host=self.n_SoundFileRootDir_Host,\
                            EDparamsHash=self.EDparamsHash,Params=self.EDparamString,MethodID=self.EDmethodID,ProcessID=self.EDprocess,CPU=self.n_EDcpu,\
                            Chunk=self.n_EDchunk)
            task4 = UnifyFE(upstream_task = task2,FGhash=self.n_FileGroupHashes[l],uTaskpath=FE1uTaskpath,FEparamsHash=self.FEparamsHash,MethodID=self.FEmethodID,\
                            ProcessID=self.FEprocess,Params=self.FEparamString,splits=self.n_FEsplits,CPU=self.n_FEcpu,SoundFileRootDir_Host=self.n_SoundFileRootDir_Host)
            task4 = AssignLabels(upstream_task1 = task3,upstream_task2 = task2,FGhash=self.FileGroupHashes[l],uTask1path=self.ALuTask1path,uTask2path=self.ALuTask2path,\
                                 ALparamsHash=self.ALparamsHash,MethodID=self.ALmethodID,ProcessID=self.ALprocess,Params=self.ALparamString,stage=self.ALstage)#copied, add params and make sure fits
            task6 = MergeFE_AL(upstream_task1 = task5,upstream_task2 = task4,FGhash=self.FileGroupHashes[l],uTask1path=self.MFAuTask1path,uTask2path=self.MFAuTask2path,\
                               ProcessID=self.MFAprocess,MFAparamsHash=self.MFAparamsHash,MethodID=self.MFAmethodID)#copied, add params and make sure fits
            task5 = ApplyModel(upstream_task1=task4,upstream_task2=task1,uTask1path=self.APMuTask1path,uTask2path=self.TM_JobHash,uTask1FileName=self.APM_Filename,\
                               FGhash=self.n_FileGroupHashes[l],ProcessID=self.TMprocess,MethodID=self.TMmethodID)
            task6 = ApplyCutoff(upstream_task=task5,uTaskpath=self.ACuTaskpath,FGhash=self.FileGroupHashes[l],CutoffHash=self.ACcutoffHash,Cutoff=self.ACcutoff)
            yield task6

    def output(self):
        return None
    def run(self):

        return None


if __name__ == '__main__':
    luigi.build([TrainTest(JobName=JobName,JobHash=JobHash,ParamsRoot=ParamsRoot,GTparamsHash=GTparamsHash,SoundFileRootDir_Host=SoundFileRootDir_Host,IDlength=IDlength,\
                            GTfile=GTfile,FGfile=FGfile,FileGroupHashes=FileGroupHashes,FileGroupID=FileGroupID,EDprocess=EDprocess,EDsplits=EDsplits,EDcpu=EDcpu,\
                            EDchunk=EDchunk,EDmethodID=EDmethodID,EDparamString=EDparamString,EDparamsHash=EDparamsHash,ALprocess=ALprocess,\
                            ALmethodID=ALmethodID,ALparamString=ALparamString,ALparamsHash=ALparamsHash,ALuTask1path=ALuTask1path,ALuTask2path=ALuTask2path,\
                            FEprocess=FEprocess,FEmethodID=FEmethodID,FEparamString=FEparamString,FEparamsHash=FEparamsHash,FE1uTaskpath=FE1uTaskpath,\
                            FEsplits=FEsplits,FEcpu=FEcpu,MFAprocess=MFAprocess,MFAmethodID=MFAmethodID,MFAparamsHash=MFAparamsHash,\
                            MFAuTask1path=MFAuTask1path,MFAuTask2path=MFAuTask2path,TMprocess=TMprocess,TMmethodID=TMmethodID,TMparams=TMparamString,\
                            CVcpu=CVcpu,TM_outName=TM_outName,TMstage=TMstage,APM_Filename=APM_Filename,APMuTask1path=APMuTask1path,ACcutoff=ACcutoffString,\
                            ACcutoffHash=ACcutoffHash,ACuTaskpath=ACuTaskpath,n_SoundFileRootDir_Host=n_SoundFileRootDir_Host,n_FileGroupID=n_FileGroupID,\
                            n_FileGroupHashes=n_FileGroupHashes,n_EDsplits=n_EDsplits,n_EDcpu=n_EDcpu,n_EDchunk=n_EDchunk,n_FEsplits=n_FEsplits,\
                            n_FEcpu=n_FEcpu,TM_JobName=TM_JobName,TM_JobHash=TM_JobHash,n_FGfile=n_FGfile)], local_scheduler=True)

