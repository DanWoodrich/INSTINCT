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

#Cross validate RF models from the training data to produce performance estimate
#

ProjectRoot='C:/instinct_dt/' #linux install
#ProjectRoot='C:/test/' #windows

CacheRoot=ProjectRoot + 'Cache/'
DataRoot=ProjectRoot +'Data/'
OutputsRoot=ProjectRoot +'Outputs/'

#####parse config for parameters#######

JobName='CVModel'
    
ParamsRoot=ProjectRoot + 'etc/' + JobName + '/'

MasterINI = configparser.ConfigParser()
MasterINI.read(ParamsRoot + 'Master.ini')

FileGroupID = MasterINI['FormatFG']['FileGroupID']
GT_signal_code = MasterINI['FormatGT']['GT_signal_code']
GTparams = sorted(MasterINI.items('FormatGT')).__str__()
GTparamsHash= GTparams.encode('utf8')
GTparamsHash = str(hashlib.sha1(GTparamsHash).hexdigest())

SoundFileRootDir_Host = MasterINI['FormatFG']['SoundFileRootDir_Host']

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
ALparamsHash= ALparamString.encode('utf8') 
ALparamsHash = str(hashlib.sha1(ALparamsHash).hexdigest())

ALuTask1path = EDparamsHash 
ALuTask2path =GTparamsHash

MFAprocess = 'MergeFE_AL'
MFAmethodID = MasterINI[MFAprocess]['MethodID']
MFAparamsHash= MFAmethodID.encode('utf8')
MFAparamsHash = str(hashlib.sha1(MFAparamsHash).hexdigest())

MFAuTask1path = EDparamsHash + '/' + FEparamsHash
MFAuTask2path = EDparamsHash + '/' + ALparamsHash

TMprocess = 'TrainModel'
TMstage='CV'
TMmethodID = MasterINI[TMprocess]['MethodID']
TM_INI = configparser.ConfigParser()
TM_INI.read(ParamsRoot + TMmethodID +'.ini')
#for general params
TMparamList_g = sorted(TM_INI.items(TMprocess))
TMparamList_g = Helper.paramList(TMparamList_g)
TMparamString_g =' '.join(TMparamList_g)
#for CV params
TMparamList_cv = sorted(TM_INI.items(TMstage))
TMparamList_cv = Helper.paramList(TMparamList_cv)
TMparamString_cv =' '.join(TMparamList_cv)
#if stage is not CV, fill in these params with dummy values.


#combine all params into one list and hash it.

TMparamString =...+ ' ' + TMmethodID
TMparamsHash= TMparamString.encode('utf8') 
TMparamsHash = str(hashlib.sha1(TMparamsHash).hexdigest())

##TMuTaskpath = EDparamsHash #don't do the upstream path like this since the methods are in this job. will still call an R method like in instinct.py

strings = [None] * IDlength

#need to add GT info in here. Instead of a path, all relevant sections in alphabetical order. 
for k in range(IDlength):
    strings[k]= ALparamsHash + EDparamsHash + FileGroupHashes[k] + GTHashes[k] + MFAparamsHash + TMparamsHash 

#hash of all the parameters
strings = sorted(strings).__str__()
JobHash= strings.encode('utf8')
JobHash = str(hashlib.sha1(JobHash).hexdigest())

class CVmodel(luigi.Task):    
    JobName=luigi.Parameter()
    JobHash=luigi.Parameter()
    
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

    def requires(self):
        for l in range(self.IDlength):
            task1 = FormatFG(FGfile = self.FGfile[l],FGhash = self.FileGroupHashes[l])
            #ok to hard link UnifyED and UnifyFE dependencies. Pass the parameter upstream in the ED group, and specify the upstream task in the splitED step. 
            task2 = FormatGT(GTfile=self.GTfile[l],FGhash = self.FileGroupHashes[l],FGfile = self.FGfile[l],GThash=self.GTparamsHash)

            task3 = UnifyED(upstream_task = task1,splits = self.EDsplits,FGhash=self.FileGroupHashes[l],SoundFileRootDir_Host=self.SoundFileRootDir_Host,\
                            EDparamsHash=self.EDparamsHash,Params=self.EDparamString,MethodID=self.EDmethodID,ProcessID=self.EDprocess,CPU=self.EDcpu,\
                            Chunk=self.EDchunk)

            task4 = AssignLabels(upstream_task1 = task3,upstream_task2 = task2,FGhash=self.FileGroupHashes[l],uTask1path=self.ALuTask1path,uTask2path=self.ALuTask2path,\
                                 ALparamsHash=self.ALparamsHash,MethodID=self.ALmethodID,ProcessID=self.ALprocess,Params=self.ALparamString)

            task5 = UnifyFE(upstream_task = task2,FGhash=self.FileGroupHashes[l],uTaskpath=FE1uTaskpath,FEparamsHash=self.FEparamsHash,MethodID=self.FEmethodID,\
                            ProcessID=self.FEprocess,Params=self.FEparamString,splits=FEsplits,CPU=FEcpu,SoundFileRootDir_Host=SoundFileRootDir_Host)
            
            task6 = MergeFE_AL(upstream_task1 = task5,upstream_task2 = task4,FGhash=self.FileGroupHashes[l],uTask1path=self.MFAuTask1path,uTask2path=self.MFAuTask2path,\
                               ProcessID=self.MFAprocess,MFAparamsHash=self.MFAparamsHash,MethodID=self.MFAmethodID)
            yield task6
    def output(self):
        return luigi.LocalTarget(OutputsRoot + self.JobName + '/' + self.JobHash + '/RFmodel.rds')
    def run(self):
        
        #concatenate outputs and summarize

        #load in 
        dataframes = [None] * self.IDlength
        FGdf = [None] * self.IDlength
        for k in range(self.IDlength):
            dataframes[k] = pd.read_csv(CacheRoot + self.FileGroupHashes[k] + '/' + self.EDparamsHash + '/' + self.ALparamsHash + '/' + self.MFAparamsHash + '/DETwFEwAL.csv.gz')
            FGdf[k] = pd.read_csv(CacheRoot + self.FileGroupHashes[k] + '/FileGroupFormat.csv.gz')
        TMdat = pd.concat(dataframes,ignore_index=True)
        FGdf = pd.concat(FGdf,ignore_index=True)

        if not os.path.exists(OutputsRoot):
            os.mkdir(OutputsRoot)

        resultPath1 = OutputsRoot + self.JobName 
        if not os.path.exists(resultPath1):
            os.mkdir(resultPath1)

        resultPath2 = OutputsRoot + self.JobName + '/' + self.JobHash
        if not os.path.exists(resultPath2):
            os.mkdir(resultPath2)


        #evetually should compress these csv for disk effeciency.  
        TMdat.to_csv(resultPath2 + '/' + self.JobName + '_Intermediate1.csv',index=False)
        FGdf.to_csv(resultPath2 + '/' + self.JobName + '_Intermediate2.csv',index=False)

        #
        TMpath = resultPath2 + '/' + self.JobName + '_Intermediate1.csv'
        FGpath = resultPath2 + '/' + self.JobName + '_Intermediate2.csv'
        Mpath = 'NULL'

        print(self.TMparams)
        command = '"C:\\Program Files\\R\\R-3.6.1\\bin\\Rscript.exe" -e "source(\'' +ProjectRoot + 'bin/' + self.TMprocess + '/' + self.TMmethodID +'/'+self.TMmethodID+'.R\')"'
        command = command +' '+ TMpath +' '+ FGpath +' '+ resultPath2 + ' ' + self.stage + ' ' + Mpath + ' ' + self.TMparams 

        subprocess.run(shlex.split(command))

        #
        

        #os.remove(resultPath2 + '/' + self.JobName + '_Intermediate1.csv')
        #os.remove(resultPath2 + '/' + self.JobName + '_Intermediate2.csv')

        #copy params to output folder


if __name__ == '__main__':
    luigi.build([TrainModel(JobName=JobName,JobHash=JobHash,ParamsRoot=ParamsRoot,GTparamsHash=GTparamsHash,SoundFileRootDir_Host=SoundFileRootDir_Host,IDlength=IDlength,\
                            GTfile=GTfile,FGfile=FGfile,FileGroupHashes=FileGroupHashes,FileGroupID=FileGroupID,EDprocess=EDprocess,EDsplits=EDsplits,EDcpu=EDcpu,\
                            EDchunk=EDchunk,EDmethodID=EDmethodID,EDparamString=EDparamString,EDparamsHash=EDparamsHash,ALprocess=ALprocess,\
                            ALmethodID=ALmethodID,ALparamString=ALparamString,ALparamsHash=ALparamsHash,ALuTask1path=ALuTask1path,ALuTask2path=ALuTask2path,\
                            FEprocess=FEprocess,FEmethodID=FEmethodID,FEparamString=FEparamString,FEparamsHash=FEparamsHash,FE1uTaskpath=FE1uTaskpath,\
                            FEsplits=FEsplits,FEcpu=FEcpu,MFAprocess=MFAprocess,MFAmethodID=MFAmethodID,MFAparamsHash=MFAparamsHash,\
                            MFAuTask1path=MFAuTask1path,MFAuTask2path=MFAuTask2path,TMprocess=TMprocess,TMmethodID=TMmethodID,TMparams=TMparamString)], local_scheduler=True)


