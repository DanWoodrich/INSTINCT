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

#this job just runs through a bunch of FE. use it to make sure FE is working for model creation, or to profile speed of FE. 



ProjectRoot='C:/instinct_dt/' #linux install
#ProjectRoot='C:/test/' #windows

CacheRoot=ProjectRoot + 'Cache/'
DataRoot=ProjectRoot +'Data/'

#####parse config for parameters#######

JobName='runFE'
    
ParamsRoot=ProjectRoot + 'etc/' + JobName + '/'

MasterINI = configparser.ConfigParser()
MasterINI.read(ParamsRoot + 'Master.ini')

FileGroupID = MasterINI['FormatFG']['FileGroupID']

SoundFileRootDir_Host = MasterINI['FormatFG']['SoundFileRootDir_Host']

FileGroupID = sorted(FileGroupID.split(','))
IDlength = len(FileGroupID)
FGfile = [None] * IDlength

FileGroupHashes = [None] * IDlength

for l in range(IDlength):
    FGfile[l] = DataRoot + 'FileGroups/' + FileGroupID[l]
    FileGroupHashes[l] = Helper.hashfile(FGfile[l])

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

strings = [None] * IDlength
        
for k in range(IDlength):
    strings[k]= CacheRoot + FileGroupHashes[k] + '/' + EDparamsHash + '/' + FEparamsHash + '/Stats.csv.gz'

#hash of all the parameters
strings = sorted(strings).__str__()
JobHash= strings.encode('utf8')
JobHash = str(hashlib.sha1(JobHash).hexdigest())

class runFE(luigi.Task):    
    JobName=luigi.Parameter()
    JobHash=luigi.Parameter()
    
    ParamsRoot=luigi.Parameter()

    SoundFileRootDir_Host = luigi.Parameter()

    IDlength = luigi.IntParameter()
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
    FEsplits = luigi.Parameter()
    FEcpu = luigi.Parameter()
    FEmethodID = luigi.Parameter()
    FEparamString=luigi.Parameter()
    FEparamsHash=luigi.Parameter()

    FE1uTaskpath=luigi.Parameter()

    def requires(self):
        for l in range(self.IDlength):
            task1 = FormatFG(FGfile = self.FGfile[l],FGhash = self.FileGroupHashes[l])

            task2 = UnifyED(upstream_task = task1,splits = self.EDsplits,FGhash=self.FileGroupHashes[l],SoundFileRootDir_Host=self.SoundFileRootDir_Host,\
                            EDparamsHash=self.EDparamsHash,Params=self.EDparamString,MethodID=self.EDmethodID,ProcessID=self.EDprocess,CPU=self.EDcpu,\
                            Chunk=self.EDchunk)

            task3 = UnifyFE(upstream_task = task2,FGhash=self.FileGroupHashes[l],uTaskpath=FE1uTaskpath,FEparamsHash=self.FEparamsHash,MethodID=self.FEmethodID,\
                            ProcessID=self.FEprocess,Params=self.FEparamString,splits=FEsplits,CPU=FEcpu,SoundFileRootDir_Host=SoundFileRootDir_Host)
            
            yield task3
    def output(self):
        return None
    def run(self):
        return None

if __name__ == '__main__':
    luigi.build([runFE(JobName=JobName,JobHash=JobHash,ParamsRoot=ParamsRoot,SoundFileRootDir_Host=SoundFileRootDir_Host,IDlength=IDlength,\
                            FGfile=FGfile,FileGroupHashes=FileGroupHashes,FileGroupID=FileGroupID,EDprocess=EDprocess,EDsplits=EDsplits,EDcpu=EDcpu,\
                            EDchunk=EDchunk,EDmethodID=EDmethodID,EDparamString=EDparamString,EDparamsHash=EDparamsHash,FEprocess=FEprocess,FEmethodID=FEmethodID,\
                            FEparamString=FEparamString,FEparamsHash=FEparamsHash,FE1uTaskpath=FE1uTaskpath,FEsplits=FEsplits,FEcpu=FEcpu)], local_scheduler=True)

