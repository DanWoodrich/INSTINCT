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
from EDperfeval import EDperfeval

#This job is to perform a parameter sweep of a single ED parameter
#Summarizing step not currently built out- plan is to combine edperfeval
#datasets, but give an identifier to which parameter is changing. 


ProjectRoot='C:/instinct_dt/' #linux install
#ProjectRoot='C:/test/' #windows

CacheRoot=ProjectRoot + 'Cache/'
DataRoot=ProjectRoot +'Data/'

#####parse config for parameters#######


MacroJobName='EDparamSweep'
JobName='EDperfeval'

EDPE_WriteToOutputs='n'

paramsweep1=range(1,2,1) #set these manually
param1pos = 9 #set these manually 
#paramsweep2=range(0,3,1)
    
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

for l in range(IDlength):
    GTfile[l] = DataRoot + 'GroundTruth/' + GT_signal_code + '_' +FileGroupID[l]
    FGfile[l] = DataRoot + 'FileGroups/' + FileGroupID[l]
    FileGroupHashes[l] = Helper.hashfile(FGfile[l])

ALprocess = 'AssignLabels'
ALmethodID = MasterINI[ALprocess]['MethodID']
AL_INI = configparser.ConfigParser()
AL_INI.read(ParamsRoot + ALmethodID +'.ini')
ALparamList = sorted(AL_INI.items(ALprocess))
ALparamList = Helper.paramList(ALparamList)
ALparamString =' '.join(ALparamList)+ ' ' + ALmethodID
ALparamsHash= ALparamString.encode('utf8') 
ALparamsHash = str(hashlib.sha1(ALparamsHash).hexdigest())

EDprocess = 'EventDetector'
EDmethodID = MasterINI[EDprocess]['MethodID']
EDsplits = int(MasterINI[EDprocess]['Splits'])
EDcpu = MasterINI[EDprocess]['CPUneed']
EDchunk = MasterINI[EDprocess]['sf_chunk_size']
ED_INI = configparser.ConfigParser()
ED_INI.read(ParamsRoot + EDmethodID + '.ini')
EDparamList=sorted(ED_INI.items(EDprocess))
EDparamID=Helper.paramID(EDparamList,param1pos)
EDparamList = Helper.paramList(EDparamList)

EDparamListString = [None]*len(paramsweep1)
EDparamListHash = [None]*len(paramsweep1)
for n in range(len(paramsweep1)):
    ListIn = EDparamList
    ListIn[param1pos] = str(paramsweep1[n]/20)
    ListInString =' '.join(ListIn)+ ' ' +EDmethodID
    EDparamListString[n] = ListInString
    ListInHash= ListInString.encode('utf8')
    ListInHash = str(hashlib.sha1(ListInHash).hexdigest())
    EDparamListHash[n] = ListInHash


ALuTask1path = EDparamListHash 
ALuTask2path =GTparamsHash

PE1process = 'PerfEval1'
PE1methodID = MasterINI[PE1process]['MethodID']
PE1paramsHash= PE1methodID.encode('utf8')
PE1paramsHash = str(hashlib.sha1(PE1paramsHash).hexdigest())

PE1uTaskpath = [None]*len(paramsweep1)
for n in range(len(paramsweep1)):
    PE1uTaskpath[n]=EDparamListHash[n] + '/' + ALparamsHash
    
hashes1 = [None] * len(paramsweep1)

for l in range(len(paramsweep1)):
    strings = [None] * IDlength

    for k in range(IDlength):
        strings[k]= CacheRoot + FileGroupHashes[k] + '/' + EDparamListHash[l] + '/' + ALparamsHash + '/' + PE1paramsHash + '/Stats.csv.gz'

    #hash of all the parameters
    strings = sorted(strings).__str__()
    JobHash= strings.encode('utf8')
    JobHash = str(hashlib.sha1(JobHash).hexdigest())
    hashes1[l]=JobHash

MacroJobHash = sorted(hashes1).__str__()
MacroJobHash= MacroJobHash.encode('utf8')
MacroJobHash = str(hashlib.sha1(MacroJobHash).hexdigest())

class EDpeParamSweep(luigi.Task):
    #define params, make lists of sweep params to pass. hardcode sweep decisions in this body instead of passing though as its own parameter

    MacroJobName=luigi.Parameter()
    MacroJobHash=luigi.Parameter()

    paramsweep = luigi.Parameter()
    EDparamID= luigi.Parameter()

    JobName=luigi.Parameter()
    EDPE_WriteToOutputs=luigi.Parameter()

    hashes1=luigi.Parameter() #n=JobHash
    
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
    
    EDparamListString=luigi.Parameter() #n=EDparamString
    EDparamListHash=luigi.Parameter()   #n=EDparamHash
    
    ALprocess = luigi.Parameter()
    ALmethodID = luigi.Parameter()
    ALparamString=luigi.Parameter()
    ALparamsHash=luigi.Parameter()

    ALuTask1path=luigi.Parameter()
    ALuTask2path=luigi.Parameter()

    PE1process = luigi.Parameter()
    PE1methodID = luigi.Parameter()
    PE1paramsHash= luigi.Parameter()

    PE1uTaskpath=luigi.Parameter()

        
    def requires(self):
        for l in range(len(self.EDparamListString)):
            yield EDperfeval(JobName=self.JobName,JobHash=self.hashes1[l],ParamsRoot=self.ParamsRoot,GTparamsHash=self.GTparamsHash,SoundFileRootDir_Host=self.SoundFileRootDir_Host,IDlength=self.IDlength,\
                            GTfile=self.GTfile,FGfile=self.FGfile,FileGroupHashes=self.FileGroupHashes,FileGroupID=self.FileGroupID,EDprocess=self.EDprocess,EDsplits=self.EDsplits,EDcpu=self.EDcpu,\
                            EDchunk=self.EDchunk,EDmethodID=self.EDmethodID,EDparamString=self.EDparamListString[l],EDparamsHash=self.EDparamListHash[l],ALprocess=self.ALprocess,\
                            ALmethodID=self.ALmethodID,ALparamString=self.ALparamString,ALparamsHash=self.ALparamsHash,ALuTask1path=ALuTask1path[l],ALuTask2path=ALuTask2path,\
                            PE1process=self.PE1process,PE1methodID=self.PE1methodID,PE1paramsHash=self.PE1paramsHash,PE1uTaskpath=PE1uTaskpath[l],WriteToOutputs=EDPE_WriteToOutputs)
    def output(self):
        #return luigi.LocalTarget(ProjectRoot+'Outputs/' + self.MacroJobName + '/' + self.MacroJobHash + '/' + self.MacroJobName + '.csv')
        None
    def run(self):
        #load in all datasets, give identifier of which parameter being changed, combine
        #can just 
        print(self.EDparamID)

if __name__ == '__main__':
    #dont even have to do it this way, can have a wrapper task to send dynamic parameters
    luigi.build([EDpeParamSweep(MacroJobName=MacroJobName,MacroJobHash=MacroJobHash,paramsweep=paramsweep1,EDparamID=EDparamID,JobName=JobName,hashes1=hashes1,ParamsRoot=ParamsRoot,GTparamsHash=GTparamsHash,SoundFileRootDir_Host=SoundFileRootDir_Host,IDlength=IDlength,\
                            GTfile=GTfile,FGfile=FGfile,FileGroupHashes=FileGroupHashes,FileGroupID=FileGroupID,EDprocess=EDprocess,EDsplits=EDsplits,EDcpu=EDcpu,\
                            EDchunk=EDchunk,EDmethodID=EDmethodID,EDparamListString=EDparamListString,EDparamListHash=EDparamListHash,ALprocess=ALprocess,\
                            ALmethodID=ALmethodID,ALparamString=ALparamString,ALparamsHash=ALparamsHash,ALuTask1path=ALuTask1path, ALuTask2path=ALuTask2path,\
                            PE1process=PE1process,PE1methodID=PE1methodID,PE1paramsHash=PE1paramsHash,PE1uTaskpath=PE1uTaskpath,EDPE_WriteToOutputs=EDPE_WriteToOutputs)], local_scheduler=True)
