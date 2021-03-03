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

#Create a single RF model from training data

ProjectRoot='C:/instinct_dt/' #linux install
#ProjectRoot='C:/test/' #windows

CacheRoot=ProjectRoot + 'Cache/'
DataRoot=ProjectRoot +'Data/'

#####parse config for parameters#######

JobName='TrainModel'
    
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
ALparamString2hash = ALparamString+ GTparamsHash #add the other params hash (not in main path)  so that if it changes to ALparams output target will also change 
ALparamsHash= ALparamString2hash.encode('utf8') 
ALparamsHash = str(hashlib.sha1(ALparamsHash).hexdigest())

ALuTask1path = EDparamsHash 
ALuTask2path =GTparamsHash

MFAprocess = 'MergeFE_AL'
MFAmethodID = MasterINI[MFAprocess]['MethodID']  
MFAmethodID2hash=MFAmethodID + FEparamsHash #add the other params hash (not in main path)  so that if it changes to ALparams output target will also change 
MFAparamsHash= MFAmethodID2hash.encode('utf8')
MFAparamsHash = str(hashlib.sha1(MFAparamsHash).hexdigest())

MFAuTask1path = EDparamsHash + '/' + FEparamsHash
MFAuTask2path = EDparamsHash + '/' + ALparamsHash

TMprocess = 'TrainModel'
TMstage = 'train'
TM_outName='RFmodel.rds' #DETwProbs.csv.gz for CV or apply stage
TMmethodID = MasterINI[TMprocess]['MethodID']
TM_INI = configparser.ConfigParser()
TM_INI.read(ParamsRoot + TMmethodID +'.ini')
#for general params
TMparamList = sorted(TM_INI.items(TMprocess)+ [('cv_it', '1'), ('cv_split', '1')]) #add dummy params 
TMparamList = Helper.paramList(TMparamList)
TMparamString =' '.join(TMparamList) + ' ' +TMmethodID
TMparamsHash= TMparamString.encode('utf8') 
TMparamsHash = str(hashlib.sha1(TMparamsHash).hexdigest())
#combine all params into one list and hash it.

CVcpu='1'

strings = [None] * IDlength

#need to add GT info in here. Instead of a path, all relevant sections in alphabetical order. 
for k in range(IDlength):
    strings[k]= ALparamsHash + EDparamsHash + FEparamsHash +FileGroupHashes[k] + GTHashes[k] + MFAparamsHash + TMparamsHash 

#hash of all the parameters
strings = sorted(strings).__str__()
JobHash= strings.encode('utf8')
JobHash = str(hashlib.sha1(JobHash).hexdigest())

if __name__ == '__main__':
    luigi.build([TrainModel(JobName=JobName,JobHash=JobHash,GTparamsHash=GTparamsHash,SoundFileRootDir_Host=SoundFileRootDir_Host,IDlength=IDlength,\
                            GTfile=GTfile,FGfile=FGfile,FileGroupHashes=FileGroupHashes,FileGroupID=FileGroupID,EDprocess=EDprocess,EDsplits=EDsplits,EDcpu=EDcpu,\
                            EDchunk=EDchunk,EDmethodID=EDmethodID,EDparamString=EDparamString,EDparamsHash=EDparamsHash,ALprocess=ALprocess,\
                            ALmethodID=ALmethodID,ALparamString=ALparamString,ALparamsHash=ALparamsHash,ALuTask1path=ALuTask1path,ALuTask2path=ALuTask2path,\
                            FEprocess=FEprocess,FEmethodID=FEmethodID,FEparamString=FEparamString,FEparamsHash=FEparamsHash,FE1uTaskpath=FE1uTaskpath,\
                            FEsplits=FEsplits,FEcpu=FEcpu,MFAprocess=MFAprocess,MFAmethodID=MFAmethodID,MFAparamsHash=MFAparamsHash,\
                            MFAuTask1path=MFAuTask1path,MFAuTask2path=MFAuTask2path,TMprocess=TMprocess,TMmethodID=TMmethodID,TMparams=TMparamString,\
                            stage=TMstage,TM_outName=TM_outName,CVcpu=CVcpu)], local_scheduler=True)


