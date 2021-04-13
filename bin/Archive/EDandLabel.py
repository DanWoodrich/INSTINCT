import luigi
import os
import hashlib
import configparser
import pandas as pd
import sys
import numpy
from instinct import *

ProjectRoot='/home/daniel.woodrich/Projects/instinct_hpc/' #linux install
#ProjectRoot='C:/test/' #windows

CacheRoot=ProjectRoot + 'Cache/'
DataRoot=ProjectRoot +'Data/'

class EDandLabel(luigi.Task):
    #this assumes that you have a GT for every FG- assign labels to ED output
    ParamsRoot=ProjectRoot + 'etc/' + 'EDandLabel/'

    MasterINI = configparser.ConfigParser()
    MasterINI.read(ParamsRoot + 'Master.ini')

    FileGroupID = MasterINI['FormatFG']['FileGroupID']
    GT_signal_code = MasterINI['FormatGT']['GT_signal_code']
    
    SoundFileRootDir_Host = MasterINI['FormatFG']['SoundFileRootDir_Host']

    FileGroupID = FileGroupID.split(',')
    IDlength = len(FileGroupID)
    FGfile = [None] * IDlength
    GTfile = [None] * IDlength

    FileGroupHashes = [None] * IDlength
    
    for l in range(IDlength):
        GTfile[l] = DataRoot + 'GroundTruth/' + GT_signal_code + '_' +FileGroupID[l]
        FGfile[l] = DataRoot + 'FileGroups/' + FileGroupID[l]
        FileGroupHashes[l] = Helper.hashfile(FGfile[l])

    EDcontName = MasterINI['EventDetector']['ContainerID']
    EDtagName = MasterINI['EventDetector']['ContainerTag']
    EDtagNameMod = EDtagName.replace('.', '-')
    EDsplits = int(MasterINI['EventDetector']['Splits'])
    
    ED_INI = configparser.ConfigParser()
    ED_INI.read(ParamsRoot + EDcontName + '-' + EDtagNameMod +'.ini')
    EDparams=sorted(ED_INI.items('EventDetector')).__str__()+EDcontName+EDtagName
    EDparamsHash= EDparams.encode('utf8') 
    EDparamsHash = str(hashlib.sha1(EDparamsHash).hexdigest())

    ALcontName = MasterINI['AssignLabels']['ContainerID']
    ALtagName = MasterINI['AssignLabels']['ContainerTag']
    ALtagNameMod = ALtagName.replace('.', '-')

    AL_INI = configparser.ConfigParser()
    AL_INI.read(ParamsRoot + ALcontName + '-' + ALtagNameMod +'.ini')

    ALparams=sorted(AL_INI.items('AssignLabels')).__str__()+ALcontName+ALtagName
    ALparamsHash= ALparams.encode('utf8') 
    ALparamsHash = str(hashlib.sha1(ALparamsHash).hexdigest())

    def requires(self):
        for l in range(self.IDlength):
            task1 = FormatFG(FGfile = self.FGfile[l],FGhash = self.FileGroupHashes[l])
            #ok to hard link UnifyED and UnifyFE dependencies. Pass the parameter upstream in the ED group, and specify the upstream task in the splitED step. 
            task2 = FormatGT(GTfile=self.GTfile[l],FGhash = self.FileGroupHashes[l],FGfile = self.FGfile[l],GT_signal_code=self.GT_signal_code)

            task3 = UnifyED(upstream_task = task1,splits = self.EDsplits,FGhash=self.FileGroupHashes[l],SoundFileRootDir_Host=self.SoundFileRootDir_Host,ContainerID=self.EDcontName,\
                    ContainerTag =self.EDtagName,EDparamsHash=self.EDparamsHash,ParamsRoot=self.ParamsRoot)

            task4 = AssignLabels(upstream_task1 = task3,upstream_task2 = task2,FGhash=self.FileGroupHashes[l],EDhash=self.EDparamsHash, ALparamsHash=self.ALparamsHash,\
                    ContainerID=self.ALcontName,ContainerTag =self.ALtagName,ParamsRoot=self.ParamsRoot,GT_signal_code=self.GT_signal_code)
           
            yield task4 #changing the yield task will change how 'deep' the path goes 
    def output(self):
        return None
    def run(self):
        return None
    
if __name__ == '__main__':
    luigi.run()
