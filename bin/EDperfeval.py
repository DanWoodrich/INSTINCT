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

#this job is to evaluate the performance of the energy detector, and summarize it across moorings.  

ProjectRoot=Helper.getProjRoot()

#####parse config for parameters#######

JobName='EDperfeval'
    
ParamsRoot=ProjectRoot + 'etc/' + JobName + '/'

MasterINI = configparser.ConfigParser()
MasterINI.read(ParamsRoot + 'Master.ini')

#get global params
system=MasterINI['Global']['system']
r_version=MasterINI['Global']['r_version']

FGparams = FG(MasterINI,'FormatFG',ProjectRoot).getParams()
GTparams = GT(MasterINI,'FormatGT',ProjectRoot,FGparams.FileGroupID).getParams()

EDparams = ED(MasterINI,'EventDetector',ParamsRoot).getParams()

ALparams = AL(MasterINI,'AssignLabels',ParamsRoot,EDparams.paramHash,GTparams.paramHash,GTparams.paramHash).getParams()

PE1params = PE1(MasterINI,'PerfEval1',str(ALparams.uTask1path + '/' + ALparams.paramHash)).getParams()

ContPath = 'y'

WriteToOutputs='y' #yes if calling as a standalone job, but if no will write to new unnamed location in cache

Job_processes = [ALparams.paramHash,EDparams.paramHash,PE1params.paramHash] #alphabetical order

JobHash =hashJob(FGparams.FileGroupHashes,GTparams.GTHashes,Job_processes)

if __name__ == '__main__':
    luigi.build([EDperfEval(JobName=JobName,JobHash=JobHash,WriteToOutputs=WriteToOutputs,SoundFileRootDir_Host=FGparams.SoundFileRootDir_Host,\
                            IDlength=FGparams.IDlength,FGfile=FGparams.FGfile,FileGroupHashes=FGparams.FileGroupHashes,FileGroupID=FGparams.FileGroupID,\
                            GTfile=GTparams.GTfile,GTparamsHash=GTparams.paramHash,EDprocess=EDparams.process,
                            EDsplits=EDparams.Splits,EDcpu=EDparams.CPUNeed,EDchunk=EDparams.sf_chunk_size,EDmethodID=EDparams.methodID,\
                            EDparamString=EDparams.paramString,EDparamsHash=EDparams.paramHash,ALprocess=ALparams.process,ALmethodID=ALparams.methodID,\
                            ALparamString=ALparams.paramString,ALparamsHash=ALparams.paramHash,ALuTask1path=ALparams.uTask1path,ALuTask2path=ALparams.uTask2path,\
                            PE1process=PE1params.process,PE1methodID=PE1params.methodID,PE1paramsHash=PE1params.paramHash,PE1uTaskpath=PE1params.uTaskpath,\
                            PE1ContPath=ContPath,ProjectRoot=ProjectRoot,system=system,r_version=r_version)], local_scheduler=True)

