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


#Performance evalation for a group of event detectors (see EDperfeval.py for example of how to populate params)

class EDperfEval(FormatFG,FormatGT,UnifyED,AssignLabels,PerfEval1):
    
    JobName=luigi.Parameter()
    EDpe1_WriteToOutputs=luigi.Parameter()

    IDlength = luigi.IntParameter()

    #nullify some inherited parameters:
    upstream_task1=None
    upstream_task2=None
    upstream_task3=None
    uTask1path=None
    uTask2path=None
    uTask3path=None

    def pipelineMap(self,l): #here is where you define pipeline structure 
        task0 = FormatFG.invoke(self,l) 
        task1 = FormatGT.invoke(self,task0,l)
        task2 = UnifyED.invoke(self,task0)
        task3 = AssignLabels.invoke(self,task2,task1,task0)
        task4 = PerfEval1.invoke(self,task3,task0,l)
        return [task0,task1,task2,task3,task4]
    def hashProcess(self):
        #this is just composed of the component hashes (PE1, method being run here, is accounted for in pipeline).
        hashStrings = [None] * self.IDlength
        for l in range(self.IDlength):
            tasks = self.pipelineMap(l)
            hashStrings[l] = ' '.join([tasks[0].hashProcess(),tasks[1].hashProcess(),tasks[2].hashProcess(),tasks[3].hashProcess(),
                                  tasks[4].hashProcess()])
    
        EDpe1_JobHash = Helper.getParamHash2(' '.join(hashStrings),12)
        return(EDpe1_JobHash)
    def outpath(self):
        if self.EDpe1_WriteToOutputs=='y':
            return self.ProjectRoot +'Outputs/' + self.JobName 
        elif self.EDpe1_WriteToOutputs=='n':
            return self.ProjectRoot + 'Cache/' + self.hashProcess()
    def requires(self):
        for l in range(self.IDlength):
            tasks = self.pipelineMap(l)

            yield tasks[len(tasks)-1]
    def output(self):
        return luigi.LocalTarget(self.outpath() + '/Stats.csv.gz')
    def run(self):
        
        #concatenate outputs and summarize

        dataframes = [None] * self.IDlength
        for k in range(self.IDlength):
            tasks=self.pipelineMap(k)
            dataframes[k] = pd.read_csv(tasks[4].outpath() + '/Stats.csv.gz',compression='gzip')
        EDeval = pd.concat(dataframes,ignore_index=True)

        resultPath = self.outpath()

        if not os.path.exists(resultPath):
            os.mkdir(resultPath)

        EDeval.to_csv(resultPath + '/Stats_Intermediate.csv',index=False)
        #send back in to PE1

        FGpath = 'NULL'
        LABpath = 'NULL'
        INTpath =  resultPath + '/Stats_Intermediate.csv'
        resultPath2 =   resultPath + '/Stats.csv.gz'
        FGID = 'NULL'

        Paths = [FGpath,LABpath,INTpath,resultPath2]
        Args = [FGID,'2'] #run second stage of R script 

        argParse.run(Program='R',rVers=self.r_version,cmdType=self.system,ProjectRoot=self.ProjectRoot,ProcessID=self.PE1process,MethodID=self.PE1methodID,Paths=Paths,Args=Args,Params='')

        os.remove(resultPath + '/Stats_Intermediate.csv')
        
    def invoke(self):
        return(EDperfEval(JobName=self.JobName,SoundFileRootDir_Host=self.SoundFileRootDir_Host,IDlength=self.IDlength,\
                   GTfile=self.GTfile,FGfile=self.FGfile,FileGroupID=self.FileGroupID,EDprocess=self.EDprocess,EDsplits=self.EDsplits,EDcpu=self.EDcpu,\
                   EDchunk=self.EDchunk,EDmethodID=self.EDmethodID,EDparamString=self.EDparamString,EDparamNames=self.EDparamNames,ALprocess=self.ALprocess,\
                   ALmethodID=self.ALmethodID,ALparamString=self.ALparamString,\
                   PE1process=self.PE1process,PE1methodID=self.PE1methodID,EDpe1_WriteToOutputs=self.EDpe1_WriteToOutputs,\
                   ProjectRoot=self.ProjectRoot,system=self.system,r_version=self.r_version))



if __name__ == '__main__':
    luigi.build([EDperfEval.invoke(params)], local_scheduler=True)

