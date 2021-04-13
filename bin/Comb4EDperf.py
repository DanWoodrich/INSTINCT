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

#Right now this is loading params from MPE. Reason being, will use this config for full model development and doesn't make sense to reproduce EDPE1 config. 

C4EP_params = Load_Job('ModelPerfEval')

#add to MPE

C4EP_params = FG(C4EP_params,'FormatFG')
C4EP_params = GT(C4EP_params,'FormatGT')
C4EP_params = ED(C4EP_params,'EventDetector')
C4EP_params = AL(C4EP_params,'AssignLabels')

#Ready a bunch of FGs for shared comparison with PE1 pt 2. 

class Comb4EDperf(FormatFG,FormatGT,UnifyED,AssignLabels,PerfEval1_s1):
    
    JobName=luigi.Parameter()
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
        task4 = PerfEval1_s1.invoke(self,task3,task0,l)
        return [task0,task1,task2,task3,task4]
    def hashProcess(self):
        #this is just composed of the component hashes (PE1, method being run here, is accounted for in pipeline).
        hashStrings = [None] * self.IDlength
        for l in range(self.IDlength):
            tasks = self.pipelineMap(l)
            hashStrings[l] = ' '.join([tasks[0].hashProcess(),tasks[1].hashProcess(),tasks[2].hashProcess(),tasks[3].hashProcess(),
                                  tasks[4].hashProcess()])
    
        return Helper.getParamHash2(' '.join(hashStrings),6)
    def outpath(self):
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

        EDeval.to_csv(resultPath + '/Stats.csv.gz',index=False,compression="gzip")
        
    def invoke(self):
        return(Comb4EDperf(JobName=self.JobName,SoundFileRootDir_Host=self.SoundFileRootDir_Host,IDlength=self.IDlength,\
                   GTfile=self.GTfile,FGfile=self.FGfile,FileGroupID=self.FileGroupID,EDprocess=self.EDprocess,EDsplits=self.EDsplits,EDcpu=self.EDcpu,\
                   EDchunk=self.EDchunk,EDmethodID=self.EDmethodID,EDparamString=self.EDparamString,EDparamNames=self.EDparamNames,ALprocess=self.ALprocess,\
                   ALmethodID=self.ALmethodID,ALparamString=self.ALparamString,\
                   PE1process=self.PE1process,PE1methodID=self.PE1methodID,\
                   ProjectRoot=self.ProjectRoot,system=self.system,r_version=self.r_version))



if __name__ == '__main__':
    luigi.build([Comb4EDperf.invoke(C4EP_params)], local_scheduler=True)

