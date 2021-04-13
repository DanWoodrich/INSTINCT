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
from runFullNovel import *
from getParams import *

#Run a model on data that does not need to have labels. 

TT_params = Load_Job('TrainTest')

TT_params = FG(TT_params,'FormatFG')
TT_params = GT(TT_params,'FormatGT')
TT_params = ED(TT_params,'EventDetector')
TT_params = FE(TT_params,'FeatureExtraction')
TT_params = AL(TT_params,'AssignLabels')
TT_params = MFA(TT_params,'MergeFE_AL')
TT_params = TM(TT_params,'TrainModel','train')
TT_params = AC(TT_params,'ApplyCutoff')
TT_params = PE1(TT_params,'PerfEval1')
TT_params = PE2(TT_params,'PerfEval2')


#novel data params

#FG for novel data
n_TT_params = Load_Job('TrainTest')
n_FGparams = FG(n_TT_params,'FormatFGapply')

n_FGparams = GT(n_FGparams,'FormatGTapply')

#only retain these ones. 
TT_params.n_FileGroupID = n_FGparams.FileGroupID
TT_params.n_FGfile = n_FGparams.FGfile
TT_params.n_IDlength = n_FGparams.IDlength
TT_params.n_GTfile = n_FGparams.GTfile

class TrainTest(Comb4EDperf_TT,Comb4FeatureTrain,ApplyCutoff,TrainModel,PerfEval2):

    JobName=luigi.Parameter()
    #nullify some inherited parameters:

    def pipelineMap(self):

            #this does perf eval on the n_ data 
            task0 = Comb4EDperf_TT.invoke(self)
            task1 = PerfEval1_s2.invoke(self,task0)
            
            task2 = Comb4FeatureTrain.invoke(self)
            task3 = TrainModel.invoke(self,task2)

            #jesus, my brain is broken. Need to do an ED perf eval 1st, that gives us the adjustment factor, not the cutoff.
            
            task4 = FormatFG(FGfile = self.n_FGfile[0],ProjectRoot=self.ProjectRoot)
            task5 = UnifyED.invoke(self,task4)
            task6 = UnifyFE.invoke(self,task5,task4)
            task7 = ApplyModel.invoke(self,task6,task3,task4)
            task8 = FormatGT(upstream_task1=task4,uTask1path=task2.outpath(),GTfile=self.n_GTfile[0],ProjectRoot=self.ProjectRoot)
            task9 = AssignLabels.invoke(self,task7,task8,task4)
            task10 = ApplyCutoff.invoke(self,task8)
            task11 = AssignLabels.invoke(self,task10,task8,task4)
            task12 = PerfEval1_s1.invoke(self,task11,task4,task10,n=0)  #This is gonna read the wrong file group...
            task13 = PerfEval2.invoke(self,task9,task1,"FG") #use the pre cutoff data
            
        return [task0,task1,task2,task3,task4,task5,task6,task7,task8,task9,task10]
    def hashProcess(self):
        hashStrings = [None] * self.n_IDlength
        for l in range(self.IDlength):
            tasks = self.pipelineMap(l)
            hashStrings[l] = ' '.join([tasks[0].hashProcess(),tasks[1].hashProcess(),tasks[2].hashProcess(),tasks[3].hashProcess(),
                                       tasks[4].hashProcess(),tasks[5].hashProcess(),tasks[6].hashProcess(),tasks[7].hashProcess(),
                                       tasks[8].hashProcess(),tasks[9].hashProcess(),tasks[10].hashProcess(),tasks[11].hashProcess(),
                                       tasks[12].hashProcess(),tasks[13].hashProcess()]) #maybe a more general way to do this? 
    
        MPE_JobHash = Helper.getParamHash2(' '.join(hashStrings),12)
        return(MPE_JobHash)
    def outpath(self):
        if self.MPE_WriteToOutputs=='y':
            return self.ProjectRoot +'Outputs/' + self.JobName + '/' + self.hashProcess()
        elif self.MPE_WriteToOutputs=='n':
            return self.ProjectRoot + 'Cache/' + self.hashProcess()
    def requires(self):
        tasks=self.pipelineMap()
        yield tasks[4]
        yield tasks[12]
        yield tasks[13]
    def output(self):
        return None
    def run(self):
        
        for k in range(self.IDlength):
            tasks=self.pipelineMap(k)
            dataframes[k] = pd.read_csv(tasks[9].outpath() + '/Stats.csv.gz',compression='gzip')
        EDeval = pd.concat(dataframes,ignore_index=True)

        resultPath = self.outpath()

        if not os.path.exists(resultPath):
            os.mkdir(resultPath)

        EDeval.to_csv(resultPath + '/Stats.csv.gz',index=False,compression="gzip")

        #combine AL for PE2 'all'

        dataframes = [None] * self.IDlength
        for k in range(self.IDlength):
            tasks=self.pipelineMap(k)
            dataframes[k] = pd.read_csv(tasks[9].outpath() + '/DETx.csv.gz',compression='gzip')
        AllDat = pd.concat(dataframes,ignore_index=True)

        AllDat.to_csv(resultPath + '/DETx.csv.gz',index=False,compression="gzip")

        #can copypaste, or run as luigi task? Hard to do since we shortcut the combining path here, not a real task.
        #the right way to do this may be to make a wrapper job to combine these, but a lot of work for now
        #PerfEval2.invoke(self,tasks[],task1,"FG")

        StatsPath = tasks[1].outpath()
        resultPath = resultPath

        DETpath = resultPath + '/DETx.csv.gz'

        if not os.path.exists(resultPath):
            os.mkdir(resultPath)

        Paths = [DETpath,resultPath,StatsPath,self.PE2datType]

        argParse.run(Program='R',rVers=self.r_version,cmdType=self.system,ProjectRoot=self.ProjectRoot,ProcessID=self.PE2process,MethodID=self.PE2methodID,Paths=Paths,Args='',Params='')

        #test out the above when I get back
        
    def invoke(obj):
        return(TrainTest(JobName=obj.JobName,ProjectRoot=obj.ProjectRoot,SoundFileRootDir_Host=obj.SoundFileRootDir_Host,\
                            IDlength=obj.IDlength,FGfile=obj.FGfile,FileGroupID=obj.FileGroupID,\
                            GTfile=obj.GTfile,EDprocess=obj.EDprocess,EDsplits=obj.EDsplits,EDcpu=obj.EDcpu,\
                            EDchunk=obj.EDchunk,EDmethodID=obj.EDmethodID,EDparamString=obj.EDparamString,\
                            EDparamNames=obj.EDparamNames,ALprocess=obj.ALprocess,ALmethodID=obj.ALmethodID,\
                            ALparamString=obj.ALparamString,FEprocess=obj.FEprocess,FEmethodID=obj.FEmethodID,\
                            FEparamString=obj.FEparamString,FEparamNames=obj.FEparamNames,FEsplits=obj.FEsplits,\
                            FEcpu=obj.FEcpu,MFAprocess=obj.MFAprocess,MFAmethodID=obj.MFAmethodID,\
                            TMprocess=obj.TMprocess,TMmethodID=obj.TMmethodID,TMparamString=obj.TMparamString,TMstage=obj.TMstage,\
                            TM_outName=obj.TM_outName,TMcpu=obj.TMcpu,ACcutoffString=obj.ACcutoffString,n_FileGroupID=obj.n_FileGroupID,\
                            PE1process=obj.PE1process,PE1methodID=obj.PE1methodID,PE2process=obj.PE2process,PE2methodID=obj.PE2methodID,\
                            n_IDlength=obj.n_IDlength,n_FGfile=obj.n_FGfile,n_GTfile=obj.n_GTfile,system=obj.system,r_version=obj.r_version))


if __name__ == '__main__':
    luigi.build([TrainTest.invoke(TT_params)], local_scheduler=True)

