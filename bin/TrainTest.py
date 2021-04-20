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
from Comb4EDperf_TT import *
from Comb4FeatureTrain import *
from Comb4PE2All import *

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
TT_params = PR(TT_params,'PerformanceReport')

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

TT_params.TT_WriteToOutputs = 'y'

class TrainTest(Comb4PE2All,Comb4EDperf_TT,Comb4FeatureTrain,PerfEval2):

    JobName=luigi.Parameter()
    TT_WriteToOutputs = luigi.Parameter()
    #nullify some inherited parameters:

    #PR
    PRprocess=luigi.Parameter()
    PRmethodID=luigi.Parameter()

    #nullify some inherited parameters:
    PE2datType=None

    #un-nullify some inherited parameters
    IDlength=luigi.IntParameter()
    FGfile=luigi.Parameter()
    GTfile =luigi.Parameter()
    FileGroupID=luigi.Parameter()


    def pipelineMap(self,l):

        #this does perf eval on the n_ data 
        task0 = Comb4EDperf_TT.invoke(self)
        task1 = PerfEval1_s2.invoke(self,task0)
        
        task2 = Comb4FeatureTrain.invoke(self)
        task3 = TrainModel.invoke(self,task2)

        task4 = Comb4PE2All.invoke(self)
        task5 = PerfEval2.invoke(self,task4,task1,"All")
        
        task6 = FormatFG(FGfile = self.n_FGfile[l],ProjectRoot=self.ProjectRoot)
        task7 = UnifyED.invoke(self,task6)
        task8 = UnifyFE.invoke(self,task7,task6)
        task9 = ApplyModel.invoke(self,task8,task3,task6)
        task10 = FormatGT(upstream_task1=task6,uTask1path=task6.outpath(),GTfile=self.n_GTfile[l],ProjectRoot=self.ProjectRoot)
        task11 = AssignLabels.invoke(self,task9,task10,task6)
        task12 = ApplyCutoff.invoke(self,task11)
        task13 = AssignLabels.invoke(self,task12,task10,task6)
        task14 = PerfEval1_s1.invoke(self,task13,task6,task12,n=l,src="n_") 
        task15 = PerfEval2.invoke(self,task11,task1,"FG") #use the pre cutoff data
            
        return [task0,task1,task2,task3,task4,task5,task6,task7,task8,task9,task10,task11,task12,task13,task14,task15]
    def outpath(self):
        if self.TT_WriteToOutputs=='y':
            return self.ProjectRoot +'Outputs/' + self.JobName + '/' + self.hashProcess()
        elif self.TT_WriteToOutputs=='n':
            return self.ProjectRoot + 'Cache/' + self.hashProcess()
    def requires(self):
        for l in range(self.loopVar):
            tasks = self.pipelineMap(l)
            yield tasks[1]
            yield tasks[5]
            yield tasks[14]
            yield tasks[15]
    def output(self):
        return luigi.LocalTarget(self.outpath() + '/FullStats.csv')
    def run(self):

        #this is basically same as MPE, maybe generalize this somehow... 
        dataframes = [None] * self.loopVar
        for k in range(self.loopVar):
            tasks=self.pipelineMap(k)
            dataframes[k] = pd.read_csv(tasks[14].outpath() + '/Stats.csv.gz',compression='gzip')
        Modeleval = pd.concat(dataframes,ignore_index=True)

        resultCache= self.ProjectRoot + 'Cache/' + self.hashProcess()
        if not os.path.exists(resultCache):
            os.mkdir(resultCache)

        Modeleval.to_csv(resultCache + '/Stats.csv.gz',index=False)

        if self.TT_WriteToOutputs=='y':
            if not os.path.exists(self.ProjectRoot +'Outputs/' + self.JobName):
                os.mkdir(self.ProjectRoot +'Outputs/' + self.JobName)
                    

        resultPath = self.outpath()

        if not os.path.exists(resultPath):
            os.mkdir(resultPath)

        FGpath = 'NULL'
        LABpath = 'NULL'
        INTpath = resultCache + '/Stats.csv.gz'
        resultPath2 =  resultPath + '/Stats.csv.gz'
        FGID = 'NULL'

        Paths = [FGpath,LABpath,INTpath,resultPath2]
        Args = [FGID,'All']
        Params = ''

        argParse.run(Program='R',rVers=self.r_version,cmdType=self.system,ProjectRoot=self.ProjectRoot,ProcessID=self.PE1process,MethodID=self.PE1methodID,Paths=Paths,Args=Args,Params=Params)

        EDstatPath= tasks[1].outpath() #reads off the last loop from earlier, doesn't matter as these don't change per loop 
        MDstatPath= self.outpath()
        MDvisPath= tasks[5].outpath()
        
        FGvis_paths = [None] * self.loopVar
        for k in range(self.loopVar):
            tasks = self.pipelineMap(k)
            FGvis_paths[k] = tasks[15].outpath()
        FGvis_paths = ','.join(FGvis_paths)
        FGIDs=','.join(self.n_FileGroupID)

        resultPath=self.outpath()

        if not os.path.exists(resultPath):
            os.mkdir(resultPath)

        Paths = [EDstatPath,MDstatPath,MDvisPath,resultPath]
        Args = [FGvis_paths,FGIDs]

        argParse.run(Program='R',rVers=self.r_version,cmdType=self.system,ProjectRoot=self.ProjectRoot,ProcessID=self.PRprocess,MethodID=self.PRmethodID,Paths=Paths,Args=Args,Params='')
        
    def invoke(obj):
        return(TrainTest(JobName=obj.JobName,ProjectRoot=obj.ProjectRoot,SoundFileRootDir_Host_Dec=obj.SoundFileRootDir_Host_Dec,\
                            IDlength=obj.IDlength,FGfile=obj.FGfile,FileGroupID=obj.FileGroupID,TT_WriteToOutputs=obj.TT_WriteToOutputs,\
                            GTfile=obj.GTfile,EDprocess=obj.EDprocess,EDsplits=obj.EDsplits,EDcpu=obj.EDcpu,\
                            EDchunk=obj.EDchunk,EDmethodID=obj.EDmethodID,EDparamString=obj.EDparamString,\
                            EDparamNames=obj.EDparamNames,ALprocess=obj.ALprocess,ALmethodID=obj.ALmethodID,\
                            ALparamString=obj.ALparamString,FEprocess=obj.FEprocess,FEmethodID=obj.FEmethodID,\
                            FEparamString=obj.FEparamString,FEparamNames=obj.FEparamNames,FEsplits=obj.FEsplits,\
                            FEcpu=obj.FEcpu,MFAprocess=obj.MFAprocess,MFAmethodID=obj.MFAmethodID,\
                            TMprocess=obj.TMprocess,TMmethodID=obj.TMmethodID,TMparamString=obj.TMparamString,TMstage=obj.TMstage,\
                            TM_outName=obj.TM_outName,TMcpu=obj.TMcpu,ACcutoffString=obj.ACcutoffString,n_FileGroupID=obj.n_FileGroupID,\
                            PE1process=obj.PE1process,PE1methodID=obj.PE1methodID,PE2process=obj.PE2process,PE2methodID=obj.PE2methodID,\
                            PRprocess=obj.PRprocess,PRmethodID=obj.PRmethodID,loopVar=obj.n_IDlength,\
                            FGmethodID=obj.FGmethodID,decimatedata = obj.decimatedata,SoundFileRootDir_Host_Raw=obj.SoundFileRootDir_Host_Raw,\
                            n_IDlength=obj.n_IDlength,n_FGfile=obj.n_FGfile,n_GTfile=obj.n_GTfile,system=obj.system,r_version=obj.r_version))


if __name__ == '__main__':
    luigi.build([TrainTest.invoke(TT_params)], local_scheduler=True)

