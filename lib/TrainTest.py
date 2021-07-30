import luigi
import os
import hashlib
import configparser
import pandas as pd
import sys
import numpy
import subprocess
import shlex
from supporting.instinct import *
from supporting.getParams import *
from supporting.Comb4EDperf_TT import *
from supporting.Comb4FeatureTrain import *
from supporting.Comb4PE2All import *
from supporting.job_fxns import * 

class TrainTest(Comb4PE2All,Comb4EDperf_TT,Comb4FeatureTrain,PerfEval2,RavenViewDETx):

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
        
        task6 = FormatFG.invoke(self,n=l,src="n_") #FormatFG(FGfile = self.n_FGfile[l],ProjectRoot=self.ProjectRoot)
        task7 = UnifyED.invoke(self,task6)
        task8 = UnifyFE.invoke(self,task7,task6)
        task9 = ApplyModel.invoke(self,task8,task3,task6)
        task10 = FormatGT.invoke(self,task6,n=l,src="n_")#FormatGT(upstream_task1=task6,uTask1path=task6.outpath(),GTfile=self.n_GTfile[l],ProjectRoot=self.ProjectRoot)
        task11 = AssignLabels.invoke(self,task9,task10,task6,AL_apply=self.AL_apply)

        task12 = ApplyCutoff.invoke(self,task11)
        task13 = AssignLabels.invoke(self,task12,task10,task6,AL_apply=self.AL_apply) #controls if different criteria is used in test- ie, png level vs det level. 
        task14 = PerfEval1_s1.invoke(self,task13,task6,task12,n=l,src="n_") 
        task15 = PerfEval2.invoke(self,task11,task1,"FG") #use the pre cutoff data

        task16 = RavenViewDETx.invoke(self,task13,task6,"T")
            
        return [task0,task1,task2,task3,task4,task5,task6,task7,task8,task9,task10,task11,task12,task13,task14,task15,task16]
    def outpath(self):
        if self.TT_WriteToOutputs=='y':
            return self.ProjectRoot +'Outputs/' + self.JobName + '/' + self.hashProcess()
        elif self.TT_WriteToOutputs=='n':
            return self.CacheRoot + 'Cache/' + self.hashProcess()
    def requires(self):
        for l in range(self.loopVar):
            tasks = self.pipelineMap(l)
            yield tasks[1]
            yield tasks[5]
            yield tasks[14]
            yield tasks[15]
            yield tasks[16]

    def output(self):
        yield luigi.LocalTarget(self.outpath() + '/FullStats.csv')
        yield luigi.LocalTarget(self.outpath() + '/RAVENx.txt')

    def run(self):

        #this is basically same as MPE, maybe generalize this somehow... 
        dataframes = [None] * self.loopVar
        for k in range(self.loopVar):
            tasks=self.pipelineMap(k)
            dataframes[k] = pd.read_csv(tasks[14].outpath() + '/Stats.csv.gz',compression='gzip')
        Modeleval = pd.concat(dataframes,ignore_index=True)

        resultCache= self.CacheRoot + 'Cache/' + self.hashProcess()
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

        argParse.run(Program='R',cmdType=self.system,ProjectRoot=self.ProjectRoot,ProcessID=self.PE1process,MethodID=self.PE1methodID,Paths=Paths,Args=Args,Params=Params)

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

        argParse.run(Program='R',cmdType=self.system,ProjectRoot=self.ProjectRoot,ProcessID=self.PRprocess,MethodID=self.PRmethodID,Paths=Paths,Args=Args,Params='')

        #load in the ravenx files, and rbind them in pandas. 

        RX_paths = [None] * self.loopVar
        for k in range(self.loopVar):
            tasks = self.pipelineMap(k)
            RX_dats[k] = pd.read_csv(tasks[16].outpath() + '/Ravenx.txt', delimiter = "\t")
            
        RX_dat = pd.concat(RX_dats,ignore_index=True)

        RX_dat.to_csv(resultCache + '/RAVENx.txt',index=False, sep='\t')
        RX_dat.to_csv(self.outpath() + '/RAVENx.txt',index=False, sep='\t')
        
    def invoke(obj):
        return(TrainTest(JobName=obj.JobName,ProjectRoot=obj.ProjectRoot,SoundFileRootDir_Host_Dec=obj.SoundFileRootDir_Host_Dec,\
                            IDlength=obj.IDlength,FGfile=obj.FGfile,FileGroupID=obj.FileGroupID,TT_WriteToOutputs=obj.TT_WriteToOutputs,\
                            GTfile=obj.GTfile,EDprocess=obj.EDprocess,EDsplits=obj.EDsplits,EDcpu=obj.EDcpu,\
                            EDchunk=obj.EDchunk,EDmethodID=obj.EDmethodID,EDparamString=obj.EDparamString,\
                            EDparamNames=obj.EDparamNames,ALprocess=obj.ALprocess,ALmethodID=obj.ALmethodID,\
                            ALparamString=obj.ALparamString,FEprocess=obj.FEprocess,FEmethodID=obj.FEmethodID,\
                            FEparamString=obj.FEparamString,FEparamNames=obj.FEparamNames,FEsplits=obj.FEsplits,\
                            FEcpu=obj.FEcpu,MFAprocess=obj.MFAprocess,MFAmethodID=obj.MFAmethodID,FGparamString=obj.FGparamString,\
                            TMprocess=obj.TMprocess,TMmethodID=obj.TMmethodID,TMparamString=obj.TMparamString,TMstage=obj.TMstage,\
                            TM_outName=obj.TM_outName,TMcpu=obj.TMcpu,ACcutoffString=obj.ACcutoffString,n_FileGroupID=obj.n_FileGroupID,\
                            PE1process=obj.PE1process,PE1methodID=obj.PE1methodID,PE2process=obj.PE2process,PE2methodID=obj.PE2methodID,\
                            PRprocess=obj.PRprocess,PRmethodID=obj.PRmethodID,loopVar=obj.n_IDlength,n_ALmethodID=obj.n_ALmethodID,\
                            n_ALparamString=obj.n_ALparamString,AL_apply=obj.AL_apply,\
                            FGmethodID=obj.FGmethodID,decimatedata = obj.decimatedata,SoundFileRootDir_Host_Raw=obj.SoundFileRootDir_Host_Raw,\
                            n_IDlength=obj.n_IDlength,n_FGfile=obj.n_FGfile,n_GTfile=obj.n_GTfile,system=obj.system,CacheRoot=obj.CacheRoot))
    def getParams(args):
        params = Load_Job('TrainTest',args)

        params = FG(params,'FormatFG')
        params = GT(params,'FormatGT')
        params = ED(params,'EventDetector')
        params = FE(params,'FeatureExtraction')
        params = AL(params,'AssignLabels')
        params = MFA(params,'MergeFE_AL')
        params = TM(params,'TrainModel','train')
        params = AC(params,'ApplyCutoff')
        params = PE1(params,'PerfEval1')
        params = PE2(params,'PerfEval2')
        params = PR(params,'PerformanceReport')

        #novel data params

        #FG for novel data
        n_params = Load_Job('TrainTest',args)
        n_params = FG(n_params,'FormatFGapply')
        n_params = GT(n_params,'FormatGTapply')

        if(params.AL_apply=='y'):
            n_params = AL(n_params,'FormatALapply')

        #only retain these ones. 
        params.n_FileGroupID = n_params.FileGroupID
        params.n_FGfile = n_params.FGfile
        params.n_IDlength = n_params.IDlength
        params.n_GTfile = n_params.GTfile

        if(params.AL_apply=='y'):
            params.n_ALmethodID = n_params.ALmethodID
            params.n_ALparamString = n_params.ALparamString
        else:
            params.n_ALmethodID = None
            params.n_ALparamString = None
            
        params.TT_WriteToOutputs = 'y'

        return params
    
if __name__ == '__main__':
    deployJob(TrainTest,sys.argv)

