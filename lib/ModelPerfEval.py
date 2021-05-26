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
from supporting.Comb4EDperf import *
from supporting.Comb4FeatureTrain import *
from supporting.getParams import *
from supporting.job_fxns import * 

class ModelPerfEval(Comb4EDperf,PerfEval1_s2,Comb4FeatureTrain,TrainModel,SplitForPE,ApplyCutoff,PerfEval2):
    
    JobName=luigi.Parameter()
    MPE_WriteToOutputs = luigi.Parameter()

    #PR
    PRprocess=luigi.Parameter()
    PRmethodID=luigi.Parameter()

    #nullify some inherited parameters:
    PE2datType=None
    
    def pipelineMap(self,l): #here is where you define pipeline structure
        task0 = Comb4EDperf.invoke(self)
        task1 = PerfEval1_s2.invoke(self,task0)
        
        task2 = Comb4FeatureTrain.invoke(self)
        task3 = TrainModel.invoke(self,task2)
        
        task4 = PerfEval2.invoke(self,task3,task1,"All")
        task5 = FormatFG.invoke(self,n=l)#redundant but lets SFPE,AL,PE1 continue their path
        task6 = SplitForPE.invoke(self,task5,task3,l)
        task7 = PerfEval2.invoke(self,task6,task1,"FG")
        task8 = ApplyCutoff.invoke(self,task6)
        task9 = FormatGT.invoke(self,task5,n=l)
        task10 = AssignLabels.invoke(self,task8,task9,task5)
        task11 = PerfEval1_s1.invoke(self,task10,task5,task8,n=l,src="GT")

        return [task0,task1,task2,task3,task4,task5,task6,task7,task8,task9,task10,task11]
    def outpath(self):
        if self.MPE_WriteToOutputs=='y':
            return self.ProjectRoot +'Outputs/' + self.JobName + '/' + self.hashProcess()
        elif self.MPE_WriteToOutputs=='n':
            return self.CacheRoot + 'Cache/' + self.hashProcess()
    def hashProcess(self):
        hashStrings = [None] * self.loopVar
        for l in range(self.loopVar):
            tasks = self.pipelineMap(l)
            taskStr = []
            for f in range(len(tasks)):
                taskStr.extend([tasks[f].hashProcess()])
            
            hashStrings[l] = ' '.join(taskStr)
            
        return Helper.getParamHash2(self.PRmethodID + ' ' + ' '.join(hashStrings),6)
    def requires(self):
        for l in range(self.loopVar):
            tasks = self.pipelineMap(l)
            yield tasks[4] #this yield feels wasteful, but no good way around it
            yield tasks[7]
            yield tasks[11]
    def output(self):
        #this is full performance report
        #return luigi.LocalTarget(OutputsRoot + self.JobName + '/' + self.JobHash + '/RFmodel.rds')
        return luigi.LocalTarget(self.outpath() + '/FullStats.csv')
    def run(self):
        #this is a mega job. Do PervEval1 stage 2 here (similar to run section of EDperfeval). Collect all artifacts and summarize into report: for now, this is going to be a static printout from R.
        #Don't worry about adding map yet, but when want to do that should draw lat long from FG.
        #if really tricking this section out, keep in python and publish web dashboard with python dash module. 
        
         #concatenate outputs and summarize

        dataframes = [None] * self.loopVar
        for k in range(self.loopVar):
            tasks = self.pipelineMap(k)
            dataframes[k] = pd.read_csv(tasks[11].outpath() + '/Stats.csv.gz',compression='gzip')
        Modeleval = pd.concat(dataframes,ignore_index=True)

        #trying to get resultPath formated like normal, messed up paths a little bit need to fix!!! Pick up here. 
        resultCache= self.CacheRoot + 'Cache/' + self.hashProcess()
        if not os.path.exists(resultCache):
            os.mkdir(resultCache)

        Modeleval.to_csv(resultCache + '/Stats.csv.gz',index=False)
        #send back in to PE1

        if self.MPE_WriteToOutputs=='y':
            if not os.path.exists(self.ProjectRoot +'Outputs/' + self.JobName):
                os.mkdir(self.ProjectRoot +'Outputs/' + self.JobName)

        if not os.path.exists(self.outpath()):
            os.mkdir(self.outpath())

        FGpath = 'NULL'
        LABpath = 'NULL'
        INTpath = resultCache + '/Stats.csv.gz'
        resultPath2 =  self.outpath() + '/Stats.csv.gz'
        FGID = 'NULL'

        Paths = [FGpath,LABpath,INTpath,resultPath2]
        Args = [FGID,'All']
        Params = ''

        argParse.run(Program='R',cmdType=self.system,ProjectRoot=self.ProjectRoot,ProcessID=self.PE1process,MethodID=self.PE1methodID,Paths=Paths,Args=Args,Params=Params)

        #now send the paths for all of the artifacts into the performance report R script.

        #list of paths to collect:
            #for each FG (n paths, comma seperated and parse in R) 
                #PRcurve
                #PRauc
        #EDperfeval
        #Modelperfeval
        #full model PR curve
        #full model AUC

        EDstatPath= tasks[1].outpath() #reads off the last loop from earlier, doesn't matter as these don't change per loop 
        MDstatPath= self.outpath()
        MDvisPath= tasks[4].outpath()
        
        FGvis_paths = [None] * self.loopVar
        for k in range(self.loopVar):
            tasks = self.pipelineMap(k)
            FGvis_paths[k] = tasks[7].outpath()
        FGvis_paths = ','.join(FGvis_paths)
        FGIDs=','.join(self.FileGroupID)

        resultPath=self.outpath()

        if not os.path.exists(self.outpath()):
            os.mkdir(self.outpath())
                    
        if not os.path.exists(resultPath):
            os.mkdir(resultPath)

        Paths = [EDstatPath,MDstatPath,MDvisPath,resultPath]
        Args = [FGvis_paths,FGIDs]

        argParse.run(Program='R',cmdType=self.system,ProjectRoot=self.ProjectRoot,ProcessID=self.PRprocess,MethodID=self.PRmethodID,Paths=Paths,Args=Args,Params='')
    def invoke(obj):
        return(ModelPerfEval(JobName=obj.JobName,MPE_WriteToOutputs=obj.MPE_WriteToOutputs,SoundFileRootDir_Host_Dec=obj.SoundFileRootDir_Host_Dec,\
                             IDlength=obj.IDlength,FGfile=obj.FGfile,FileGroupID=obj.FileGroupID,GTfile=obj.GTfile,EDprocess=obj.EDprocess,\
                             EDsplits=obj.EDsplits,EDcpu=obj.EDcpu,EDchunk=obj.EDchunk,EDmethodID=obj.EDmethodID,EDparamString=obj.EDparamString,\
                             EDparamNames=obj.EDparamNames,ALprocess=obj.ALprocess,ALmethodID=obj.ALmethodID,ALparamString=obj.ALparamString,\
                             FEprocess=obj.FEprocess,FEmethodID=obj.FEmethodID,FEparamString=obj.FEparamString,FEparamNames=obj.FEparamNames,\
                             FEsplits=obj.FEsplits,FEcpu=obj.FEcpu,MFAprocess=obj.MFAprocess,MFAmethodID=obj.MFAmethodID,TMprocess=obj.TMprocess,\
                             TMmethodID=obj.TMmethodID,TMparamString=obj.TMparamString,TMstage=obj.TMstage,TM_outName=obj.TM_outName,FGparamString=obj.FGparamString,\
                             FGmethodID=obj.FGmethodID,decimatedata = obj.decimatedata,SoundFileRootDir_Host_Raw=obj.SoundFileRootDir_Host_Raw,\
                             TMcpu=obj.TMcpu,PE1process=obj.PE1process,PE1methodID=obj.PE1methodID,PE2process=obj.PE2process,PE2methodID=obj.PE2methodID,\
                             ACcutoffString=obj.ACcutoffString,PRprocess=obj.PRprocess,PRmethodID=obj.PRmethodID,ProjectRoot=obj.ProjectRoot,system=obj.system,\
                             CacheRoot=obj.CacheRoot,loopVar = obj.IDlength))
    def getParams(args):    
        params = Load_Job('ModelPerfEval',args)

        params = FG(params,'FormatFG')
        params = GT(params,'FormatGT')
        params = ED(params,'EventDetector')
        params = FE(params,'FeatureExtraction')
        params = AL(params,'AssignLabels')
        params = PE1(params,'PerfEval1')
        params = MFA(params,'MergeFE_AL')
        params = PE2(params,'PerfEval2')
        params = TM(params,'TrainModel','CV')
        params = AC(params,'ApplyCutoff')
        params = PR(params,'PerformanceReport')

        params.MPE_WriteToOutputs = 'y'

        return params

if __name__ == '__main__':
    deployJob(ModelPerfEval,sys.argv)

