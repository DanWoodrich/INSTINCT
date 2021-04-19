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
from Comb4EDperf import *
from Comb4FeatureTrain import *
from getParams import * 

#Performance evaluation from CV models,
#run PE2 stage 1 from instinct.py, and run PE2 stage 2 from def run(self):.
#home/daniel.woodrich/Projects/instinct_dt/
#C:/Apps/instinct_dt

MPE_params = Load_Job('ModelPerfEval')

#add to MPE

MPE_params = FG(MPE_params,'FormatFG')
MPE_params = GT(MPE_params,'FormatGT')
MPE_params = ED(MPE_params,'EventDetector')
MPE_params = FE(MPE_params,'FeatureExtraction')
MPE_params = AL(MPE_params,'AssignLabels')
MPE_params = PE1(MPE_params,'PerfEval1')
MPE_params = MFA(MPE_params,'MergeFE_AL')
MPE_params = PE2(MPE_params,'PerfEval2')
MPE_params = TM(MPE_params,'TrainModel','CV')
MPE_params = AC(MPE_params,'ApplyCutoff')
MPE_params = PR(MPE_params,'PerformanceReport')

MPE_params.MPE_WriteToOutputs = 'y'

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
        task5 = FormatFG.invoke(self,l)#redundant but lets SFPE,AL,PE1 continue their path
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
            return self.ProjectRoot + 'Cache/' + self.hashProcess()
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
        resultCache= self.ProjectRoot + 'Cache/' + self.hashProcess()
        if not os.path.exists(resultCache):
            os.mkdir(resultCache)

        Modeleval.to_csv(resultCache + '/Stats.csv.gz',index=False)
        #send back in to PE1

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

        argParse.run(Program='R',rVers=self.r_version,cmdType=self.system,ProjectRoot=self.ProjectRoot,ProcessID=self.PE1process,MethodID=self.PE1methodID,Paths=Paths,Args=Args,Params=Params)

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

        argParse.run(Program='R',rVers=self.r_version,cmdType=self.system,ProjectRoot=self.ProjectRoot,ProcessID=self.PRprocess,MethodID=self.PRmethodID,Paths=Paths,Args=Args,Params='')
    def invoke(obj):
        return(ModelPerfEval(JobName=obj.JobName,MPE_WriteToOutputs=obj.MPE_WriteToOutputs,SoundFileRootDir_Host=obj.SoundFileRootDir_Host,\
                             IDlength=obj.IDlength,FGfile=obj.FGfile,FileGroupID=obj.FileGroupID,GTfile=obj.GTfile,EDprocess=obj.EDprocess,\
                             EDsplits=obj.EDsplits,EDcpu=obj.EDcpu,EDchunk=obj.EDchunk,EDmethodID=obj.EDmethodID,EDparamString=obj.EDparamString,\
                             EDparamNames=obj.EDparamNames,ALprocess=obj.ALprocess,ALmethodID=obj.ALmethodID,ALparamString=obj.ALparamString,\
                             FEprocess=obj.FEprocess,FEmethodID=obj.FEmethodID,FEparamString=obj.FEparamString,FEparamNames=obj.FEparamNames,\
                             FEsplits=obj.FEsplits,FEcpu=obj.FEcpu,MFAprocess=obj.MFAprocess,MFAmethodID=obj.MFAmethodID,TMprocess=obj.TMprocess,\
                             TMmethodID=obj.TMmethodID,TMparamString=obj.TMparamString,TMstage=obj.TMstage,TM_outName=obj.TM_outName,\
                             TMcpu=obj.TMcpu,PE1process=obj.PE1process,PE1methodID=obj.PE1methodID,PE2process=obj.PE2process,PE2methodID=obj.PE2methodID,\
                             ACcutoffString=obj.ACcutoffString,PRprocess=obj.PRprocess,PRmethodID=obj.PRmethodID,ProjectRoot=obj.ProjectRoot,system=obj.system,\
                             r_version=obj.r_version,loopVar = obj.IDlength,decimatedata = obj.decimatedata))
    
if __name__ == '__main__':
    luigi.build([ModelPerfEval.invoke(MPE_params)], local_scheduler=True)    

