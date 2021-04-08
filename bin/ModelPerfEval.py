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

#Performance evaluation from CV models,
#run PE2 stage 1 from instinct.py, and run PE2 stage 2 from def run(self):.
#home/daniel.woodrich/Projects/instinct_dt/
#C:/Apps/instinct_dt

MPE_params = MPE_Job('ModelPerfEval')

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

class ModelPerfEval(EDperfEval,TrainModel,SplitForPE,ApplyCutoff,PerfEval2):
    
    #macro job
    MPE_JobName=luigi.Parameter()
    MPE_WriteToOutputs = luigi.Parameter()

    #PR
    PRprocess=luigi.Parameter()
    PRmethodID=luigi.Parameter()

    #nullify some inherited parameters:
    PE2datType=None
    EDpe1_JobName=None

    #downstream default (required for this to work correctly, but changeable when running EDpe1 as a job. 
    EDpe1_WriteToOutputs='n'
    
    def pipelineMap(self,l): #here is where you define pipeline structure
        task0 = EDperfEval.invoke(self)
        task1 = TrainModel.invoke(self)
        task2 = PerfEval2.invoke(self,task1,task0,"All")
        task3 = FormatFG.invoke(self,l)#redundant but lets SFPE,AL,PE1 continue their path
        task4 = SplitForPE.invoke(self,task3,task1,l) 
        task5 = PerfEval2.invoke(self,task4,task0,"FG")
        task6 = ApplyCutoff.invoke(self,task4)
        task7 = FormatGT.invoke(self,l)
        task8 = AssignLabels.invoke(self,task6,task7,task3)
        task9 = PerfEval1.invoke(self,task8,task3,n=l)

        return [task0,task1,task2,task3,task4,task5,task6,task7,task8,task9]
    def hashProcess(self):
        hashStrings = [None] * self.IDlength
        for l in range(self.IDlength):
            tasks = self.pipelineMap(l)
            hashStrings[l] = ' '.join([tasks[0].hashProcess(),tasks[1].hashProcess(),tasks[2].hashProcess(),tasks[3].hashProcess(),
                                       tasks[4].hashProcess(),tasks[5].hashProcess(),tasks[6].hashProcess(),tasks[7].hashProcess(),
                                       tasks[8].hashProcess(),tasks[9].hashProcess()]) #maybe a more general way to do this? 
    
        MPE_JobHash = Helper.getParamHash2(self.PRmethodID + ' ' + ' '.join(hashStrings),12)
        return(MPE_JobHash)
    def outpath(self):
        if self.MPE_WriteToOutputs=='y':
            return self.ProjectRoot +'Outputs/' + self.MPE_JobName + '/' + self.hashProcess()
        elif self.MPE_WriteToOutputs=='n':
            return self.ProjectRoot + 'Cache/' + self.hashProcess()
    def requires(self):
        for l in range(self.IDlength):
            tasks = self.pipelineMap(l)
            yield tasks[2] #this yield feels wasteful, but no good way around it
            yield tasks[5]
            yield tasks[9]
    def output(self):
        #this is full performance report
        #return luigi.LocalTarget(OutputsRoot + self.JobName + '/' + self.JobHash + '/RFmodel.rds')
        return luigi.LocalTarget(self.outpath() + '/FullStats.csv')
    def run(self):
        #this is a mega job. Do PervEval1 stage 2 here (similar to run section of EDperfeval). Collect all artifacts and summarize into report: for now, this is going to be a static printout from R.
        #Don't worry about adding map yet, but when want to do that should draw lat long from FG.
        #if really tricking this section out, keep in python and publish web dashboard with python dash module. 
        
         #concatenate outputs and summarize

        dataframes = [None] * self.IDlength
        for k in range(self.IDlength):
            tasks = self.pipelineMap(k)
            dataframes[k] = pd.read_csv(tasks[9].outpath() + '/Stats.csv.gz',compression='gzip')
        Modeleval = pd.concat(dataframes,ignore_index=True)

        #trying to get resultPath formated like normal, messed up paths a little bit need to fix!!! Pick up here. 
        resultCache= self.ProjectRoot + 'Cache/' + self.hashProcess()
        if not os.path.exists(resultCache):
            os.mkdir(resultCache)

        Modeleval.to_csv(resultCache + '/Stats_Intermediate.csv',index=False)
        #send back in to PE1

        FGpath = 'NULL'
        LABpath = 'NULL'
        INTpath = resultCache + '/Stats_Intermediate.csv'
        resultPath2 =  resultCache + '/Stats.csv.gz'
        FGID = 'NULL'

        Paths = [FGpath,LABpath,INTpath,resultPath2]
        Args = [FGID,'2']
        Params = ''

        argParse.run(Program='R',rVers=self.r_version,cmdType=self.system,ProjectRoot=self.ProjectRoot,ProcessID=self.PE1process,MethodID=self.PE1methodID,Paths=Paths,Args=Args,Params=Params)

        os.remove(resultCache + '/Stats_Intermediate.csv')

        #now send the paths for all of the artifacts into the performance report R script.

        #list of paths to collect:
            #for each FG (n paths, comma seperated and parse in R) 
                #PRcurve
                #PRauc
        #EDperfeval
        #Modelperfeval
        #full model PR curve
        #full model AUC

        EDstatPath= tasks[0].outpath() #reads off the last loop from earlier, doesn't matter as these don't change per loop 
        MDstatPath= self.ProjectRoot + 'Cache/' + self.hashProcess()
        MDvisPath= tasks[2].outpath()
        
        FGvis_paths = [None] * self.IDlength
        for k in range(self.IDlength):
            tasks = self.pipelineMap(k)
            FGvis_paths[k] = tasks[5].outpath()
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
        return(ModelPerfEval(MPE_JobName=obj.MPE_JobName,MPE_WriteToOutputs=obj.MPE_WriteToOutputs,SoundFileRootDir_Host=obj.SoundFileRootDir_Host,\
                             IDlength=obj.IDlength,FGfile=obj.FGfile,FileGroupID=obj.FileGroupID,GTfile=obj.GTfile,EDprocess=obj.EDprocess,\
                             EDsplits=obj.EDsplits,EDcpu=obj.EDcpu,EDchunk=obj.EDchunk,EDmethodID=obj.EDmethodID,EDparamString=obj.EDparamString,\
                             EDparamNames=obj.EDparamNames,ALprocess=obj.ALprocess,ALmethodID=obj.ALmethodID,ALparamString=obj.ALparamString,\
                             FEprocess=obj.FEprocess,FEmethodID=obj.FEmethodID,FEparamString=obj.FEparamString,FEparamNames=obj.FEparamNames,\
                             FEsplits=obj.FEsplits,FEcpu=obj.FEcpu,MFAprocess=obj.MFAprocess,MFAmethodID=obj.MFAmethodID,TMprocess=obj.TMprocess,\
                             TMmethodID=obj.TMmethodID,TMparamString=obj.TMparamString,TMstage=obj.TMstage,TM_outName=obj.TM_outName,\
                             TMcpu=obj.TMcpu,PE1process=obj.PE1process,PE1methodID=obj.PE1methodID,PE2process=obj.PE2process,PE2methodID=obj.PE2methodID,\
                             ACcutoffString=obj.ACcutoffString,PRprocess=obj.PRprocess,PRmethodID=obj.PRmethodID,ProjectRoot=obj.ProjectRoot,system=obj.system,\
                             r_version=obj.r_version))
    
if __name__ == '__main__':
    luigi.build([ModelPerfEval.invoke(MPE_params)], local_scheduler=True)    

