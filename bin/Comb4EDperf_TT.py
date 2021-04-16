from instinct import *
from Comb4EDperf import * 

#not to run alone just with TT

#combine PE1_S1 into csv, run PE2 on them, put stats.csv and outputs into one folder 

class Comb4EDperf_TT(Comb4Standard,FormatFG,FormatGT,UnifyED,AssignLabels,PerfEval1_s1):

    JobName=luigi.Parameter()
    #nullify some inherited parameters:
    upstream_task1=None
    upstream_task2=None
    upstream_task3=None
    uTask1path=None
    uTask2path=None
    uTask3path=None

    FGfile=None
    GTfile = None
    FileGroupID= None

    fileName = 'Stats.csv.gz'
    
    #new params
    n_IDlength=luigi.IntParameter()
    n_FGfile=luigi.Parameter()
    n_GTfile = luigi.Parameter()
    n_FileGroupID= luigi.Parameter()

    def pipelineMap(self,l): #here is where you define pipeline structure
        task0 = FormatFG(FGfile = self.n_FGfile[l],ProjectRoot=self.ProjectRoot)
        task1 = FormatGT(upstream_task1=task0,uTask1path=task0.outpath(),GTfile=self.n_GTfile[l],ProjectRoot=self.ProjectRoot)
        task2 = UnifyED.invoke(self,task0)
        task3 = AssignLabels.invoke(self,task2,task1,task0)
        task4 = PerfEval1_s1.invoke(self,task3,task0,task3,n=l,src="n_")
        return [task0,task1,task2,task3,task4]
    def invoke(self):
        return(Comb4EDperf_TT(JobName=self.JobName,SoundFileRootDir_Host=self.SoundFileRootDir_Host,n_IDlength=self.n_IDlength,\
                   n_GTfile=self.n_GTfile,n_FGfile=self.n_FGfile,n_FileGroupID=self.n_FileGroupID,EDprocess=self.EDprocess,EDsplits=self.EDsplits,EDcpu=self.EDcpu,\
                   EDchunk=self.EDchunk,EDmethodID=self.EDmethodID,EDparamString=self.EDparamString,EDparamNames=self.EDparamNames,ALprocess=self.ALprocess,\
                   ALmethodID=self.ALmethodID,ALparamString=self.ALparamString,loopVar = self.n_IDlength,\
                   PE1process=self.PE1process,PE1methodID=self.PE1methodID,\
                   ProjectRoot=self.ProjectRoot,system=self.system,r_version=self.r_version))

