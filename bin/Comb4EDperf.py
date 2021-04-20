from instinct import *
#Ready a bunch of FGs for shared comparison with PE1 pt 2. 

class Comb4EDperf(Comb4Standard,FormatFG,FormatGT,UnifyED,AssignLabels,PerfEval1_s1):
    
    JobName=luigi.Parameter()
    IDlength = luigi.IntParameter()

    #nullify some inherited parameters:
    upstream_task1=None
    upstream_task2=None
    upstream_task3=None
    uTask1path=None
    uTask2path=None
    uTask3path=None

    fileName = 'Stats.csv.gz'

    def pipelineMap(self,l): #here is where you define pipeline structure 
        task0 = FormatFG.invoke(self,n=l) 
        task1 = FormatGT.invoke(self,task0,n=l)
        task2 = UnifyED.invoke(self,task0)
        task3 = AssignLabels.invoke(self,task2,task1,task0)
        task4 = PerfEval1_s1.invoke(self,task3,task0,task3,n=l,src="GT")
        return [task0,task1,task2,task3,task4]     
    def invoke(self):
        return(Comb4EDperf(JobName=self.JobName,SoundFileRootDir_Host_Dec=self.SoundFileRootDir_Host_Dec,IDlength=self.IDlength,\
                   GTfile=self.GTfile,FGfile=self.FGfile,FileGroupID=self.FileGroupID,EDprocess=self.EDprocess,EDsplits=self.EDsplits,EDcpu=self.EDcpu,\
                   EDchunk=self.EDchunk,EDmethodID=self.EDmethodID,EDparamString=self.EDparamString,EDparamNames=self.EDparamNames,ALprocess=self.ALprocess,\
                   ALmethodID=self.ALmethodID,ALparamString=self.ALparamString,loopVar=self.IDlength,\
                   FGmethodID=self.FGmethodID,decimatedata = self.decimatedata,SoundFileRootDir_Host_Raw=self.SoundFileRootDir_Host_Raw,\
                   FGparamString=self.FGparamString,PE1process=self.PE1process,PE1methodID=self.PE1methodID,\
                   ProjectRoot=self.ProjectRoot,system=self.system,r_version=self.r_version))


