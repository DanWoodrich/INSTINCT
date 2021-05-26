import shutil
from supporting.instinct import *
from supporting.getParams import *
from supporting.job_fxns import * 

class ViewDet(FormatFG,FormatGT,UnifyED,AssignLabels,RavenViewDETx):
    
    JobName=luigi.Parameter()

    #nullify some inherited parameters:
    upstream_task1=None
    upstream_task2=None
    upstream_task3=None

    RavenFill=None

    def pipelineMap(self): #here is where you define pipeline structure 
        task0 = FormatFG.invoke(self,n=0) 
        task1 = FormatGT.invoke(self,task0,n=0)
        task2 = UnifyED.invoke(self,task0)
        task3 = AssignLabels.invoke(self,task2,task1,task0)
        task4 = RavenViewDETx.invoke(self,task3,task0)
        return [task0,task1,task2,task3,task4]
    def hashProcess(self):
        taskStr = []
        tasks = self.pipelineMap()
        for f in range(len(tasks)):
            taskStr.extend([tasks[f].hashProcess()])
            
        hashStrings = ' '.join(taskStr)

        return Helper.getParamHash2(' '.join(hashStrings),6)
    def requires(self):
        tasks = self.pipelineMap()
        return tasks[4]
    def outpath(self):
        return self.ProjectRoot +'Outputs/' + self.JobName + '/' + self.hashProcess()
    def output(self):
        return luigi.LocalTarget(self.outpath() + '/RAVENx.txt')
    def run(self):

        #move file
        tasks = self.pipelineMap()
        filepath = tasks[4].outpath() + '/RAVENx.txt'
        filedest = self.outpath() + '/RAVENx.txt'

        if not os.path.exists(self.ProjectRoot +'Outputs/' + self.JobName):
            os.mkdir(self.ProjectRoot +'Outputs/' + self.JobName)

        if not os.path.exists(self.outpath()):
            os.mkdir(self.outpath())

        shutil.copy(filepath, filedest)
        
    def invoke(self):
        return(ViewDet(JobName=self.JobName,SoundFileRootDir_Host_Dec=self.SoundFileRootDir_Host_Dec,\
                   GTfile=self.GTfile,FGfile=self.FGfile,EDprocess=self.EDprocess,EDsplits=self.EDsplits,EDcpu=self.EDcpu,\
                   EDchunk=self.EDchunk,EDmethodID=self.EDmethodID,EDparamString=self.EDparamString,EDparamNames=self.EDparamNames,ALprocess=self.ALprocess,\
                   ALmethodID=self.ALmethodID,ALparamString=self.ALparamString,RVmethodID=self.RVmethodID,\
                   FGmethodID=self.FGmethodID,decimatedata = self.decimatedata,SoundFileRootDir_Host_Raw=self.SoundFileRootDir_Host_Raw,\
                   FGparamString=self.FGparamString,ProjectRoot=self.ProjectRoot,system=self.system,CacheRoot=self.CacheRoot))
    
    def getParams(args):

        params = Load_Job('ViewDet',args)
        if len(args)==4:
            params = FG(params,'FormatFG',FGovr=args[3])
        else:
            params = FG(params,'FormatFG')
        params = GT(params,'FormatGT')
        params = ED(params,'EventDetector')
        params = AL(params,'AssignLabels')
        params = RV(params,'RavenViewDETx')

        return params
    
if __name__ == '__main__':
    deployJob(ViewDet,sys.argv)
