from supporting.instinct import *
from supporting.getParams import *
from supporting.job_fxns import * 
import shutil

class ViewGT(FormatFG,FormatGT,RavenViewDETx):
    
    JobName=luigi.Parameter()
    IDlength = luigi.IntParameter()

    #nullify some inherited parameters:
    upstream_task1=None
    upstream_task2=None

    def pipelineMap(self): #here is where you define pipeline structure 
        task0 = FormatFG.invoke(self,n=0) 
        task1 = FormatGT.invoke(self,task0,n=0)
        task2 = RavenViewDETx.invoke(self,task1,task0)
        return [task0,task1,task2]
    def hashProcess(self):
        taskStr = []
        tasks = self.pipelineMap()
        for f in range(len(tasks)):
            taskStr.extend([tasks[f].hashProcess()])
            
        hashStrings = ' '.join(taskStr)

        return Helper.getParamHash2(' '.join(hashStrings),6)
    def requires(self):
        tasks = self.pipelineMap()
        return tasks[2]
    def outpath(self):
        return self.ProjectRoot +'Outputs/' + self.JobName + '/' + self.hashProcess()
    def output(self):
        return luigi.LocalTarget(self.outpath() + '/RAVENx.txt')
    def run(self):
        #this is copy pasted from ViewDet, condense this later

        #move file
        tasks = self.pipelineMap()
        filepath = tasks[2].outpath() + '/RAVENx.txt'
        filedest = self.outpath() + '/RAVENx.txt'

        if not os.path.exists(self.ProjectRoot +'Outputs/' + self.JobName):
            os.mkdir(self.ProjectRoot +'Outputs/' + self.JobName)

        if not os.path.exists(self.outpath()):
            os.mkdir(self.outpath())

        shutil.copy(filepath, filedest)
        
    def invoke(self):
        return(ViewGT(JobName=self.JobName,SoundFileRootDir_Host_Dec=self.SoundFileRootDir_Host_Dec,IDlength=self.IDlength,\
                   GTfile=self.GTfile,FGfile=self.FGfile,RVmethodID=self.RVmethodID,\
                   FGmethodID=self.FGmethodID,decimatedata = self.decimatedata,SoundFileRootDir_Host_Raw=self.SoundFileRootDir_Host_Raw,\
                   FGparamString=self.FGparamString,ProjectRoot=self.ProjectRoot,system=self.system,r_version=self.r_version))
    def getParams(args):

        params = Load_Job('EditGTwRaven',args)
        params = FG(params,'FormatFG')
        if len(args)==4:
            ind=params.FileGroupID.index(args[3])
            params = FG(params,'FormatFG',ind=ind)
        params = GT(params,'FormatGT')
        params = RV(params,'RavenViewDETx')

        return params

if __name__ == '__main__':
    deployJob(ViewGT,sys.argv)
