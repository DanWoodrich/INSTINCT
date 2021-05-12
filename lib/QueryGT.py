from supporting.instinct import *
from supporting.getParams import *
from supporting.job_fxns import * 
import shutil

class ViewGT(QueryData,FormatFG,FormatGT,RavenViewDETx):
    
    JobName=luigi.Parameter()

    RavenFill=None

    #nullify some inherited parameters:
    upstream_task1=None
    upstream_task2=None

    def pipelineMap(self): #here is where you define pipeline structure 
        task0 = QueryData.invoke(self)
        task1 = FormatFG.invoke(self,upstream1=task0,n=0) 
        task2 = FormatGT.invoke(self,task1,n=0)
        task3 = RavenViewDETx.invoke(self,task2,task1,"T")
        return [task0,task1,task2,task3]
    def hashProcess(self):
        taskStr = []
        tasks = self.pipelineMap()
        for f in range(len(tasks)):
            taskStr.extend([tasks[f].hashProcess()])
            
        hashStrings = ' '.join(taskStr)

        return Helper.getParamHash2(' '.join(hashStrings),6)
    def requires(self):
        tasks = self.pipelineMap()
        return tasks[3]
    def outpath(self):
        return self.ProjectRoot +'Outputs/' + self.JobName + '/' + self.hashProcess()
    def output(self):
        return luigi.LocalTarget(self.outpath() + '/RAVENx.txt')
    def run(self):
        #this is copy pasted from ViewDet, condense this later

        #move file
        tasks = self.pipelineMap()
        filepath = tasks[3].outpath() + '/RAVENx.txt'
        filedest = self.outpath() + '/RAVENx.txt'

        if not os.path.exists(self.ProjectRoot +'Outputs/' + self.JobName):
            os.mkdir(self.ProjectRoot +'Outputs/' + self.JobName)

        if not os.path.exists(self.outpath()):
            os.mkdir(self.outpath())

        shutil.copy(filepath, filedest)
        
    def invoke(self):
        return(ViewGT(JobName=self.JobName,SoundFileRootDir_Host_Dec=self.SoundFileRootDir_Host_Dec,\
                      GTfile=self.GTfile,FGfile=self.FGfile,RVmethodID=self.RVmethodID,QDmethodID=self.QDmethodID,QDsource=self.QDsource,\
                      QDstatement,GT_signal_code=self.GT_signal_code
                      FGmethodID=self.FGmethodID,decimatedata = self.decimatedata,SoundFileRootDir_Host_Raw=self.SoundFileRootDir_Host_Raw,\
                      FGparamString=self.FGparamString,ProjectRoot=self.ProjectRoot,system=self.system,r_version=self.r_version))
    def getParams(args):

        params = Load_Job('QueryGT',args)
        params = FG(params,'FormatFG')
        if len(args)==4:
            params.FileGroupID=[args[4]]
            params.FGfile = [self.ProjectRoot +'Data/' + 'FileGroups/' + args[4]]
        params = GT(params,'FormatGT')
        params = RV(params,'RavenViewDETx')

        return params

if __name__ == '__main__':
    deployJob(ViewGT,sys.argv)
