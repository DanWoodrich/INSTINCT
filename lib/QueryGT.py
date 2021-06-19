from supporting.instinct import *
from supporting.getParams import *
from supporting.job_fxns import * 
import shutil

class QueryGT(QueryData,FormatFG,FormatGT,RavenViewDETx):
    
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
        return [task1,task2,task3] #intentionally leave out task0- it will hash anyway depending on what is given to FormatFG.
    #And should allow to refer to same file for EditGT
    def hashProcess(self):
        taskStr = []
        tasks = self.pipelineMap()
        for f in range(len(tasks)):
            taskStr.extend([tasks[f].hashProcess()])
            
        hashStrings = ' '.join(taskStr)

        return Helper.getParamHash2(' '.join(hashStrings),6)
    def requires(self):
        tasks = self.pipelineMap()
        yield tasks[0]
        yield tasks[1]
        yield tasks[2]
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
        return(QueryGT(JobName=self.JobName,SoundFileRootDir_Host_Dec=self.SoundFileRootDir_Host_Dec,\
                      GTfile=self.GTfile,FGfile=self.FGfile,RVmethodID=self.RVmethodID,QDmethodID=self.QDmethodID,\
                      QDparamString=self.QDparamString,FileGroupID=self.FileGroupID,\
                      FGmethodID=self.FGmethodID,decimatedata = self.decimatedata,SoundFileRootDir_Host_Raw=self.SoundFileRootDir_Host_Raw,\
                      FGparamString=self.FGparamString,ProjectRoot=self.ProjectRoot,system=self.system,CacheRoot=self.CacheRoot))
    def getParams(args):

        params = Load_Job('EditGTwRaven',args)
        params = QD(params,'QueryData')
        if len(args)==4:
            params = FG(params,'FormatFG',FGovr=args[3])
        else:
            params = FG(params,'FormatFG')
        params = GT(params,'FormatGT')
        params = RV(params,'RavenViewDETx')

        return params

if __name__ == '__main__':
    deployJob(QueryGT,sys.argv)
