from supporting.instinct import *
from supporting.getParams import *
from ViewGT import *
from supporting.job_fxns import * 
import shutil

class ReduceGT(FormatFG,FormatGT,RavenViewDETx,RavenToDETx,ReduceByGT):
    
    JobName=luigi.Parameter()
    IDlength = luigi.IntParameter()

    GT_signal_code=luigi.Parameter()
    FileGroupID=luigi.Parameter()

    RavenFill=None
    
    #nullify some inherited parameters:
    upstream_task1=None
    upstream_task2=None

    def pipelineMap(self): #here is where you define pipeline structure
        task0 = FormatFG.invoke(self,n=0)
        task1 = ViewGT.invoke(self)
        task2 = RavenToDETx.invoke(self,task1,task0)
        task3 = ReduceByGT.invoke(self,task2,task0)
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
        yield luigi.LocalTarget(self.outpath() + '/' + self.FileGroupID[0][0:(len(self.FileGroupID[0])-4)] + '_reduce.csv')
        return luigi.LocalTarget(self.outpath() + '/' + self.GT_signal_code + '_' + self.FileGroupID[0][0:(len(self.FileGroupID[0])-4)] + '_reduce.csv')
    def run(self):
        #move file
        tasks = self.pipelineMap()
        filepath1 = tasks[3].outpath() + '/DETx.csv.gz'
        filepath2 = tasks[3].outpath() + '/FileGroupFormat.csv.gz'

        GT = pd.read_csv(filepath1)
        if not os.path.exists(self.ProjectRoot +'Outputs/' + self.JobName):
            os.mkdir(self.ProjectRoot +'Outputs/' + self.JobName)

        if not os.path.exists(self.outpath()):
            os.mkdir(self.outpath())

        GT.to_csv(self.outpath() + '/' + self.GT_signal_code + '_' + self.FileGroupID[0][0:(len(self.FileGroupID[0])-4)] + '_reduce.csv',index=False)

        FG = pd.read_csv(filepath2)

        FG.to_csv(self.outpath() + '/' + self.FileGroupID[0][0:(len(self.FileGroupID[0])-4)] + '_reduce.csv',index=False)

    def invoke(self):
        return(ReduceGT(JobName=self.JobName,SoundFileRootDir_Host_Dec=self.SoundFileRootDir_Host_Dec,IDlength=self.IDlength,GT_signal_code=self.GT_signal_code,\
                   GTfile=self.GTfile,FGfile=self.FGfile,RVmethodID=self.RVmethodID,RDmethodID=self.RDmethodID,FileGroupID=self.FileGroupID,\
                   RGmethodID=self.RGmethodID,RGparamString=self.RGparamString,FGmethodID=self.FGmethodID,decimatedata = self.decimatedata,\
                   SoundFileRootDir_Host_Raw=self.SoundFileRootDir_Host_Raw,FGparamString=self.FGparamString,ProjectRoot=self.ProjectRoot,\
                   system=self.system,CacheRoot=self.CacheRoot))
    def getParams(args):
        
        params = Load_Job('EditGTwRaven',args)
        
        params = FG(params,'FormatFG')
        if len(args)==4:
            params = FG(params,'FormatFG',FGovr=args[3])
        else:
            params = FG(params,'FormatFG')
        params = GT(params,'FormatGT')
        params = RV(params,'RavenViewDETx')
        params = RD(params,'RavenToDETx')
        params = RG(params,'ReduceByGT')

        return params

if __name__ == '__main__':
    deployJob(ReduceGT,sys.argv)
