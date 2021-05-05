from instinct import *
from getParams import *
from ViewGT import * 
import shutil

class EditGT(FormatFG,FormatGT,RavenViewDETx,RavenToDETx):
    
    JobName=luigi.Parameter()
    IDlength = luigi.IntParameter()

    GT_signal_code=luigi.Parameter()
    FileGroupID=luigi.Parameter()
    
    #nullify some inherited parameters:
    upstream_task1=None
    upstream_task2=None
    uTask1path=None
    uTask2path=None

    def pipelineMap(self): #here is where you define pipeline structure
        task0 = FormatFG.invoke(self,n=0)
        task1 = ViewGT.invoke(self)
        task2 = RavenToDETx.invoke(self,task1,task0)
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
    def getOutName(self):
        return self.GT_signal_code + '_' + self.FileGroupID[0][0:(len(self.FileGroupID[0])-4)] + '_edit.csv'
    def outpath(self):
        return self.ProjectRoot +'Data/GroundTruth'
    def output(self):
        return luigi.LocalTarget(self.outpath() + '/' + self.getOutName())
    def run(self):
        #move file
        tasks = self.pipelineMap()
        filepath = tasks[2].outpath() + '/DETx.csv.gz'
        filedest = self.outpath() + '/DETx.txt'

        GT = pd.read_csv(filepath)
        if not os.path.exists(self.ProjectRoot +'Outputs/' + self.JobName):
            os.mkdir(self.ProjectRoot +'Outputs/' + self.JobName)

        if not os.path.exists(self.outpath()):
            os.mkdir(self.outpath())

        GT.to_csv(self.outpath() + '/' + self.getOutName(),index=False)

    def invoke(self):
        return(EditGT(JobName=self.JobName,SoundFileRootDir_Host_Dec=self.SoundFileRootDir_Host_Dec,IDlength=self.IDlength,GT_signal_code=self.GT_signal_code,\
                   GTfile=self.GTfile,FGfile=self.FGfile,RVmethodID=self.RVmethodID,RDmethodID=self.RDmethodID,FileGroupID=self.FileGroupID,\
                   FGmethodID=self.FGmethodID,decimatedata = self.decimatedata,SoundFileRootDir_Host_Raw=self.SoundFileRootDir_Host_Raw,\
                   FGparamString=self.FGparamString,ProjectRoot=self.ProjectRoot,system=self.system,r_version=self.r_version))
    def getParams(args):
        
        params = Load_Job('EditGTwRaven',args)
        
        params = FG(params,'FormatFG')
        params = GT(params,'FormatGT')
        params = RV(params,'RavenViewDETx')
        params = RD(params,'RavenToDETx')

        return params

if __name__ == '__main__':
    deployJob(EditGT,sys.argv)
