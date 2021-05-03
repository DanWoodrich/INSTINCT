from instinct import *
from getParams import *
import shutil
#Ready a bunch of FGs for shared comparison with PE1 pt 2.

VD_params = Load_Job('ViewDet')
VD_params = FG(VD_params,'FormatFG')
VD_params = GT(VD_params,'FormatGT')
VD_params = ED(VD_params,'EventDetector')
VD_params = AL(VD_params,'AssignLabels')
VD_params = RV(VD_params,'RavenViewDETx')

class ViewDet(FormatFG,FormatGT,UnifyED,AssignLabels,RavenViewDETx):
    
    JobName=luigi.Parameter()
    IDlength = luigi.IntParameter()

    #nullify some inherited parameters:
    upstream_task1=None
    upstream_task2=None
    upstream_task3=None
    uTask1path=None
    uTask2path=None
    uTask3path=None

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
        return(ViewDet(JobName=self.JobName,SoundFileRootDir_Host_Dec=self.SoundFileRootDir_Host_Dec,IDlength=self.IDlength,\
                   GTfile=self.GTfile,FGfile=self.FGfile,EDprocess=self.EDprocess,EDsplits=self.EDsplits,EDcpu=self.EDcpu,\
                   EDchunk=self.EDchunk,EDmethodID=self.EDmethodID,EDparamString=self.EDparamString,EDparamNames=self.EDparamNames,ALprocess=self.ALprocess,\
                   ALmethodID=self.ALmethodID,ALparamString=self.ALparamString,RVmethodID=self.RVmethodID,\
                   FGmethodID=self.FGmethodID,decimatedata = self.decimatedata,SoundFileRootDir_Host_Raw=self.SoundFileRootDir_Host_Raw,\
                   FGparamString=self.FGparamString,ProjectRoot=self.ProjectRoot,system=self.system,r_version=self.r_version))

if __name__ == '__main__':
    luigi.build([ViewDet.invoke(VD_params)], local_scheduler=True)

    print(ViewDet.invoke(VD_params).hashProcess())
