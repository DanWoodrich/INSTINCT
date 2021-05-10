import shutil
from supporting.instinct import *
from supporting.Comb4FeatureTrain import *
from supporting.getParams import *
from supporting.job_fxns import * 

class ViewFGfromCV(Comb4FeatureTrain,TrainModel,SplitForPE,ApplyCutoff,RavenViewDETx):
    
    JobName=luigi.Parameter()
    topLoop=luigi.Parameter()
    
    def pipelineMap(self,l): #here is where you define pipeline structure
        task0 = Comb4FeatureTrain.invoke(self)
        task1 = TrainModel.invoke(self,task0)
        
        task2 = FormatFG.invoke(self,n=l)#redundant but lets SFPE,AL,PE1 continue their path
        task3 = SplitForPE.invoke(self,task2,task1,n=l)
        task4 = ApplyCutoff.invoke(self,task3)
        task5 = FormatGT.invoke(self,task2,n=l)
        task6 = AssignLabels.invoke(self,task4,task5,task2)
        task7 = RavenViewDETx.invoke(self,task6,task2)

        return [task0,task1,task2,task3,task4,task5,task6,task7]
    def outpath(self):
        return self.ProjectRoot +'Outputs/' + self.JobName + '/' + self.hashProcess()
    def hashProcess(self):
        taskStr = []
        tasks = self.pipelineMap(self.topLoop)
        for f in range(len(tasks)):
            taskStr.extend([tasks[f].hashProcess()])
            
        hashStrings = ' '.join(taskStr)

        return Helper.getParamHash2(' '.join(hashStrings),6)
    def requires(self):
        tasks = self.pipelineMap(self.topLoop)
        return tasks[7]
    def output(self):
        #this is full performance report
        #return luigi.LocalTarget(OutputsRoot + self.JobName + '/' + self.JobHash + '/RFmodel.rds')
        return luigi.LocalTarget(self.outpath() + '/RAVENx.csv')
    def run(self):

        #move file
        tasks = self.pipelineMap(self.topLoop)
        filepath = tasks[7].outpath() + '/RAVENx.txt'
        filedest = self.outpath() + '/RAVENx.txt'

        if not os.path.exists(self.ProjectRoot +'Outputs/' + self.JobName):
            os.mkdir(self.ProjectRoot +'Outputs/' + self.JobName)

        if not os.path.exists(self.outpath()):
            os.mkdir(self.outpath())

        shutil.copy(filepath, filedest)

    def invoke(obj):
        return(ViewFGfromCV(JobName=obj.JobName,SoundFileRootDir_Host_Dec=obj.SoundFileRootDir_Host_Dec,\
                             IDlength=obj.IDlength,FGfile=obj.FGfile,FileGroupID=obj.FileGroupID,GTfile=obj.GTfile,EDprocess=obj.EDprocess,\
                             EDsplits=obj.EDsplits,EDcpu=obj.EDcpu,EDchunk=obj.EDchunk,EDmethodID=obj.EDmethodID,EDparamString=obj.EDparamString,\
                             EDparamNames=obj.EDparamNames,ALprocess=obj.ALprocess,ALmethodID=obj.ALmethodID,ALparamString=obj.ALparamString,\
                             FEprocess=obj.FEprocess,FEmethodID=obj.FEmethodID,FEparamString=obj.FEparamString,FEparamNames=obj.FEparamNames,\
                             FEsplits=obj.FEsplits,FEcpu=obj.FEcpu,MFAprocess=obj.MFAprocess,MFAmethodID=obj.MFAmethodID,TMprocess=obj.TMprocess,\
                             TMmethodID=obj.TMmethodID,TMparamString=obj.TMparamString,TMstage=obj.TMstage,TM_outName=obj.TM_outName,FGparamString=obj.FGparamString,\
                             FGmethodID=obj.FGmethodID,decimatedata = obj.decimatedata,SoundFileRootDir_Host_Raw=obj.SoundFileRootDir_Host_Raw,\
                             TMcpu=obj.TMcpu,ACcutoffString=obj.ACcutoffString,ProjectRoot=obj.ProjectRoot,system=obj.system,RVmethodID=obj.RVmethodID,\
                             r_version=obj.r_version,loopVar = obj.IDlength,topLoop=obj.topLoop))
    def getParams(args):    
        params = Load_Job('ViewFGfromCV',args)

        params = FG(params,'FormatFG')
        params = GT(params,'FormatGT')
        params = ED(params,'EventDetector')
        params = FE(params,'FeatureExtraction')
        params = AL(params,'AssignLabels')
        params = MFA(params,'MergeFE_AL')
        params = TM(params,'TrainModel','CV')
        params = AC(params,'ApplyCutoff')
        params = RV(params,'RavenViewDETx')

        #last arg is an optional file group to specify (in case reading from a list)
        if len(args)==4:
            print(params.FileGroupID)
            ind=params.FileGroupID.index(args[3])
            assert isinstance(ind, int)
            params.topLoop=ind
            
        return params

if __name__ == '__main__':
    deployJob(ViewFGfromCV,sys.argv)

