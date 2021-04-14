from instinct import *
from Comb4FeatureTrain import *

#just run in TT, for now. 

class Comb4PE2All(Comb4FeatureTrain,ApplyModel,TrainModel,ApplyCutoff):

    n_IDlength=luigi.IntParameter()
    n_FGfile=luigi.Parameter()
    n_GTfile = luigi.Parameter()

    #nullify some inherited parameters:
    upstream_task1=None
    upstream_task2=None
    upstream_task3=None
    uTask1path=None
    uTask2path=None
    uTask3path=None
    
    def pipelineMap(self,l):
        task0 = Comb4FeatureTrain.invoke(self)
        task1 = TrainModel.invoke(self,task0)
        
        task2 = FormatFG(FGfile = self.n_FGfile[l],ProjectRoot=self.ProjectRoot)
        task3 = UnifyED.invoke(self,task2)
        task4 = UnifyFE.invoke(self,task3,task2)
        task5 = ApplyModel.invoke(self,task4,task1,task2)
        task6 = FormatGT(upstream_task1=task2,uTask1path=task2.outpath(),GTfile=self.n_GTfile[l],ProjectRoot=self.ProjectRoot)
        task7 = AssignLabels.invoke(self,task5,task6,task2)

        return [task0,task1,task2,task3,task4,task5,task6,task7]
    def hashProcess(self):
        hashStrings = [None] * self.n_IDlength
        for l in range(self.n_IDlength):
            print(l)
            self.n_FGfile
            print(1)
            tasks = self.pipelineMap(l)
            taskStr = []
            for f in range(len(tasks)):
                taskStr.extend([tasks[f].hashProcess()])
            
            hashStrings[l] = ' '.join(taskStr)
    
        return Helper.getParamHash2(' '.join(hashStrings),6)
    def outpath(self):
        return self.ProjectRoot + 'Cache/' + self.hashProcess()
    def requires(self):
        for l in range(self.n_IDlength):
            tasks = self.pipelineMap(l)

            yield tasks[len(tasks)-1]
    def output(self):
        return luigi.LocalTarget(self.outpath() + '/DETx.csv.gz') 
    def run(self):
        
        resultPath = self.outpath()

        if not os.path.exists(resultPath):
            os.mkdir(resultPath)
            
        dataframes = [None] * self.n_IDlength
        for k in range(self.n_IDlength):
            tasks=self.pipelineMap(k)
            dataframes[k] = pd.read_csv(tasks[7].outpath() + '/DETx.csv.gz',compression='gzip')
        AllDat = pd.concat(dataframes,ignore_index=True)

        AllDat.to_csv(resultPath + '/DETx.csv.gz',index=False,compression="gzip")
        
    def invoke(obj):
        return(Comb4PE2All(ProjectRoot=obj.ProjectRoot,SoundFileRootDir_Host=obj.SoundFileRootDir_Host,\
                            IDlength=obj.IDlength,FGfile=obj.FGfile,FileGroupID=obj.FileGroupID,\
                            GTfile=obj.GTfile,EDprocess=obj.EDprocess,EDsplits=obj.EDsplits,EDcpu=obj.EDcpu,\
                            EDchunk=obj.EDchunk,EDmethodID=obj.EDmethodID,EDparamString=obj.EDparamString,\
                            EDparamNames=obj.EDparamNames,ALprocess=obj.ALprocess,ALmethodID=obj.ALmethodID,\
                            ALparamString=obj.ALparamString,FEprocess=obj.FEprocess,FEmethodID=obj.FEmethodID,\
                            FEparamString=obj.FEparamString,FEparamNames=obj.FEparamNames,FEsplits=obj.FEsplits,\
                            FEcpu=obj.FEcpu,MFAprocess=obj.MFAprocess,MFAmethodID=obj.MFAmethodID,\
                            TMprocess=obj.TMprocess,TMmethodID=obj.TMmethodID,TMparamString=obj.TMparamString,TMstage=obj.TMstage,\
                            TM_outName=obj.TM_outName,TMcpu=obj.TMcpu,ACcutoffString=obj.ACcutoffString,\
                            n_IDlength=obj.n_IDlength,n_FGfile=obj.n_FGfile,n_GTfile=obj.n_GTfile,system=obj.system,r_version=obj.r_version))
    
