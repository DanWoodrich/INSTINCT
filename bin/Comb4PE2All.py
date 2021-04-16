from instinct import *
from Comb4FeatureTrain import * 
#just run in TT, for now.

class Comb4PE2All(Comb4Standard,FormatFG,FormatGT,UnifyED,AssignLabels,UnifyFE,MergeFE_AL,ApplyModel,TrainModel,ApplyCutoff):

    n_IDlength=luigi.IntParameter()
    n_FGfile=luigi.Parameter()
    n_GTfile = luigi.Parameter()

    IDlength=luigi.Parameter()
    FileGroupID=luigi.Parameter()

    fileName = 'DETx.csv.gz'
    
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
    def invoke(obj):
        return(Comb4PE2All(ProjectRoot=obj.ProjectRoot,SoundFileRootDir_Host=obj.SoundFileRootDir_Host,loopVar = obj.n_IDlength,\
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
    
