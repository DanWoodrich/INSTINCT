from instinct import *


class Comb4FeatureTrain(FormatFG,FormatGT,UnifyED,AssignLabels,UnifyFE,MergeFE_AL):

    IDlength = luigi.IntParameter()
    FileGroupID = luigi.Parameter()

    #nullify some inherited parameters:
    upstream_task1=None
    upstream_task2=None
    upstream_task3=None
    uTask1path=None
    uTask2path=None
    uTask3path=None

    def pipelineMap(self,l): #here is where you define pipeline structure
        task0 = FormatFG.invoke(self,l) 
        task1 = FormatGT.invoke(self,task0,l)
        task2 = UnifyED.invoke(self,task0)
        task3 = AssignLabels.invoke(self,task2,task1,task0)
        task4 = UnifyFE.invoke(self,task2,task0)
        task5 = MergeFE_AL.invoke(self,task3,task4)
        return [task0,task1,task2,task3,task4,task5]
    def hashProcess(self):
        hashStrings = [None] * self.IDlength
        for l in range(self.IDlength):
            tasks = self.pipelineMap(l)
            hashStrings[l] = ' '.join([tasks[0].hashProcess(),tasks[1].hashProcess(),tasks[2].hashProcess(),tasks[3].hashProcess(),
                                  tasks[4].hashProcess(),tasks[5].hashProcess()])
    
        return Helper.getParamHash2(' '.join(hashStrings),6)
        #Method + pipeline  hashes
    def outpath(self):
        return self.ProjectRoot + 'Cache/' + self.hashProcess()
    def requires(self):
        for l in range(self.IDlength):
            tasks = self.pipelineMap(l)

            yield tasks[len(tasks)-1]
    def output(self):
        #return two outputs, TM_intermediate and FG_intermediate
        yield luigi.LocalTarget(self.outpath() +'/FileGroupFormat.csv.gz')
        yield luigi.LocalTarget(self.outpath() +'/DETx.csv.gz')
    def run(self):

        #concatenate outputs and summarize
        #load in 
        dataframes = [None] * self.IDlength
        FGdf = [None] * self.IDlength
        for k in range(self.IDlength):
            tasks = self.pipelineMap(k)
            
            dataframes[k] = pd.read_csv(tasks[5].outpath()+ '/DETx.csv.gz') # 
            dataframes[k]['FGID'] = pd.Series(self.FileGroupID[k], index=dataframes[k].index)
            
            FGdf[k] = pd.read_csv(tasks[0].outpath() + '/FileGroupFormat.csv.gz')
        TMdat = pd.concat(dataframes,ignore_index=True)
        FGdf = pd.concat(FGdf,ignore_index=True)

        resultPath =self.outpath()
        if not os.path.exists(resultPath):
            os.mkdir(resultPath)

        TMdat.to_csv(resultPath + '/DETx.csv.gz',index=False,compression='gzip')
        FGdf.to_csv(resultPath + '/FileGroupFormat.csv.gz',index=False,compression='gzip')

        #

        #copy params to output folder
    def invoke(self):
        return(Comb4FeatureTrain(SoundFileRootDir_Host=self.SoundFileRootDir_Host,\
                                 IDlength=self.IDlength,GTfile=self.GTfile,FGfile=self.FGfile,FileGroupID=self.FileGroupID,EDprocess=self.EDprocess,EDsplits=self.EDsplits,\
                                 EDcpu=self.EDcpu,EDchunk=self.EDchunk,EDmethodID=self.EDmethodID,EDparamString=self.EDparamString,EDparamNames=self.EDparamNames,ALprocess=self.ALprocess,\
                                 ALmethodID=self.ALmethodID,ALparamString=self.ALparamString,\
                                 FEprocess=self.FEprocess,FEmethodID=self.FEmethodID,FEparamString=self.FEparamString,FEparamNames=self.FEparamNames,\
                                 FEsplits=self.FEsplits,FEcpu=self.FEcpu,MFAprocess=self.MFAprocess,MFAmethodID=self.MFAmethodID,\
                                 system=self.system,ProjectRoot=self.ProjectRoot,r_version=self.r_version))

