import luigi
import os
import hashlib
import configparser
import pandas as pd
import sys
import numpy
import subprocess
import shlex

#########################
#misc functions 
#########################

class Helper:
    def peek(self,fn_type,fp_type,st_type,dur_type,comp_type=0):
        if comp_type != 0:
            heads = pd.read_csv(self, nrows=1,compression=comp_type)
        else:
            heads = pd.read_csv(self, nrows=1)
        heads = heads.columns.tolist()
        heads.remove('FileName')
        heads.remove('StartTime')
        heads.remove('Duration')
        hdict = {'FileName': fn_type, 'FullPath': fp_type, 'StartTime': st_type, 'Duration': dur_type}
        if len(heads) != 0:
            metadict = dict.fromkeys(heads , 'category')
            hdict.update(metadict)
        return hdict
    def hashfile(self):
        buff_size = 65536
        sha1 = hashlib.sha1()
        with open(self, 'rb') as f:
            while True:
                data = f.read(buff_size)
                if not data:
                    break
                sha1.update(data)
        return sha1.hexdigest()
    def getDifftime(self):
        self=self.sort_values(['StartTime'], ascending=[True])
        self['EndTime'] = self['StartTime']+pd.to_timedelta(self['Duration'], unit='s')
        self['DiffTime'] = self['EndTime'][0:(len(self['EndTime'])-1)] - self['StartTime'][1:len(self['StartTime'])].values
        self['DiffTime'] = self['DiffTime']>pd.to_timedelta(-2,unit='s') #makes the assumption that if sound files are 1 second apart they are actually consecutive (deals with rounding differences)
        consecutive = numpy.empty(len(self['DiffTime']), dtype=int)
        consecutive[0] = 1
        iterator = 1
        for n in range(0,(len(self['DiffTime']))-1):
            if self['DiffTime'].values[n] != True:
                iterator = iterator+1
                consecutive[n + 1] = iterator
            else:
                consecutive[n + 1] = iterator
        self['DiffTime'] = consecutive
        self = self.drop(columns='EndTime')
        return(self)
    def paramString(self):
        string_out = ""
        for p in range(len(self)):
            string_out = string_out + " " + self[p][1]
        string_out = string_out + " "
        return(string_out)
    def paramList(self):
        paramList = [None]*len(self) 
        for p in range(len(self)):
            paramList[p] = self[p][1]
        return(paramList)
    def paramID(self,x):
        self[x][0]
    def getProjRoot():
        appPath = os.getcwd()
        appPath= appPath[:-4]
        appPath=appPath.replace('\\', '/')
        return(appPath + '/')
        

##########################################
#parse variable inputs into system command
##########################################

class argParse:
    def run(cmdType,MethodID,Paths,Args,Params,ProcessID=None,Program=None,ProjectRoot=None,rVers=None,dockerVolMounts=None):
        #windows cmd just wants all arguments seperated by a space
        if(cmdType=='win'):
            if(Program=='R'):
                #default install vers
                command1 = '"'+ rVers + '/bin/Rscript.exe" -e "source(\'' + ProjectRoot + 'bin/' + ProcessID + '/' + MethodID +'/'+MethodID+'.R\')"'
                command2 = ' '.join(Paths) + ' ' + ' '.join(Args) + ' ' + Params
                print(command1)
                print(command2)
                command = command1 + ' ' + command2
                subprocess.run(shlex.split(command))
                return None
            elif(Program!='R'):
                #do something else
                return None
        elif(cmdType=='lin'):
            if(Program=='R'):
                command1 = 'Rscript' + ' ' + ProjectRoot + 'bin/' + ProcessID + '/' + MethodID +'/'+MethodID+'.R'
                command2 = ' '.join(Paths) + ' ' + ' '.join(Args) + ' ' + Params
                command = command1 + ' ' + command2
                os.system(command)
            elif(Program!='R'):
                #do something else
                return None
        elif(cmdType=='docker'):
            #pseudocode (don't need this right now)
            #figure out container name and tab from MethodID var
            #determine length of Paths, combine them with corresponding dockerVolMounts and turn into -v arguments
            #turn args and params into -e arguments
            command1='docker run' 
            return None

    
########################
#format metadata
########################
        
class FormatFG(luigi.Task):

    ProjectRoot=luigi.Parameter()
    
    FGhash = luigi.Parameter()
    FGfile = luigi.Parameter()
    
    def outpath(self):
        outpath = self.ProjectRoot + 'Cache/' + self.FGhash
        return outpath
    def requires(self):
        return None
    def output(self):
        return luigi.LocalTarget(self.outpath() + '/FileGroupFormat.csv.gz')
    def run(self):
        #supports additional metadata fields
        FG_dict = Helper.peek(self.FGfile,fn_type = object,fp_type = object,st_type = object,dur_type = 'float64')
        FG = pd.read_csv(self.FGfile, dtype=FG_dict)
        FG['StartTime'] = pd.to_datetime(FG['StartTime'], format='%y%m%d-%H%M%S')
        FG=Helper.getDifftime(FG)
        os.mkdir(self.outpath())
        FG.to_csv(self.outpath() + '/FileGroupFormat.csv.gz',index=False,compression='gzip')

class FormatGT(luigi.Task):
    ProjectRoot=luigi.Parameter()

    FGhash = luigi.Parameter()
    FGfile = luigi.Parameter()
    GTfile = luigi.Parameter()
    GThash = luigi.Parameter()
    
    def outpath(self):
        outpath = self.ProjectRoot + 'Cache/' + self.FGhash + '/' + self.GThash
        return outpath
    def requires(self):
        return FormatFG(FGhash=self.FGhash,FGfile=self.FGfile,ProjectRoot=self.ProjectRoot)
    def output(self):
        return luigi.LocalTarget(self.outpath() + '/GTFormat.csv.gz')
    def run(self):
        os.mkdir(self.outpath())
        GT = pd.read_csv(self.GTfile)
        GT.to_csv(self.outpath() + '/GTFormat.csv.gz',index=False,compression='gzip')
        
###############################################################################
#Defined event detection with abilty to split into chunks w/o affecting outputs
###############################################################################

class SplitED(luigi.Task):
    ProjectRoot=luigi.Parameter()

    upstream_task = luigi.Parameter()

    #To ensure reliable retrieval of data, hashes always constructed as addition of parameters in order appearing in .ini file
    splits = luigi.IntParameter()
    splitNum = luigi.IntParameter()
    FGhash = luigi.Parameter()
    def outpath(self):
        outpath = self.ProjectRoot + 'Cache/' + self.FGhash
        return outpath
    def requires(self):
        return self.upstream_task
    def output(self):
        return luigi.LocalTarget(self.outpath() + '/FileGroupFormatSplit' + str(self.splitNum+1) + '.csv.gz')
    def run(self):
        #run docker container corresponding to energy detector
        FG_dict = Helper.peek(self.input().fn,fn_type = object,fp_type = object,st_type = object,\
                              dur_type = 'float64',comp_type = 'gzip')
        FG = pd.read_csv(self.input().fn, dtype=FG_dict,compression='gzip')
        if self.splits == 1:
            FG.to_csv(self.outpath() + '/FileGroupFormatSplit1.csv.gz',index=False,compression='gzip')
        #need to test this section to ensure forking works 
        else:
            row_counts = len(FG['DiffTime'])
            breaks = int(row_counts/self.splits)
            blist = numpy.repeat(range(0,self.splits),breaks)
            bdiff = row_counts - len(blist)
            extra = numpy.repeat(self.splits-1,bdiff)
            flist = list(blist.tolist() + extra.tolist())
            FG.loc[[x==self.splitNum for x in flist]].to_csv(self.outpath() + '/FileGroupFormatSplit' + str(self.splitNum+1)\
                                                + '.csv.gz',index=False,compression='gzip')

class RunED(luigi.Task):
    ProjectRoot= luigi.Parameter()
    system= luigi.Parameter()
    r_version=luigi.Parameter()
    
    upstream_task = luigi.Parameter()

    splits = luigi.IntParameter()
    CPU = luigi.Parameter()
    Chunk  = luigi.Parameter()
    splitNum = luigi.IntParameter()
    
    FGhash = luigi.Parameter()
    SoundFileRootDir_Host = luigi.Parameter()
    EDparamsHash = luigi.Parameter()
    Params = luigi.Parameter()

    MethodID = luigi.Parameter()
    ProcessID = luigi.Parameter()

    EDstage = '1' #process in chunks instead of by difftime

    def outpath(self):
        inpath = self.ProjectRoot + 'Cache/' + self.FGhash  
        outpath =  inpath + '/' + self.EDparamsHash   
        return outpath
    def requires(self):
        return SplitED(upstream_task=self.upstream_task,splits=self.splits,splitNum=self.splitNum,FGhash=self.FGhash,ProjectRoot=self.ProjectRoot)
    def output(self):
        return luigi.LocalTarget(self.outpath() + '/EDSplit' + str(self.splitNum+1) + '.csv.gz')
    def run(self):
        #define volume arguments
        FGpath = self.ProjectRoot + 'Cache/' + self.FGhash +'/'
        ReadFile = 'FileGroupFormatSplit' + str(self.splitNum+1) + '.csv.gz'
        DataPath = self.SoundFileRootDir_Host
        resultPath =  self.outpath()
        if not os.path.exists(resultPath):
            os.mkdir(resultPath)

        Paths = [DataPath,FGpath,resultPath]
        Args = [ReadFile,self.EDstage,self.CPU,self.Chunk]
        Params = self.Params

        argParse.run(Program='R',rVers=self.r_version,cmdType=self.system,ProjectRoot=self.ProjectRoot,ProcessID=self.ProcessID,MethodID=self.MethodID,Paths=Paths,Args=Args,Params=Params)
        
class UnifyED(luigi.Task):
    ProjectRoot= luigi.Parameter()
    system= luigi.Parameter()
    r_version=luigi.Parameter()

    upstream_task = luigi.Parameter()

    splits = luigi.IntParameter()
    CPU = luigi.Parameter()
    Chunk  = luigi.Parameter()

    FGhash = luigi.Parameter()
    SoundFileRootDir_Host = luigi.Parameter()
    EDparamsHash = luigi.Parameter()
    Params = luigi.Parameter()

    MethodID = luigi.Parameter()
    ProcessID = luigi.Parameter()

    EDstage = '2' 

    def outpath(self):
        inpath = self.ProjectRoot + 'Cache/' + self.FGhash  
        outpath =  inpath + '/' + self.EDparamsHash   
        return outpath
    def requires(self):
        for k in range(self.splits):
            yield RunED(upstream_task=self.upstream_task,splits=self.splits,splitNum=k,FGhash=self.FGhash,SoundFileRootDir_Host=self.SoundFileRootDir_Host,\
                        MethodID=self.MethodID,ProcessID=self.ProcessID,EDparamsHash=self.EDparamsHash,Params=self.Params,CPU=self.CPU,Chunk=self.Chunk,ProjectRoot=self.ProjectRoot,\
                        system=self.system,r_version=self.r_version)
    def output(self):
        return luigi.LocalTarget(self.outpath() + '/Detections.csv.gz')
    def run(self):
        #this task will perform several functions:
        #combine the split data,
        #assess where difftimes were broken,
        #subset data to these times,
        #pass subset data to ED container to rerun
        #merge outputs with original df to make final df
        #save df

        #define data types
        EDdict = {'StartTime': 'float64', 'EndTime': 'float64','LowFreq': 'int', 'HighFreq': 'int', 'StartFile': 'category','EndFile': 'category','ProcessTag': 'category'}
        
        dataframes = [None] * self.splits
        for k in range(self.splits):
            dataframes[k] = pd.read_csv(self.outpath() +'/EDSplit' + str(k+1) + '.csv.gz',dtype=EDdict)
        ED = pd.concat(dataframes,ignore_index=True)
        ED['ProcessTag2']=ED.ProcessTag.str.split('_', 1).map(lambda x: x[0])
        #determin PT changes
        
        statustest=[None]*len(ED['ProcessTag'])
        for n in range(len(ED['StartTime'])-1):
            statustest[n]=(ED['ProcessTag'][n]==ED['ProcessTag'][n+1])
        #will need a catch in here for if this situation is not present
        chED= ED.loc[[x==False for x in statustest]]
        statustest2=[None]*len(chED['ProcessTag'])
        for n in range(len(chED['StartTime'])-1):
            statustest2[n]=(chED['ProcessTag2'].values[n]==chED['ProcessTag2'].values[n+1])
        chED2= chED.loc[[x==True for x in statustest2]]
        indecesED = chED2.index.append(chED2.index+1)
        #if indecesED doesn't exist, skip the patching step and just save combined data. Assume it's in correct order
        if indecesED.empty:
            EDfin = ED[['StartTime','EndTime','LowFreq','HighFreq','StartFile','EndFile']]
            ED.to_csv(self.outpath() + '/Detections.csv.gz',index=False,compression='gzip')
        else:
            EDfin = ED.loc[indecesED._values]
            #reduce this to just file names to pass to Energy detector (FG style)


            #load in the processed FG


            FG_cols = ['FileName','FullPath','StartTime','Duration','DiffTime']

            FG_dict = {'FileName': 'string','FullPath': 'category', 'StartTime': 'string','Duration': 'int', 'DiffTime':'int'}
            FG = pd.read_csv(self.ProjectRoot + 'Cache/' + self.FGhash +'/FileGroupFormat.csv.gz', dtype=FG_dict, usecols=FG_cols)

            #retain only the file names, and use these to subset original FG
            FG = FG[FG.FileName.isin(EDfin['StartFile'])]
            #recalculate difftime based on new files included
            FG['StartTime'] = pd.to_datetime(FG['StartTime'], format='%Y-%m-%d %H:%M:%S')
            FG = Helper.getDifftime(FG)
            
            #save FG
            FG.to_csv(self.outpath() + '/EDoutCorrect.csv.gz',index=False,compression='gzip')

            FGpath = self.outpath()
            ReadFile = 'EDoutCorrect.csv.gz'
            DataPath = self.SoundFileRootDir_Host
            resultPath =  self.outpath()

            Paths = [DataPath,FGpath,resultPath]
            Args = [ReadFile,self.EDstage,self.CPU,self.Chunk]
            Params = self.Params

            argParse.run(Program='R',rVers=self.r_version,cmdType=self.system,ProjectRoot=self.ProjectRoot,ProcessID=self.ProcessID,MethodID=self.MethodID,Paths=Paths,Args=Args,Params=Params)

            #drop process data from ED
            ED = ED.drop(columns="ProcessTag")
            ED = ED.drop(columns="ProcessTag2")

            EDdict2 = {'StartTime': 'float64', 'EndTime': 'float64','LowFreq': 'int', 'HighFreq': 'int', 'StartFile': 'category','EndFile': 'category','DiffTime': 'int'}
            #now load in result,
            EDpatches = pd.read_csv(FGpath+'/EDunify.csv.gz',dtype=EDdict2)
            PatchList = [None] * len(EDpatches['DiffTime'].unique().tolist())
            for n in range(len(EDpatches['DiffTime'].unique().tolist())):
                EDpatchN= EDpatches[[x == EDpatches['DiffTime'].unique().tolist()[n] for x in EDpatches['DiffTime']]]
                FirstFile = EDpatchN.iloc[[0]]['StartFile'].astype('string').iloc[0]
                FirstDur = FG.loc[FG['FileName'] == FirstFile]['Duration'].iloc[0]
                FirstDurHalf=FirstDur/2

            
                LastFile = EDpatchN.iloc[[-1]]['EndFile'].astype('string').iloc[0]
                LastDur = FG.loc[FG['FileName'] == LastFile]['Duration'].iloc[0]
                LastDurHalf=LastDur/2

                #subset EDpatch
                EDpatchN = EDpatchN[((EDpatchN['StartTime'] > FirstDurHalf) & (EDpatchN['StartFile'] == FirstFile)) | (EDpatchN['StartFile'] != FirstFile)]
                EDpatchN = EDpatchN[((EDpatchN['EndTime'] < LastDurHalf) & (EDpatchN['EndFile'] == LastFile)) | (EDpatchN['EndFile'] != LastFile)]

                EDpatchN=EDpatchN.drop(columns="DiffTime")


                #subset ED
                ED = ED[((ED['StartTime'] <= FirstDurHalf) & (ED['StartFile'] == FirstFile)) | (ED['StartFile'] != FirstFile)]
                ED = ED[((ED['EndTime'] >= LastDurHalf) & (ED['EndFile'] == LastFile)) | (ED['EndFile'] != LastFile)]

                EDpNfiles = pd.Series(EDpatchN['StartFile'].append(EDpatchN['EndFile']).unique()) #switched to numpy array on an unknown condition, pd.Series forces it to stay this datatype. Needs testing

                FandLfile = [FirstFile,LastFile]
                internalFiles = EDpNfiles[EDpNfiles.isin(FandLfile)==False]

                if len(internalFiles)>0:
                    #subset to remove internal files from patch from ED
                    ED = ED[(ED.StartFile.isin(internalFiles)==False)&(ED.EndFile.isin(internalFiles)==False)]

                #here, subset all the detections within EDpatchN: find any sound files that are not start and end file, and remove them from ED
                #hint: isin to find files in EDpN, and isin to subset ED. 
                #ED = ED[(ED.StartFile.isin(EDpatchN['StartFile'])==False)

                #save ED patch
                PatchList[n]=EDpatchN

                
            EDpatchsub = pd.concat(PatchList,ignore_index=True)
            #combine ED and EDpatch
            
            ED = pd.concat([EDpatchsub,ED],ignore_index=True)
            ED = ED.sort_values(['StartTime'], ascending=True)

            ED.to_csv(self.outpath() + '/Detections.csv.gz',index=False,compression='gzip')

            os.remove(self.outpath() + '/EDunify.csv.gz') 
            os.remove(self.outpath() + '/EDoutCorrect.csv.gz')
            
        for n in range(self.splits):
            os.remove(self.outpath() + '/EDSplit' + str(n+1) + '.csv.gz') 
        for n in range(self.splits):
            os.remove(self.ProjectRoot + 'Cache/' + self.FGhash + '/FileGroupFormatSplit' + str(n+1) + '.csv.gz') #might want to rework this to make the files load in the current directory instead of the upstream directory. makes more atomic

                
        #merge outputs with ED:
        #pseudo code: for each difftime in patches, eliminate detections in starting in the first half of 1st file and ending in the last half of last file
        #for same file in ED, eliminate detections in first patches file ending in the
        #finally, merge, and eliminate any redundant detections            

####################################################################
#Feature extraction with horizontal scaling and containerized method
####################################################################

class SplitFE(luigi.Task):
    ProjectRoot= luigi.Parameter()

    upstream_task = luigi.Parameter()
    uTaskpath = luigi.Parameter()

    splits = luigi.IntParameter()
    splitNum = luigi.IntParameter()
    FGhash = luigi.Parameter()

    def requires(self):
        return self.upstream_task
    def output(self):
        return luigi.LocalTarget(self.ProjectRoot + 'Cache/' + self.FGhash + '/' + self.uTaskpath + '/DETsplitForFE' + str(self.splitNum+1) + '.csv.gz')
    def run(self):
        #pseudocode:
        #split dataset into num of workers

        DETdict = {'StartTime': 'float64', 'EndTime': 'float64','LowFreq': 'int', 'HighFreq': 'int', 'StartFile': 'category','EndFile': 'category'}
        DET = pd.read_csv(self.ProjectRoot + 'Cache/' + self.FGhash + '/' + self.uTaskpath + '/Detections.csv.gz', dtype=DETdict,compression='gzip')
        
        if self.splits == 1:
            DET.to_csv(self.ProjectRoot + 'Cache/' + self.FGhash + '/' + self.uTaskpath + '/DETsplitForFE1.csv.gz',index=False,compression='gzip')
        #need to test this section to ensure forking works 
        else:
            row_counts = len(DET['StartTime'])
            breaks = int(row_counts/self.splits)
            blist = numpy.repeat(range(0,self.splits),breaks)
            bdiff = row_counts - len(blist)
            extra = numpy.repeat(self.splits-1,bdiff)
            flist = list(blist.tolist() + extra.tolist())
            DET.loc[[x==self.splitNum for x in flist]].to_csv(self.ProjectRoot + 'Cache/' + self.FGhash + '/' + self.uTaskpath + '/DETsplitForFE' + str(self.splitNum+1)\
                                                + '.csv.gz',index=False,compression='gzip')
class RunFE(luigi.Task):
    ProjectRoot= luigi.Parameter()
    system= luigi.Parameter()
    r_version=luigi.Parameter()
    
    upstream_task = luigi.Parameter()
    uTaskpath = luigi.Parameter()

    splits = luigi.IntParameter()
    CPU = luigi.Parameter()
    
    splitNum = luigi.Parameter()
    FGhash = luigi.Parameter()
    SoundFileRootDir_Host = luigi.Parameter()
    FEparamsHash=luigi.Parameter()

    Params = luigi.Parameter()

    MethodID = luigi.Parameter()
    ProcessID = luigi.Parameter()

    def requires(self):
        return SplitFE(upstream_task=self.upstream_task,splits=self.splits,splitNum=self.splitNum,FGhash=self.FGhash,uTaskpath=self.uTaskpath,ProjectRoot=self.ProjectRoot)
    def output(self):
        return luigi.LocalTarget(self.ProjectRoot + 'Cache/' + self.FGhash + '/' + self.uTaskpath + '/' + self.FEparamsHash + '/DETwFeaturesSplit' + str(self.splitNum+1) + '.csv.gz')
    def run(self):
        #define volume arguments
        FGpath = self.ProjectRoot + 'Cache/' + self.FGhash + '/'
        DETpath = self.ProjectRoot + 'Cache/' + self.FGhash + '/' + self.uTaskpath
        DataPath = self.SoundFileRootDir_Host
        resultPath =  self.ProjectRoot + 'Cache/' + self.FGhash + '/' + self.uTaskpath + '/' + self.FEparamsHash
        if not os.path.exists(resultPath):
            os.mkdir(resultPath)

        Paths = [FGpath,DETpath,DataPath,resultPath]
        Args = [str(self.splitNum+1),str(self.CPU)]
        Params = self.Params

        argParse.run(Program='R',rVers=self.r_version,cmdType=self.system,ProjectRoot=self.ProjectRoot,ProcessID=self.ProcessID,MethodID=self.MethodID,Paths=Paths,Args=Args,Params=Params)

class UnifyFE(luigi.Task):
    ProjectRoot= luigi.Parameter()
    system= luigi.Parameter()
    r_version=luigi.Parameter()
    
    upstream_task = luigi.Parameter()
    uTaskpath = luigi.Parameter()

    splits = luigi.IntParameter()
    CPU = luigi.Parameter()

    FGhash = luigi.Parameter()
    SoundFileRootDir_Host = luigi.Parameter()
    FEparamsHash=luigi.Parameter()

    Params = luigi.Parameter()

    MethodID = luigi.Parameter()
    ProcessID = luigi.Parameter()
    
    def requires(self):
        for k in range(self.splits):
            yield RunFE(upstream_task=self.upstream_task,uTaskpath=self.uTaskpath,splits=self.splits,splitNum=k,FGhash=self.FGhash,SoundFileRootDir_Host=self.SoundFileRootDir_Host,\
                        FEparamsHash=self.FEparamsHash,Params=self.Params,MethodID=self.MethodID,ProcessID=self.ProcessID,CPU=self.CPU,ProjectRoot=self.ProjectRoot,\
                        system=self.system,r_version=self.r_version)
    def output(self):
        return luigi.LocalTarget(self.ProjectRoot + 'Cache/' + self.FGhash + '/' + self.uTaskpath + '/' + self.FEparamsHash + '/DETwFeatures.csv.gz')
    def run(self):
        #maybe should try to specify data types, but should assume numeric for these?
        dataframes = [None] * self.splits
        for k in range(self.splits):
            dataframes[k] = pd.read_csv(self.ProjectRoot + 'Cache/' + self.FGhash + '/' + self.uTaskpath + '/' + self.FEparamsHash + '/DETwFeaturesSplit' + str(k+1)+ '.csv.gz')
        FE = pd.concat(dataframes,ignore_index=True)
        FE.to_csv(self.ProjectRoot + 'Cache/' + self.FGhash + '/' + self.uTaskpath + '/' + self.FEparamsHash + '/DETwFeatures.csv.gz',index=False,compression='gzip')

        for n in range(self.splits):
            os.remove(self.ProjectRoot + 'Cache/' + self.FGhash + '/' + self.uTaskpath + '/' + self.FEparamsHash + '/DETwFeaturesSplit' + str(n+1) + '.csv.gz')
        for n in range(self.splits):
            os.remove(self.ProjectRoot + 'Cache/' + self.FGhash + '/' + self.uTaskpath + '/DETsplitForFE' + str(n+1) + '.csv.gz')

############################################################
#Label detector outputs with GT using containerized method 
############################################################

class AssignLabels(luigi.Task):
    ProjectRoot= luigi.Parameter()
    system= luigi.Parameter()
    r_version=luigi.Parameter()

    
    upstream_task1 = luigi.Parameter() #det output
    upstream_task2 = luigi.Parameter() #GT

    FGhash = luigi.Parameter()
    uTask1path = luigi.Parameter()
    uTask2path = luigi.Parameter()
    
    ALparamsHash=luigi.Parameter()
    Params=luigi.Parameter()
    ProcessID = luigi.Parameter()
    MethodID = luigi.Parameter()

    #
    stage=luigi.Parameter()
    
    def outpath(self):
        if self.stage=='1':
            inpath = self.ProjectRoot + 'Cache/' + self.FGhash + '/' + self.uTask1path 
            outpath =  inpath + '/' + self.ALparamsHash
            return outpath
        elif self.stage=='2':
            inpath = self.ProjectRoot + 'Cache/' + self.FGhash + '/' + self.uTask1path 
            return inpath #shorten path to be nice to windows- also, it is redundant to add new path as if AL changes it will automatically change everything. 
    def requires(self):
        yield self.upstream_task1
        yield self.upstream_task2
    def output(self):
        return luigi.LocalTarget(self.outpath() + '/DETwLabels.csv.gz')
    def run(self):
        
        FGpath = self.ProjectRoot + 'Cache/' + self.FGhash + '/'
        DETpath = self.ProjectRoot + 'Cache/' + self.FGhash + '/' + self.uTask1path + '/'
        GTpath = self.ProjectRoot + 'Cache/' + self.FGhash + '/' + self.uTask2path + '/'
        resultPath =  self.outpath()
        if not os.path.exists(resultPath):
            os.mkdir(resultPath)

        Paths = [FGpath,GTpath,DETpath,resultPath]
        Args = ''
        Params = self.Params

        argParse.run(Program='R',rVers=self.r_version,cmdType=self.system,ProjectRoot=self.ProjectRoot,ProcessID=self.ProcessID,MethodID=self.MethodID,Paths=Paths,Args=Args,Params=Params)


########################
#Det with labels and FE 
########################

class MergeFE_AL(luigi.Task):
    ProjectRoot= luigi.Parameter()
    system= luigi.Parameter()
    r_version=luigi.Parameter()

    
    upstream_task1 = luigi.Parameter()
    upstream_task2 = luigi.Parameter()
    
    FGhash = luigi.Parameter()
    uTask1path = luigi.Parameter()
    uTask2path = luigi.Parameter()
    
    ProcessID = luigi.Parameter()
    MethodID = luigi.Parameter()
    MFAparamsHash=luigi.Parameter()

    def outpath(self):
        inpath = self.ProjectRoot + 'Cache/' + self.FGhash + '/' + self.uTask2path
        outpath =  inpath + '/' + self.MFAparamsHash
        return outpath
    def requires(self):
        yield self.upstream_task1
        yield self.upstream_task2
    def output(self):
        return luigi.LocalTarget(self.outpath() + '/DETwFEwAL.csv.gz')
    def run(self):

        DETwFEpath = self.ProjectRoot + 'Cache/' + self.FGhash + '/' + self.uTask1path + '/'
        DETwALpath = self.ProjectRoot + 'Cache/' + self.FGhash + '/' + self.uTask2path + '/'
        resultPath =  self.outpath()
        print(resultPath)

        if not os.path.exists(resultPath):
            os.mkdir(resultPath)

        Paths = [DETwFEpath,DETwALpath,resultPath]
        Args = ''
        Params = ''

        argParse.run(Program='R',rVers=self.r_version,cmdType=self.system,ProjectRoot=self.ProjectRoot,ProcessID=self.ProcessID,MethodID=self.MethodID,Paths=Paths,Args=Args,Params=Params)


############################################################
#Performance stats based on specification dependent labels
############################################################

class PerfEval1(luigi.Task):
    ProjectRoot= luigi.Parameter()
    system= luigi.Parameter()
    r_version=luigi.Parameter()

    
    upstream_task = luigi.Parameter()

    FGhash = luigi.Parameter()
    uTaskpath = luigi.Parameter()
    FileGroupID = luigi.Parameter()
    ProcessID = luigi.Parameter()

    MethodID = luigi.Parameter()
    PE1paramsHash=luigi.Parameter()

    PE1stage ='1'

    ContPath=luigi.Parameter()

    def outpath(self):
        if self.ContPath=='y':
            inpath = self.ProjectRoot + 'Cache/' + self.FGhash + '/' + self.uTaskpath
            outpath =  inpath + '/' + self.PE1paramsHash
            return outpath
        elif self.ContPath=='n':
            inpath = self.ProjectRoot + 'Cache/' + self.FGhash + '/' + self.uTaskpath
            return inpath
    def requires(self):
        yield self.upstream_task
    def output(self):
        return luigi.LocalTarget(self.outpath() + '/Stats.csv.gz')
    def run(self):

        FGpath = self.ProjectRoot + 'Cache/' + self.FGhash + '/'
        LABpath = FGpath + self.uTaskpath
        INTpath = 'NULL'
        resultPath =  self.outpath()
        if not os.path.exists(resultPath):
            os.mkdir(resultPath)

        Paths = [FGpath,LABpath,INTpath,resultPath]
        Args = [self.FileGroupID,self.PE1stage]
        Params = ''

        argParse.run(Program='R',rVers=self.r_version,cmdType=self.system,ProjectRoot=self.ProjectRoot,ProcessID=self.ProcessID,MethodID=self.MethodID,Paths=Paths,Args=Args,Params=Params)

        #command = '"C:\\Program Files\\R\\R-3.6.1\\bin\\Rscript.exe" -e "source(\'' + ProjectRoot + 'bin/' + self.ProcessID + '/' + self.MethodID +'/'+self.MethodID+'.R\')"'
        #command = command +' '+ FGpath +' '+ LABpath +' '+ INTpath + ' '+ resultPath + ' '+ self.FileGroupID + ' ' + self.PE1stage 

        #subprocess.run(shlex.split(command))

        #print(command)

############################################################
#Performance stats based on specification independent labels
############################################################

class PerfEval2(luigi.Task):
    ProjectRoot= luigi.Parameter()
    system= luigi.Parameter()
    r_version=luigi.Parameter()
    
    upstream_task1 = luigi.Parameter() 
    upstream_task2 = luigi.Parameter() 

    uTask1path = luigi.Parameter()#model output
    uTask2path = luigi.Parameter()#pe1 or edperfeval output

    ProcessID = luigi.Parameter()
    MethodID = luigi.Parameter()
    PE2paramsHash=luigi.Parameter()

    FGhash = luigi.Parameter()
    rootPath=luigi.Parameter()

    def outpath(self):
        if self.rootPath=='Job':
            resultPath = self.ProjectRoot + 'Cache/'+ self.uTask1path+ '/' + self.PE2paramsHash
        elif self.rootPath=='FG':
            resultPath = self.ProjectRoot + 'Cache/'+  self.FGhash + '/'+ self.uTask1path+ '/' + self.PE2paramsHash
        return resultPath
    def requires(self):
        yield self.upstream_task1
        yield self.upstream_task2
    def output(self):
        #there are other outputs here, but the output here is the final saved one (so if R script crashes beforehand should prevent this file being saved) 
        return luigi.LocalTarget(self.outpath() + '/PRcurve_auc.txt')
    def run(self):

        #this is kind of an ugly solution. Should rework eventually so FGhash is part of relative path so we don't get situations like this. 
        StatsPath = self.ProjectRoot + 'Cache/' + '/' +self.uTask2path
        resultPath = self.outpath()

        if self.rootPath=="Job":
            DETpath = self.ProjectRoot + 'Cache/' + self.uTask1path 
        elif self.rootPath=="FG":
            DETpath = self.ProjectRoot + 'Cache/' + self.FGhash + '/' + self.uTask1path 

        rootPath = self.rootPath

        if not os.path.exists(resultPath):
            os.mkdir(resultPath)

        Paths = [DETpath,resultPath,StatsPath,rootPath]
        Args = ''
        Params = ''

        argParse.run(Program='R',rVers=self.r_version,cmdType=self.system,ProjectRoot=self.ProjectRoot,ProcessID=self.ProcessID,MethodID=self.MethodID,Paths=Paths,Args=Args,Params=Params)

#############################################################
#apply cutoff on DETwProbs object, stage represents if it is in FG paths in cache (2) or in job in cache (1)
#############################################################

class ApplyCutoff(luigi.Task):
    ProjectRoot= luigi.Parameter()
    
    upstream_task = luigi.Parameter()
    uTaskpath = luigi.Parameter()

    FGhash = luigi.Parameter()

    CutoffHash= luigi.Parameter()
    Cutoff = luigi.Parameter()

    def outpath(self):
        inpath = self.ProjectRoot + 'Cache/' + self.FGhash + '/' + self.uTaskpath
        outpath =  inpath + '/' + self.CutoffHash
        return outpath
    def requires(self):
        yield self.upstream_task
    def output(self):
        return luigi.LocalTarget(self.outpath() + '/DETwProbs.csv.gz')
    def run(self):

        DETwProbs = pd.read_csv(self.ProjectRoot + 'Cache/' + self.FGhash + '/' + self.uTaskpath + '/DETwProbs.csv.gz',compression='gzip')

        if not os.path.exists(self.outpath()):
            os.mkdir(self.outpath())

        DwPcut = DETwProbs[DETwProbs.probs>=float(self.Cutoff)]
        DwPcut.to_csv(self.outpath() + '/DETwProbs.csv.gz',index=False,compression='gzip')

######################################################################
#Apply a model to data with features, generate probability. 
######################################################################

class ApplyModel(luigi.Task):
    ProjectRoot= luigi.Parameter()
    system= luigi.Parameter()
    r_version=luigi.Parameter()

    upstream_task1 = luigi.Parameter() #DETwFE
    upstream_task2 = luigi.Parameter() #Train model

    FGhash = luigi.Parameter()
    uTask1path = luigi.Parameter()
    uTask1FileName = luigi.Parameter() #'/DETwFeatures.csv.gz' or '/DETwFEwAL.csv.gz'
    uTask2path = luigi.Parameter() #model path 
    
    ProcessID = luigi.Parameter()
    MethodID = luigi.Parameter()

    stage='apply'

    def outpath(self):
        inpath = self.ProjectRoot + 'Cache/' + self.FGhash + '/' + self.uTask1path
        outpath =  inpath + '/' + self.uTask2path
        return outpath
    def requires(self):
        yield self.upstream_task1
        yield self.upstream_task2
    def output(self):
        return luigi.LocalTarget(self.outpath() + '/DETwProbs.csv.gz')
    def run(self):

        DETpath= self.ProjectRoot + 'Cache/' + self.FGhash + '/' + self.uTask1path + '/' + self.uTask1FileName
        FGpath = self.ProjectRoot + 'Cache/' + self.FGhash + '/FileGroupFormat.csv.gz'
        resultPath = self.outpath()
        Mpath= self.ProjectRoot + 'Cache/' + self.uTask2path + '/RFmodel.rds'

        if not os.path.exists(resultPath):
            os.mkdir(resultPath)

        Paths = [DETpath,FGpath,resultPath,Mpath]
        Args = [self.stage]
        Params = ''

        argParse.run(Program='R',rVers=self.r_version,cmdType=self.system,ProjectRoot=self.ProjectRoot,ProcessID=self.ProcessID,MethodID=self.MethodID,Paths=Paths,Args=Args,Params=Params)    

####################################################################################
#Split data with probs and labels back into FG components (used for perf eval 2) 
#####################################################################################

class SplitForPE(luigi.Task):
    ProjectRoot= luigi.Parameter()
    
    upstream_task1 = luigi.Parameter() #will refer to train model 
    uTask1path = luigi.Parameter() #train model object path aka job hash 

    #upstream task 2 is implied from train model (should make a new path if FG path in cache has been cleared, but might want to check that behavior) 
    uTask2path=luigi.Parameter() 
    #goes back to the original cache paths #Cache + FG + ED + FE + DetwFEwAL + SplitForPE

    FGhash = luigi.Parameter()
    FileGroupID = luigi.Parameter() #single value

    def outpath(self):
        return self.ProjectRoot + 'Cache/' + self.FGhash + '/' + self.uTask2path + '/' + self.uTask1path
    def requires(self):
        return self.upstream_task1
    def output(self):
        return luigi.LocalTarget(self.outpath() +'/DETwProbs.csv.gz')
    def run(self): 
        #pseudocode:
        #split dataset into num of workers

        DETwProbs = pd.read_csv(self.ProjectRoot + 'Cache/' + self.uTask1path + '/DETwProbs.csv.gz',compression='gzip')

        if not os.path.exists(self.outpath()):
            os.makedirs(self.outpath(),exist_ok=True)
            
        #This will save subset file in original path. 
        DwPsubset = DETwProbs[DETwProbs.FGID == self.FileGroupID]
        DwPsubset.to_csv(self.outpath() + '/DETwProbs.csv.gz',index=False,compression='gzip')


##################################################################################


#JOBS


##################################################################################

#Performance evalation for a group of event detectors (see EDperfeval.py for example of how to populate params)

class EDperfeval(luigi.Task):

    ProjectRoot=luigi.Parameter()
    system=luigi.Parameter()
    r_version=luigi.Parameter()


    JobName=luigi.Parameter()
    JobHash=luigi.Parameter()
    
    GTparamsHash =luigi.Parameter()

    SoundFileRootDir_Host = luigi.Parameter()

    IDlength = luigi.IntParameter()
    GTfile = luigi.Parameter()
    FGfile = luigi.Parameter()
    FileGroupHashes = luigi.Parameter()
    FileGroupID = luigi.Parameter()

    EDprocess = luigi.Parameter()
    EDsplits = luigi.IntParameter()
    EDcpu = luigi.Parameter()
    EDchunk = luigi.Parameter()
    EDmethodID = luigi.Parameter()
    EDparamString=luigi.Parameter()
    EDparamsHash=luigi.Parameter()
    
    ALprocess = luigi.Parameter()
    ALmethodID = luigi.Parameter()
    ALparamString=luigi.Parameter()
    ALparamsHash=luigi.Parameter()

    ALuTask1path=luigi.Parameter()
    ALuTask2path=luigi.Parameter()

    ALstage='1'

    PE1process = luigi.Parameter()
    PE1methodID = luigi.Parameter()
    PE1paramsHash= luigi.Parameter()

    PE1uTaskpath=luigi.Parameter()

    PE1ContPath=luigi.Parameter()

    WriteToOutputs=luigi.Parameter()

    PE1stage = '2'

    def rootpath(self):
        if self.WriteToOutputs=='y':
            return self.ProjectRoot +'Outputs/' + '/' + self.JobName + '/'
        elif self.WriteToOutputs=='n':
            return self.ProjectRoot + 'Cache/'
    def requires(self):
        for l in range(self.IDlength):
            task1 = FormatFG(FGfile = self.FGfile[l],FGhash = self.FileGroupHashes[l],ProjectRoot=self.ProjectRoot)
            task2 = FormatGT(GTfile=self.GTfile[l],FGhash = self.FileGroupHashes[l],FGfile = self.FGfile[l],GThash=self.GTparamsHash,ProjectRoot=self.ProjectRoot)
            task3 = UnifyED(upstream_task = task1,splits = self.EDsplits,FGhash=self.FileGroupHashes[l],SoundFileRootDir_Host=self.SoundFileRootDir_Host,\
                            EDparamsHash=self.EDparamsHash,Params=self.EDparamString,MethodID=self.EDmethodID,ProcessID=self.EDprocess,CPU=self.EDcpu,\
                            Chunk=self.EDchunk,system=self.system,ProjectRoot=self.ProjectRoot,r_version=self.r_version)
            task4 = AssignLabels(upstream_task1 = task3,upstream_task2 = task2,FGhash=self.FileGroupHashes[l],uTask1path=self.ALuTask1path,uTask2path=self.ALuTask2path,\
                                 ALparamsHash=self.ALparamsHash,MethodID=self.ALmethodID,ProcessID=self.ALprocess,Params=self.ALparamString,stage=self.ALstage,\
                                 system=self.system,ProjectRoot=self.ProjectRoot,r_version=self.r_version)
            task5 = PerfEval1(upstream_task=task4,FGhash=self.FileGroupHashes[l],uTaskpath=self.PE1uTaskpath, PE1paramsHash=self.PE1paramsHash,\
                              FileGroupID=self.FileGroupID[l],MethodID=self.PE1methodID,ProcessID=self.PE1process,ContPath=self.PE1ContPath,system=self.system,ProjectRoot=self.ProjectRoot,\
                              r_version=self.r_version)
            yield task5
    def output(self):
        return luigi.LocalTarget(self.rootpath() + self.JobHash + '/Stats.csv.gz')
    def run(self):
        
        #concatenate outputs and summarize

        dataframes = [None] * self.IDlength
        for k in range(self.IDlength):
            dataframes[k] = pd.read_csv(self.ProjectRoot + 'Cache/' + self.FileGroupHashes[k] + '/' + self.EDparamsHash + '/' + self.ALparamsHash + '/' + self.PE1paramsHash + '/Stats.csv.gz',compression='gzip')
        EDeval = pd.concat(dataframes,ignore_index=True)

        if not os.path.exists(self.rootpath()):
            os.mkdir(self.rootpath())

        resultPath2= self.rootpath() + self.JobHash
        if not os.path.exists(resultPath2):
            os.mkdir(resultPath2)

        EDeval.to_csv(resultPath2 + '/Stats_Intermediate.csv',index=False)
        #send back in to PE1

        FGpath = 'NULL'
        LABpath = 'NULL'
        INTpath = resultPath2 + '/Stats_Intermediate.csv'
        resultPath =  resultPath2 + '/Stats.csv.gz'
        FGID = 'NULL'

        Paths = [FGpath,LABpath,INTpath,resultPath]
        Args = [FGID,self.PE1stage]
        Params = ''

        argParse.run(Program='R',rVers=self.r_version,cmdType=self.system,ProjectRoot=self.ProjectRoot,ProcessID=self.PE1process,MethodID=self.PE1methodID,Paths=Paths,Args=Args,Params=Params)

        os.remove(resultPath2 + '/Stats_Intermediate.csv')

        #if self.WriteToOutputs=='y':
        #    return save params to output folder
        #elif self.WriteToOutputs=='n':
        #    return dont save params

        #copy params to output folder

#Train an RF model. See TrainModel.py for parameters

class TrainModel(luigi.Task):

    ProjectRoot=luigi.Parameter()
    system=luigi.Parameter()
    r_version=luigi.Parameter()
    
    JobName=luigi.Parameter()
    JobHash=luigi.Parameter()
    
    GTparamsHash =luigi.Parameter()

    SoundFileRootDir_Host = luigi.Parameter()

    IDlength = luigi.IntParameter()
    GTfile = luigi.Parameter()
    FGfile = luigi.Parameter()
    FileGroupHashes = luigi.Parameter()
    FileGroupID = luigi.Parameter()

    EDprocess = luigi.Parameter()
    EDsplits = luigi.IntParameter()
    EDcpu = luigi.Parameter()
    EDchunk = luigi.Parameter()
    EDmethodID = luigi.Parameter()
    EDparamString=luigi.Parameter()
    EDparamsHash=luigi.Parameter()

    FEprocess = luigi.Parameter()
    FEmethodID = luigi.Parameter()
    FEcpu = luigi.Parameter()
    FEsplits = luigi.IntParameter()
    FEparamString =luigi.Parameter()
    FEparamsHash = luigi.Parameter()

    FEuTaskpath = luigi.Parameter()
        
    ALprocess = luigi.Parameter()
    ALmethodID = luigi.Parameter()
    ALparamString=luigi.Parameter()
    ALparamsHash=luigi.Parameter()

    ALuTask1path=luigi.Parameter()
    ALuTask2path=luigi.Parameter()

    ALstage='1'

    MFAprocess = luigi.Parameter()
    MFAmethodID = luigi.Parameter()
    MFAparamsHash = luigi.Parameter()

    MFAuTask1path = luigi.Parameter()
    MFAuTask2path = luigi.Parameter()

    TMprocess = luigi.Parameter()
    TMmethodID = luigi.Parameter()
    TMparams = luigi.Parameter()

    stage=luigi.Parameter()
    TM_outName=luigi.Parameter()

    CVcpu=luigi.Parameter()

    def requires(self):
        for l in range(self.IDlength):
            task1 = FormatFG(FGfile = self.FGfile[l],FGhash = self.FileGroupHashes[l],ProjectRoot=self.ProjectRoot)
            task2 = FormatGT(GTfile=self.GTfile[l],FGhash = self.FileGroupHashes[l],FGfile = self.FGfile[l],GThash=self.GTparamsHash,ProjectRoot=self.ProjectRoot)
            task3 = UnifyED(upstream_task = task1,splits = self.EDsplits,FGhash=self.FileGroupHashes[l],SoundFileRootDir_Host=self.SoundFileRootDir_Host,\
                            EDparamsHash=self.EDparamsHash,Params=self.EDparamString,MethodID=self.EDmethodID,ProcessID=self.EDprocess,CPU=self.EDcpu,\
                            Chunk=self.EDchunk,system=self.system,ProjectRoot=self.ProjectRoot,r_version=self.r_version)
            task4 = AssignLabels(upstream_task1 = task3,upstream_task2 = task2,FGhash=self.FileGroupHashes[l],uTask1path=self.ALuTask1path,uTask2path=self.ALuTask2path,\
                                 ALparamsHash=self.ALparamsHash,MethodID=self.ALmethodID,ProcessID=self.ALprocess,Params=self.ALparamString,stage=self.ALstage,\
                                 system=self.system,ProjectRoot=self.ProjectRoot,r_version=self.r_version)
            task5 = UnifyFE(upstream_task = task2,FGhash=self.FileGroupHashes[l],uTaskpath=self.FEuTaskpath,FEparamsHash=self.FEparamsHash,MethodID=self.FEmethodID,\
                            ProcessID=self.FEprocess,Params=self.FEparamString,splits=self.FEsplits,CPU=self.FEcpu,SoundFileRootDir_Host=self.SoundFileRootDir_Host,\
                            system=self.system,ProjectRoot=self.ProjectRoot,r_version=self.r_version)
            task6 = MergeFE_AL(upstream_task1 = task5,upstream_task2 = task4,FGhash=self.FileGroupHashes[l],uTask1path=self.MFAuTask1path,uTask2path=self.MFAuTask2path,\
                               ProcessID=self.MFAprocess,MFAparamsHash=self.MFAparamsHash,MethodID=self.MFAmethodID,system=self.system,ProjectRoot=self.ProjectRoot,\
                               r_version=self.r_version)
            yield task6
    def output(self):
        return luigi.LocalTarget(self.ProjectRoot + 'Cache/' + self.JobHash + '/' + self.TM_outName)
    def run(self):

        #concatenate outputs and summarize

        #load in 
        dataframes = [None] * self.IDlength
        FGdf = [None] * self.IDlength
        for k in range(self.IDlength):
            dataframes[k] = pd.read_csv(self.ProjectRoot + 'Cache/' + self.FileGroupHashes[k] + '/' + self.EDparamsHash + '/' + self.ALparamsHash + '/' + self.MFAparamsHash + '/DETwFEwAL.csv.gz')
            dataframes[k]['FGID'] = pd.Series(self.FileGroupID[k], index=dataframes[k].index)
            
            FGdf[k] = pd.read_csv(self.ProjectRoot + 'Cache/' + self.FileGroupHashes[k] + '/FileGroupFormat.csv.gz')
        TMdat = pd.concat(dataframes,ignore_index=True)
        FGdf = pd.concat(FGdf,ignore_index=True)

        resultPath = self.ProjectRoot + 'Cache/' + self.JobHash 
        if not os.path.exists(resultPath):
            os.mkdir(resultPath)

        TMdat.to_csv(resultPath + '/TM_Intermediate1.csv.gz',index=False,compression='gzip')
        FGdf.to_csv(resultPath + '/FG_Intermediate2.csv.gz',index=False,compression='gzip')

        #
        TMpath = resultPath + '/TM_Intermediate1.csv.gz'
        FGpath = resultPath + '/FG_Intermediate2.csv.gz'
        Mpath = 'NULL'

        Paths = [TMpath,FGpath,resultPath,Mpath]
        Args = [self.stage,self.CVcpu]
        Params = self.TMparams 

        argParse.run(Program='R',rVers=self.r_version,cmdType=self.system,ProjectRoot=self.ProjectRoot,ProcessID=self.TMprocess,MethodID=self.TMmethodID,Paths=Paths,Args=Args,Params=Params)

        #os.remove(resultPath + '/TM_Intermediate1.csv.gz')
        #os.remove(resultPath + '/FG_Intermediate2.csv.gz')

        #copy params to output folder


