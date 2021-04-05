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
    def tplExtract(val,n):
        if(n=='default'):
            #do nothing
            valOut=val
        else:
            valOut=val[n]
        return valOut
##########################################
#parse variable inputs into system command
##########################################

class argParse:
    #I need to do some work on this. Command1 should be split, since the parts of it which depend on OS are serpeate from the relationship of the arguments. 
    def run(cmdType,MethodID,Paths,Args,Params,paramsNames=None,ProcessID=None,Program=None,ProjectRoot=None,rVers=None,dockerVolMounts=None,Wrapper=False):
        #windows cmd just wants all arguments seperated by a space
        if(cmdType=='win'):
            if(Program=='R'):
                #default install vers
                if(Wrapper==False):
                    command1 = '"'+ rVers + '/bin/Rscript.exe" -e "source(\'' + ProjectRoot + 'bin/' + ProcessID + '/' + MethodID +'/'+MethodID+'.R\')"'
                    command2 = ' '.join(Paths) + ' ' + ' '.join(Args) + ' ' + Params

                elif(Wrapper):
                    command1 = '"'+ rVers + '/bin/Rscript.exe" -e "source(\'' + ProjectRoot + 'bin/' + ProcessID + '/' + ProcessID + 'Wrapper.R\')"'
                    command2 = ProjectRoot + ' ' + ' '.join(Paths) + ' ' + ' '.join(Args) #don't think lists need to be flattened, since these should refer to process not the individual methods

                    if not(isinstance(MethodID, list)): #this will be a list if there are multiple methods being passed 
                        command2 = command2 + ' method1 ' + MethodID + ' ' + Params + ' ' + paramsNames
                    else:
                        loopVar = 1
                        for h in range(len(MethodID)):
                            IDit = "method" + str(loopVar)
                            command2 = command2 + ' ' + IDit + ' ' + MethodID[h] + ' ' + Params[h] + ' ' + paramsNames[h]


                #when writing argParse command in python, just make nested list of MethodID, and MethodID params

                #schema for wrapper will be 1: project root, 2: Paths 3: Args 4: "method1" 5:Method1 name 6: method1 params 7: "method2" 8: method2 name 9: method2 params... etc
                #this way, wrapper can get load in relevant paths, but keep methods parsing dynamic for individual methods. Find method params logic is look after methodx for each method to find name and
                #then pass params to method. 
                #paths and args determined by process, such as 

                command = command1 + ' ' + command2
                print(command)
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

#make this class to avoid repetition above when settled on parser. Do this to add to linux as well 

#class makeCommand2:
    #this will determine if needs 
#    def run(MethodID,Paths,Args,Params):

########################
#instinct base fxns
########################

class INSTINCT_Task(luigi.Task):

    ProjectRoot=luigi.Parameter()
    #Add in the below before too long. Will be useful to direct Cache to NAS in some cases. 
    #CacheRoot=luigi.Parameter()

class INSTINCT_Rmethod_Task(INSTINCT_Task):

    system= luigi.Parameter()
    r_version=luigi.Parameter()

########################
#format metadata
########################
        
class FormatFG(INSTINCT_Task):
    
    FileGroupHashes = luigi.Parameter()
    FGfile = luigi.Parameter()
    
    def outpath(self):
        outpath = self.ProjectRoot + 'Cache/' + self.FileGroupHashes
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
    def invoke(obj,n='default'): #shortcut to call this without specifying parameters which typically stay fixed.
        FGfile = Helper.tplExtract(obj.FGfile,n=n)
        FileGroupHashes = Helper.tplExtract(obj.FileGroupHashes,n=n)
        return(FormatFG(FGfile = FGfile,FileGroupHashes = FileGroupHashes,ProjectRoot=obj.ProjectRoot))

class FormatGT(FormatFG):
    GTfile = luigi.Parameter()
    GTparamsHash = luigi.Parameter()
    
    def outpath(self):
        outpath = self.ProjectRoot + 'Cache/' + self.FileGroupHashes + '/' + self.GTparamsHash
        return outpath
    def requires(self):
        return FormatFG.invoke(self)
    def output(self):
        return luigi.LocalTarget(self.outpath() + '/GTFormat.csv.gz')
    def run(self):
        os.mkdir(self.outpath())
        GT = pd.read_csv(self.GTfile)
        GT.to_csv(self.outpath() + '/GTFormat.csv.gz',index=False,compression='gzip')
    def invoke(obj,n='default'):
        FGfile = Helper.tplExtract(obj.FGfile,n=n)
        FileGroupHashes = Helper.tplExtract(obj.FileGroupHashes,n=n)
        GTfile = Helper.tplExtract(obj.GTfile,n=n)
        return(FormatGT(GTfile=GTfile,FileGroupHashes=FileGroupHashes,FGfile=FGfile,GTparamsHash=obj.GTparamsHash,ProjectRoot=obj.ProjectRoot))    
        
###############################################################################
#Defined event detection with abilty to split into chunks w/o affecting outputs
###############################################################################

class SplitED(INSTINCT_Task):
    upstream_task1 = luigi.Parameter()

    EDsplits = luigi.IntParameter()
    splitNum = luigi.IntParameter()
    FileGroupHashes = luigi.Parameter()
    
    def outpath(self):
        outpath = self.ProjectRoot + 'Cache/' + self.FileGroupHashes
        return outpath
    def requires(self):
        return self.upstream_task1
    def output(self):
        return luigi.LocalTarget(self.outpath() + '/FileGroupFormatSplit' + str(self.splitNum+1) + '.csv.gz')
    def run(self):
        #run docker container corresponding to energy detector
        FG_dict = Helper.peek(self.input().fn,fn_type = object,fp_type = object,st_type = object,\
                              dur_type = 'float64',comp_type = 'gzip')
        FG = pd.read_csv(self.input().fn, dtype=FG_dict,compression='gzip')
        if self.EDsplits == 1:
            FG.to_csv(self.outpath() + '/FileGroupFormatSplit1.csv.gz',index=False,compression='gzip')
        #need to test this section to ensure forking works 
        else:
            row_counts = len(FG['DiffTime'])
            breaks = int(row_counts/self.EDsplits)
            blist = numpy.repeat(range(0,self.EDsplits),breaks)
            bdiff = row_counts - len(blist)
            extra = numpy.repeat(self.EDsplits-1,bdiff)
            flist = list(blist.tolist() + extra.tolist())
            FG.loc[[x==self.splitNum for x in flist]].to_csv(self.outpath() + '/FileGroupFormatSplit' + str(self.splitNum+1)\
                                                + '.csv.gz',index=False,compression='gzip')
    def invoke(obj):
        return(SplitED(upstream_task1=obj.upstream_task1,EDsplits=obj.EDsplits,splitNum=obj.splitNum,FileGroupHashes=obj.FileGroupHashes,ProjectRoot=obj.ProjectRoot))
        
class RunED(SplitED,INSTINCT_Rmethod_Task):
    
    EDcpu = luigi.Parameter()
    EDchunk  = luigi.Parameter()
    
    SoundFileRootDir_Host = luigi.Parameter()
    EDparamsHash = luigi.Parameter()
    EDparamString = luigi.Parameter()
    EDparamsNames =luigi.Parameter()

    EDmethodID = luigi.Parameter()
    EDprocess = luigi.Parameter()

    def outpath(self):
        inpath = self.ProjectRoot + 'Cache/' + self.FileGroupHashes  
        outpath =  inpath + '/' + self.EDparamsHash   
        return outpath
    def requires(self):
        return SplitED.invoke(self)
    def output(self):
        return luigi.LocalTarget(self.outpath() + '/EDSplit' + str(self.splitNum+1) + '.csv.gz')
    def run(self):
        #define volume arguments
        FGpath = self.ProjectRoot + 'Cache/' + self.FileGroupHashes +'/'
        ReadFile = 'FileGroupFormatSplit' + str(self.splitNum+1) + '.csv.gz'
        DataPath = self.SoundFileRootDir_Host
        resultPath =  self.outpath()
        if not os.path.exists(resultPath):
            os.mkdir(resultPath)

        Paths = [DataPath,FGpath,resultPath]
        Args = [ReadFile,'1',self.EDcpu,self.EDchunk]

        #just consolidate all command args into one line, args. Send MethodID as a list of length n, if > 1 interpreted as a wrapper
        #argParse.run(Program='R',rVers=self.r_version,cmdType=self.system,

        argParse.run(Program='R',rVers=self.r_version,cmdType=self.system,ProjectRoot=self.ProjectRoot,ProcessID=self.EDprocess,MethodID=self.EDmethodID,Paths=Paths,Args=Args,Params=self.EDparamString,\
                     paramsNames=self.EDparamsNames,Wrapper=True)
        
    def invoke(obj,n):
        return(RunED(upstream_task1=obj.upstream_task1,EDsplits=obj.EDsplits,splitNum=n,FileGroupHashes=obj.FileGroupHashes,SoundFileRootDir_Host=obj.SoundFileRootDir_Host,\
                     EDmethodID=obj.EDmethodID,EDprocess=obj.EDprocess,EDparamsHash=obj.EDparamsHash,EDparamsNames=obj.EDparamsNames,EDparamString=obj.EDparamString,EDcpu=obj.EDcpu,EDchunk=obj.EDchunk,\
                     ProjectRoot=obj.ProjectRoot,system=obj.system,r_version=obj.r_version))
        
class UnifyED(RunED):

    splitNum = None

    def outpath(self):
        inpath = self.ProjectRoot + 'Cache/' + self.FileGroupHashes  
        outpath =  inpath + '/' + self.EDparamsHash   
        return outpath
    def requires(self):
        for k in range(self.EDsplits):
            yield RunED.invoke(self,k)
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
        
        dataframes = [None] * self.EDsplits
        for k in range(self.EDsplits):
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
            FG = pd.read_csv(self.ProjectRoot + 'Cache/' + self.FileGroupHashes +'/FileGroupFormat.csv.gz', dtype=FG_dict, usecols=FG_cols)

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
            Args = [ReadFile,'2',self.EDcpu,self.EDchunk]

            argParse.run(Program='R',rVers=self.r_version,cmdType=self.system,ProjectRoot=self.ProjectRoot,ProcessID=self.EDprocess,MethodID=self.EDmethodID,Paths=Paths,Args=Args,Params=self.EDparamString,\
                     paramsNames=self.EDparamsNames,Wrapper=True)

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
            
        for n in range(self.EDsplits):
            os.remove(self.outpath() + '/EDSplit' + str(n+1) + '.csv.gz') 
        for n in range(self.EDsplits):
            os.remove(self.ProjectRoot + 'Cache/' + self.FileGroupHashes + '/FileGroupFormatSplit' + str(n+1) + '.csv.gz') #might want to rework this to make the files load in the current directory instead of the upstream directory. makes more atomic

    def invoke(obj,upstream1,n='default'):
        FileGroupHashes = Helper.tplExtract(obj.FileGroupHashes,n=n)
        return(UnifyED(upstream_task1 = upstream1,EDsplits = obj.EDsplits,FileGroupHashes=FileGroupHashes,SoundFileRootDir_Host=obj.SoundFileRootDir_Host,\
                       EDparamsHash=obj.EDparamsHash,EDparamsNames=obj.EDparamsNames,EDparamString=obj.EDparamString,EDmethodID=obj.EDmethodID,EDprocess=obj.EDprocess,EDcpu=obj.EDcpu,\
                       EDchunk=obj.EDchunk,system=obj.system,ProjectRoot=obj.ProjectRoot,r_version=obj.r_version))
                
        #merge outputs with ED:
        #pseudo code: for each difftime in patches, eliminate detections in starting in the first half of 1st file and ending in the last half of last file
        #for same file in ED, eliminate detections in first patches file ending in the
        #finally, merge, and eliminate any redundant detections            

####################################################################
#Feature extraction with horizontal scaling and containerized method
####################################################################

class SplitFE(INSTINCT_Task):
    upstream_task1 = luigi.Parameter()
    uTask1path = luigi.Parameter()

    FEsplits = luigi.IntParameter()
    splitNum = luigi.IntParameter()
    FileGroupHashes = luigi.Parameter()

    def requires(self):
        return self.upstream_task1
    def output(self):
        return luigi.LocalTarget(self.uTask1path + '/DETsplitForFE' + str(self.splitNum+1) + '.csv.gz')
    def run(self):
        #pseudocode:
        #split dataset into num of workers

        DETdict = {'StartTime': 'float64', 'EndTime': 'float64','LowFreq': 'int', 'HighFreq': 'int', 'StartFile': 'category','EndFile': 'category'}
        DET = pd.read_csv(self.uTask1path + '/Detections.csv.gz', dtype=DETdict,compression='gzip')
        
        if self.FEsplits == 1:
            DET.to_csv(self.uTask1path + '/DETsplitForFE1.csv.gz',index=False,compression='gzip')
        #need to test this section to ensure forking works 
        else:
            row_counts = len(DET['StartTime'])
            breaks = int(row_counts/self.FEsplits)
            blist = numpy.repeat(range(0,self.FEsplits),breaks)
            bdiff = row_counts - len(blist)
            extra = numpy.repeat(self.FEsplits-1,bdiff)
            flist = list(blist.tolist() + extra.tolist())
            DET.loc[[x==self.splitNum for x in flist]].to_csv(self.uTask1path + '/DETsplitForFE' + str(self.splitNum+1)\
                                                + '.csv.gz',index=False,compression='gzip')
    def invoke(obj):
        return(SplitFE(upstream_task1=obj.upstream_task1,FEsplits=obj.FEsplits,splitNum=obj.splitNum,FileGroupHashes=obj.FileGroupHashes,uTask1path=obj.uTask1path,ProjectRoot=obj.ProjectRoot))
        
class RunFE(SplitFE,INSTINCT_Rmethod_Task): 

    FEcpu = luigi.Parameter()
    
    SoundFileRootDir_Host = luigi.Parameter()
    FEparamsHash=luigi.Parameter()

    FEparamString = luigi.Parameter()
    FEparamsNames = luigi.Parameter()

    FEprocess = luigi.Parameter()
    FEmethodID = luigi.Parameter()

    def requires(self):
        return SplitFE.invoke(self)
    def output(self):
        return luigi.LocalTarget(self.uTask1path + '/' + self.FEparamsHash + '/DETwFeaturesSplit' + str(self.splitNum+1) + '.csv.gz')
    def run(self):
        #define volume arguments
        FGpath = self.ProjectRoot + 'Cache/' + self.FileGroupHashes + '/'
        DETpath = self.uTask1path
        DataPath = self.SoundFileRootDir_Host
        resultPath = self.uTask1path + '/' + self.FEparamsHash
        if not os.path.exists(resultPath):
            os.mkdir(resultPath)

        Paths = [FGpath,DETpath,DataPath,resultPath]
        Args = [str(self.splitNum+1),str(self.FEcpu)]

        argParse.run(Program='R',rVers=self.r_version,cmdType=self.system,ProjectRoot=self.ProjectRoot,ProcessID=self.FEprocess,MethodID=self.FEmethodID,Paths=Paths,Args=Args,Params=self.FEparamString,\
                     paramsNames=self.paramsNames,Wrapper=True)
        
    def invoke(obj,n):
        return(RunFE(upstream_task1=obj.upstream_task1,uTask1path=obj.uTask1path,FEsplits=obj.FEsplits,splitNum=n,FileGroupHashes=obj.FileGroupHashes,SoundFileRootDir_Host=obj.SoundFileRootDir_Host,\
                     FEparamsHash=obj.FEparamsHash,FEparamString=obj.FEparamString,FEparamsNames=obj.FEparamsNames,FEmethodID=obj.FEmethodID,FEprocess=obj.FEprocess,FEcpu=obj.FEcpu,
                     ProjectRoot=obj.ProjectRoot,system=obj.system,r_version=obj.r_version))

class UnifyFE(RunFE):

    splitNum=None

    def outpath(self):
        return self.uTask1path + '/' + self.FEparamsHash 
    def requires(self):
        for k in range(self.FEsplits):
            return RunFE.invoke(self,k)
    def output(self):
        return luigi.LocalTarget(self.outpath() + '/DETwFeatures.csv.gz')
    def run(self):
        #maybe should try to specify data types, but should assume numeric for these?
        dataframes = [None] * self.FEsplits
        for k in range(self.FEsplits):
            dataframes[k] = pd.read_csv(self.outpath() + '/DETwFeaturesSplit' + str(k+1)+ '.csv.gz')
        FE = pd.concat(dataframes,ignore_index=True)
        FE.to_csv(self.outpath() + '/DETwFeatures.csv.gz',index=False,compression='gzip')

        for n in range(self.FEsplits):
            os.remove(self.outpath() + '/DETwFeaturesSplit' + str(n+1) + '.csv.gz')
        for n in range(self.FEsplits):
            os.remove(self.uTask1path + '/DETsplitForFE' + str(n+1) + '.csv.gz')
            
    def invoke(obj,upstream1,n='default'):
        FileGroupHashes = Helper.tplExtract(obj.FileGroupHashes,n=n)
        return(UnifyFE(upstream_task1 = upstream1,FileGroupHashes=FileGroupHashes,uTask1path=upstream1.outpath(),FEparamsHash=obj.FEparamsHash,FEparamsNames=obj.FEparamsNames,FEmethodID=obj.FEmethodID,\
                       FEprocess=obj.FEprocess,FEparamString=obj.FEparamString,FEsplits=obj.FEsplits,FEcpu=obj.FEcpu,SoundFileRootDir_Host=obj.SoundFileRootDir_Host,\
                       system=obj.system,ProjectRoot=obj.ProjectRoot,r_version=obj.r_version)) 

############################################################
#Label detector outputs with GT using containerized method 
############################################################

class AssignLabels(INSTINCT_Rmethod_Task):

    upstream_task1 = luigi.Parameter() #det output
    upstream_task2 = luigi.Parameter() #GT
    uTask1path = luigi.Parameter()
    uTask2path = luigi.Parameter()

    ALparamsHash=luigi.Parameter()
    ALparamString=luigi.Parameter()
    ALprocess = luigi.Parameter()
    ALmethodID = luigi.Parameter()

    ALstage=luigi.Parameter()

    FileGroupHashes=luigi.Parameter()
    
    def outpath(self):
        if self.ALstage=='1':
            return(self.uTask1path  + '/' + self.ALparamsHash)
        elif self.ALstage=='2':
            return(self.uTask1path) #shorten path to be nice to windows- also, it is redundant to add new path as if AL changes it will automatically change everything. 
    def requires(self):
        yield self.upstream_task1
        yield self.upstream_task2
    def output(self):
        return(luigi.LocalTarget(self.outpath() + '/DETwLabels.csv.gz'))
    def run(self):

        FGpath = self.ProjectRoot + 'Cache/' + self.FileGroupHashes + '/'
        DETpath = self.uTask1path + '/'
        GTpath = self.uTask2path + '/'
        resultPath =  self.outpath()
        if not os.path.exists(resultPath):
            os.mkdir(resultPath)

        Paths = [FGpath,GTpath,DETpath,resultPath]
        Args = ''

        argParse.run(Program='R',rVers=self.r_version,cmdType=self.system,ProjectRoot=self.ProjectRoot,ProcessID=self.ALprocess,MethodID=self.ALmethodID,Paths=Paths,Args=Args,Params=self.ALparamString)
        
    def invoke(obj,upstream1,upstream2,ALstageDef=None,n='default'):
        FileGroupHashes = Helper.tplExtract(obj.FileGroupHashes,n=n)
        return(AssignLabels(upstream_task1 = upstream1,FileGroupHashes=FileGroupHashes,upstream_task2 = upstream2,uTask1path=upstream1.outpath(),uTask2path=upstream2.outpath(),\
                            ALparamsHash=obj.ALparamsHash,ALmethodID=obj.ALmethodID,ALprocess=obj.ALprocess,ALparamString=obj.ALparamString,ALstage=ALstageDef,\
                            system=obj.system,ProjectRoot=obj.ProjectRoot,r_version=obj.r_version))

########################
#Det with labels and FE 
########################

class MergeFE_AL(INSTINCT_Rmethod_Task):

    upstream_task1 = luigi.Parameter()
    upstream_task2 = luigi.Parameter()
    uTask1path = luigi.Parameter()
    uTask2path = luigi.Parameter()
    
    FileGroupHashes = luigi.Parameter()
    
    MFAprocess = luigi.Parameter()
    MFAmethodID = luigi.Parameter()
    MFAparamsHash=luigi.Parameter()

    def outpath(self):
        return self.uTask2path + '/' + self.MFAparamsHash
    def requires(self):
        yield self.upstream_task1
        yield self.upstream_task2
    def output(self):
        return luigi.LocalTarget(self.outpath() + '/DETwFEwAL.csv.gz')
    def run(self):

        DETwFEpath = self.uTask1path + '/'
        DETwALpath = self.uTask2path + '/'
        resultPath =  self.outpath()

        if not os.path.exists(resultPath):
            os.mkdir(resultPath)

        Paths = [DETwFEpath,DETwALpath,resultPath]

        argParse.run(Program='R',rVers=self.r_version,cmdType=self.system,ProjectRoot=self.ProjectRoot,ProcessID=self.MFAprocess,MethodID=self.MFAmethodID,Paths=Paths,Args='',Params='')
        
    def invoke(obj,upstream1,upstream2,n='default'):
        FileGroupHashes = Helper.tplExtract(obj.FileGroupHashes,n=n)
        return(MergeFE_AL(upstream_task1 = upstream1,upstream_task2 = upstream2,FileGroupHashes=FileGroupHashes,uTask1path=upstream1.outpath(),uTask2path=upstream2.outpath(),\
                          MFAprocess=obj.MFAprocess,MFAparamsHash=obj.MFAparamsHash,MFAmethodID=obj.MFAmethodID,system=obj.system,ProjectRoot=obj.ProjectRoot,\
                          r_version=obj.r_version))


############################################################
#Performance stats based on specification dependent labels
############################################################

class PerfEval1(INSTINCT_Rmethod_Task):

    upstream_task1 = luigi.Parameter()
    uTask1path = luigi.Parameter()

    FileGroupHashes = luigi.Parameter()
    FileGroupID = luigi.Parameter()
    
    PE1process = luigi.Parameter()
    PE1methodID = luigi.Parameter()
    PE1paramsHash=luigi.Parameter()

    PE1ContPath=luigi.Parameter()

    def outpath(self):
        if self.PE1ContPath=='y':
            return self.uTask1path + '/' + self.PE1paramsHash
        elif self.PE1ContPath=='n':
            return self.uTask1path
    def requires(self):
        yield self.upstream_task1
    def output(self):
        return luigi.LocalTarget(self.outpath() + '/Stats.csv.gz')
    def run(self):

        FGpath = self.ProjectRoot + 'Cache/' + self.FileGroupHashes + '/'
        LABpath = self.uTask1path
        INTpath = 'NULL'
        resultPath =  self.outpath()
        if not os.path.exists(resultPath):
            os.mkdir(resultPath)

        Paths = [FGpath,LABpath,INTpath,resultPath]
        Args = [self.FileGroupID,'1']

        argParse.run(Program='R',rVers=self.r_version,cmdType=self.system,ProjectRoot=self.ProjectRoot,ProcessID=self.PE1process,MethodID=self.PE1methodID,Paths=Paths,Args=Args,Params='')

    def invoke(obj,upstream1,PE1ContPathDef=None,n='default'):
        FileGroupHashes = Helper.tplExtract(obj.FileGroupHashes,n=n)
        FileGroupID = Helper.tplExtract(obj.FileGroupID,n=n)

        return(PerfEval1(upstream_task1=upstream1,FileGroupHashes=FileGroupHashes,uTask1path=upstream1.outpath(),PE1paramsHash=obj.PE1paramsHash,\
                         FileGroupID=FileGroupID,PE1methodID=obj.PE1methodID,PE1process=obj.PE1process,PE1ContPath=PE1ContPathDef,system=obj.system,ProjectRoot=obj.ProjectRoot,\
                         r_version=obj.r_version))

############################################################
#Performance stats based on specification independent labels
############################################################

class PerfEval2(INSTINCT_Rmethod_Task):

    upstream_task1 = luigi.Parameter() 
    upstream_task2 = luigi.Parameter() 

    uTask1path = luigi.Parameter()#model output
    uTask2path = luigi.Parameter()#pe1 or edperfeval output

    PE2process = luigi.Parameter()
    PE2methodID = luigi.Parameter()
    PE2paramsHash=luigi.Parameter()

    FileGroupHashes = luigi.Parameter()

    PE2datType = luigi.Parameter()

    def outpath(self):
        return self.uTask1path+ '/' + self.PE2paramsHash
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

        DETpath = self.uTask1path

        if not os.path.exists(resultPath):
            os.mkdir(resultPath)

        Paths = [DETpath,resultPath,StatsPath,rootPath]
        Args = ''
        Params = ''

        argParse.run(Program='R',rVers=self.r_version,cmdType=self.system,ProjectRoot=self.ProjectRoot,ProcessID=self.ProcessID,MethodID=self.MethodID,Paths=Paths,Args=Args,Params=Params)

    def invoke(obj,upstream1,upstream2,PE2datTypeDef=None,n='default'):
        FileGroupHashes = Helper.tplExtract(obj.FileGroupHashes,n=n) 
        return(PerfEval2(upstream_task1=upstream1,upstream_task2=upstream2,uTask1path=upstream1.outpath(),uTask2path=upstream2.outpath(),PE2process=obj.PE2process,PE2methodID=obj.PE2methodID,\
                          PE2paramsHash=obj.PE2paramsHash,PE2datType=PE2datTypeDef,FileGroupHashes=FileGroupHashes,system=obj.system,ProjectRoot=obj.ProjectRoot,r_version=obj.r_version))

#############################################################
#apply cutoff on DETwProbs object, stage represents if it is in FG paths in cache (2) or in job in cache (1)
#############################################################

class ApplyCutoff(INSTINCT_Task):
    
    upstream_task1 = luigi.Parameter()
    uTask1path = luigi.Parameter()

    FileGroupHashes = luigi.Parameter()

    ACcutoffString = luigi.Parameter()
    ACcutoffHash= luigi.Parameter()

    def outpath(self):
        return self.uTask1path + '/' + self.ACcutoffHash
    def requires(self):
        yield self.upstream_task1
    def output(self):
        return luigi.LocalTarget(self.outpath() + '/DETwProbs.csv.gz')
    def run(self):

        DETwProbs = pd.read_csv(self.uTask1path + '/DETwProbs.csv.gz',compression='gzip')

        if not os.path.exists(self.outpath()):
            os.mkdir(self.outpath())

        DwPcut = DETwProbs[DETwProbs.probs>=float(self.ACcutoffString)]
        DwPcut.to_csv(self.outpath() + '/DETwProbs.csv.gz',index=False,compression='gzip')
    def invoke(obj,upstream1,n='default'):
        FileGroupHashes = Helper.tplExtract(obj.FileGroupHashes,n=n) 
        ApplyCutoff(upstream_task1=upstream1,uTask1path=upstream1.outpath(),FileGroupHashes=FileGroupHashes,ACcutoffHash=obj.ACcutoffHash,ACcutoffString=obj.ACcutoffString,\
                    ProjectRoot=obj.ProjectRoot)

######################################################################
#Apply a model to data with features, generate probability. 
######################################################################

class ApplyModel(INSTINCT_Rmethod_Task):

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

        argParse.run(Program='R',rVers=self.r_version,cmdType=self.system,ProjectRoot=self.ProjectRoot,ProcessID=self.ProcessID,MethodID=self.MethodID,Paths=Paths,Args=Args,Params='')    

####################################################################################
#Split data with probs and labels back into FG components (used for perf eval 2) 
#####################################################################################

class SplitForPE(INSTINCT_Task):

    upstream_task1 = luigi.Parameter() #will refer to train model
    uTask1path = luigi.Parameter() #train model object path aka job hash 

    #upstream task 2 is implied from train model (should make a new path if FG path in cache has been cleared, but might want to check that behavior) 
    SFPEspecialPath=luigi.Parameter() 
    #goes back to the original cache paths #Cache + FG + ED + FE + DetwFEwAL + SplitForPE

    FileGroupHashes = luigi.Parameter()
    FileGroupID = luigi.Parameter() #single value

    def outpath(self):
        return self.ProjectRoot + 'Cache/' + self.FileGroupHashes + '/' + self.SFPEspecialPath + '/' + self.uTask1path #make a special case for this
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
        #this is kind of a weird one. Might not be very modular to other applications as is. 
    def invoke(obj,upstream1,n='default'):
        FileGroupHashes = Helper.tplExtract(obj.FileGroupHashes,n=n)
        return(SplitForPE(upstream_task1=upstream1,uTask1path=obj.TM_JobHash,SFPEspecialPath=obj.SFPEspecialPath,FileGroupID=obj.FileGroupID[n],\
                          FileGroupHashes=FileGroupHashes,ProjectRoot=obj.ProjectRoot))
        
##################################################################################


#JOBS


##################################################################################

#Performance evalation for a group of event detectors (see EDperfeval.py for example of how to populate params)

class EDperfEval(FormatGT,UnifyED,AssignLabels,PerfEval1):
    
    EDpe1_JobName=luigi.Parameter()
    EDpe1_JobHash=luigi.Parameter()
    EDpe1_WriteToOutputs=luigi.Parameter()

    IDlength = luigi.IntParameter()

    #nullify some inherited parameters:
    upstream_task1=None
    upstream_task2=None
    uTask1path=None
    uTask2path=None
    ALstage=None
    PE1ContPath=None

    def outpath(self):
        if self.EDpe1_WriteToOutputs=='y':
            return self.ProjectRoot +'Outputs/' + self.EDpe1_JobName 
        elif self.EDpe1_WriteToOutputs=='n':
            return self.ProjectRoot + 'Cache'
    def requires(self):
        for l in range(self.IDlength):
            task1 = FormatFG.invoke(self,l) 
            task2 = FormatGT.invoke(self,l)
            task3 = UnifyED.invoke(self,task1,l)
            task4 = AssignLabels.invoke(self,task3,task2,ALstageDef='1',n=l)
            task5 = PerfEval1.invoke(self,task4,PE1ContPathDef='y',n=l)

            yield task5
    def output(self):
        return luigi.LocalTarget(self.outpath() + self.EDpe1_JobHash + '/Stats.csv.gz')
    def run(self):
        
        #concatenate outputs and summarize

        dataframes = [None] * self.IDlength
        for k in range(self.IDlength):
            dataframes[k] = pd.read_csv(self.ProjectRoot + 'Cache/' + self.FileGroupHashes[k] + '/' + self.EDparamsHash + '/' + self.ALparamsHash + '/' + self.PE1paramsHash + '/Stats.csv.gz',compression='gzip')
        EDeval = pd.concat(dataframes,ignore_index=True)

        if not os.path.exists(self.outpath()):
            os.mkdir(self.outpath())

        resultPath2= self.outpath() + '/' + self.EDpe1_JobHash
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
        Args = [FGID,'2'] #run second stage of R script 

        argParse.run(Program='R',rVers=self.r_version,cmdType=self.system,ProjectRoot=self.ProjectRoot,ProcessID=self.PE1process,MethodID=self.PE1methodID,Paths=Paths,Args=Args,Params='')

        os.remove(resultPath2 + '/Stats_Intermediate.csv')
        
    def invoke(self):
        return(EDperfEval(EDpe1_JobName=self.EDpe1_JobName,EDpe1_JobHash=self.EDpe1_JobHash,GTparamsHash=self.GTparamsHash,SoundFileRootDir_Host=self.SoundFileRootDir_Host,IDlength=self.IDlength,\
                   GTfile=self.GTfile,FGfile=self.FGfile,FileGroupHashes=self.FileGroupHashes,FileGroupID=self.FileGroupID,EDprocess=self.EDprocess,EDsplits=self.EDsplits,EDcpu=self.EDcpu,\
                   EDchunk=self.EDchunk,EDmethodID=self.EDmethodID,EDparamString=self.EDparamString,EDparamsHash=self.EDparamsHash,EDparamsNames=self.EDparamsNames,ALprocess=self.ALprocess,\
                   ALmethodID=self.ALmethodID,ALparamString=self.ALparamString,ALparamsHash=self.ALparamsHash,\
                   PE1process=self.PE1process,PE1methodID=self.PE1methodID,PE1paramsHash=self.PE1paramsHash,EDpe1_WriteToOutputs=self.EDpe1_WriteToOutputs,\
                   ProjectRoot=self.ProjectRoot,system=self.system,r_version=self.r_version))

#Train an RF model. See TrainModel.py for parameters

class TrainModel(FormatGT,UnifyED,AssignLabels,UnifyFE,MergeFE_AL):

    IDlength = luigi.IntParameter()
    FileGroupID = luigi.Parameter()

    TM_JobName=luigi.Parameter()
    TM_JobHash=luigi.Parameter()

    TMprocess = luigi.Parameter()
    TMmethodID = luigi.Parameter()
    TMparams = luigi.Parameter()

    TMstage=luigi.Parameter()
    TM_outName=luigi.Parameter()

    TMcpu=luigi.Parameter()

    #nullify some inherited parameters:
    upstream_task1=None
    upstream_task2=None
    uTask1path=None
    uTask2path=None
    ALstage=None

    def outpath(self):
        return self.ProjectRoot + 'Cache/' + self.TM_JobHash
    def requires(self):
        for l in range(self.IDlength):
            task1 = FormatFG.invoke(self,l) 
            task2 = FormatGT.invoke(self,l)
            task3 = UnifyED.invoke(self,task1,l)
            task4 = AssignLabels.invoke(self,task3,task2,ALstageDef='1',n=l)
            task5 = UnifyFE.invoke(self,task3,l)
            task6 = MergeFE_AL.invoke(self,task5,task4,l)
            
            yield task6
    def output(self):
        return luigi.LocalTarget(self.outpath() + '/' + self.TM_outName)
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

        resultPath =self.outpath()
        if not os.path.exists(resultPath):
            os.mkdir(resultPath)

        TMdat.to_csv(self.outpath() + '/TM_Intermediate1.csv.gz',index=False,compression='gzip')
        FGdf.to_csv(self.outpath() + '/FG_Intermediate2.csv.gz',index=False,compression='gzip')

        #
        TMpath = resultPath + '/TM_Intermediate1.csv.gz'
        FGpath = resultPath + '/FG_Intermediate2.csv.gz'
        Mpath = 'NULL'

        Paths = [TMpath,FGpath,resultPath,Mpath]
        Args = [self.TMstage,self.TMcpu]

        argParse.run(Program='R',rVers=self.r_version,cmdType=self.system,ProjectRoot=self.ProjectRoot,ProcessID=self.TMprocess,MethodID=self.TMmethodID,Paths=Paths,Args=Args,Params=self.TMparams)

        os.remove(resultPath + '/TM_Intermediate1.csv.gz')
        os.remove(resultPath + '/FG_Intermediate2.csv.gz')

        #copy params to output folder
    def invoke(self):
        return(TrainModel(TM_JobName=self.TM_JobName,TM_JobHash=self.TM_JobHash,GTparamsHash=self.GTparamsHash,SoundFileRootDir_Host=self.SoundFileRootDir_Host,\
                           IDlength=self.IDlength,GTfile=self.GTfile,FGfile=self.FGfile,FileGroupHashes=self.FileGroupHashes,FileGroupID=self.FileGroupID,EDprocess=self.EDprocess,EDsplits=self.EDsplits,\
                           EDcpu=self.EDcpu,EDchunk=self.EDchunk,EDmethodID=self.EDmethodID,EDparamString=self.EDparamString,EDparamsHash=self.EDparamsHash,EDparamsNames=self.EDparamsNames,ALprocess=self.ALprocess,\
                           ALmethodID=self.ALmethodID,ALparamString=self.ALparamString,ALparamsHash=self.ALparamsHash,\
                           FEprocess=self.FEprocess,FEmethodID=self.FEmethodID,FEparamString=self.FEparamString,FEparamsHash=self.FEparamsHash,FEparamsNames=self.FEparamsNames,\
                           FEsplits=self.FEsplits,FEcpu=self.FEcpu,MFAprocess=self.MFAprocess,MFAmethodID=self.MFAmethodID,MFAparamsHash=self.MFAparamsHash,\
                           TMprocess=self.TMprocess,TMmethodID=self.TMmethodID,TMparams=self.TMparams,\
                           TMstage=self.TMstage,TM_outName=self.TM_outName,TMcpu=self.TMcpu,system=self.system,ProjectRoot=self.ProjectRoot,r_version=self.r_version))
    def runJob(self):
        #not sure if this will work, but this would be dank 
        params = self.loadParams(self) #populate all param names onto params object. 
        luigi.build([self.invoke(params)]) #run the job. 


