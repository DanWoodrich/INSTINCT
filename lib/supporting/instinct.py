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
    def peek(self,fn_type,fp_type,st_type,dur_type,comp_type=0):#this is out of date- don't think I need to have fxn variables for how I load in the standard metadata.
        if comp_type != 0:
            heads = pd.read_csv(self, nrows=1,compression=comp_type)
        else:
            heads = pd.read_csv(self, nrows=1)
        heads = heads.columns.tolist()
        heads.remove('FileName')
        heads.remove('StartTime')
        heads.remove('Duration')
        heads.remove('SegStart')
        heads.remove('SegDur')
        hdict = {'FileName': fn_type, 'FullPath': fp_type, 'StartTime': st_type, 'Duration': dur_type, 'SegStart': 'float64', 'SegDur': 'float64'}
        if len(heads) != 0:
            metadict = dict.fromkeys(heads , 'category')
            hdict.update(metadict)
        return hdict
    def hashfile(self,hlen):
        buff_size = 65536
        sha1 = hashlib.sha1()
        with open(self, 'rb') as f:
            while True:
                data = f.read(buff_size)
                if not data:
                    break
                sha1.update(data)
        return sha1.hexdigest()[0:hlen]
    def getDifftime(self):
        #self=self.sort_values(['Deployment','StartTime','SegStart'], ascending=[True,True,True]) #change this behavior 7/12/21, don't re-sort. 
        self['TrueStart'] = self['StartTime']+pd.to_timedelta(self['SegStart'], unit='s')
        self['TrueEnd'] = self['TrueStart']+pd.to_timedelta(self['SegDur'], unit='s')
        #self['EndTime'] = self['StartTime']+pd.to_timedelta(self['Duration'], unit='s')
        self['DiffTime']=pd.to_timedelta(0)
        self['DiffTime'][1:len(self)] = pd.to_timedelta(abs(self['TrueEnd'][0:(len(self['TrueEnd'])-1)] - self['TrueStart'][1:len(self['TrueStart'])].values)) #changes 7/12/21, fix bug where difftime was assigned improperly
        self['DiffTime'] = self['DiffTime']>pd.to_timedelta(2,unit='s') #makes the assumption that if sound files are 1 second apart they are actually consecutive (deals with rounding differences)
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
        self = self.drop(columns='TrueStart')
        self = self.drop(columns='TrueEnd')
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
        if(type(val)==str):
            #do nothing
            valOut=val
        else:
            valOut=val[n]
        return valOut
    def getParamHash2(paramString,hLen):
        hashval = str(hashlib.sha1(paramString.encode('utf8')).hexdigest())[0:hLen]
        return hashval
##########################################
#parse variable inputs into system command
##########################################

class argParse:
    #start reworking this 7/13/21. I don't know what led me to originally use the -e source() syntax, but I think I can get this accomplished with just Rscipt [path].R
    #now. Maybe adding the user path fixed it. Currently testing
    def run(cmdType,MethodID,Paths,Args,Params,paramsNames=None,ProcessID=None,Program=None,ProjectRoot=None,dockerVolMounts=None,Wrapper=False,venv=None,venvName=None):

        SlashPos= [pos for pos, char in enumerate(MethodID) if char == "-"]
        MethodIDcut = MethodID[:SlashPos[-2]]
        
        if(Program=='R'):
            executable1 = 'Rscript '
            executable2 = '.R'
        elif(Program=='Python'):
            executable1 = 'python '
            executable2 = '.py'
        
        if(Wrapper==False):
            command1 = executable1 + ProjectRoot + 'lib/methods/' + ProcessID + '/' + MethodIDcut + '/' + MethodID + executable2
            command2 = ' '.join(Paths) + ' ' + ' '.join(Args) + ' ' + Params

        elif(Wrapper):
            command1 = executable1 + ProjectRoot + 'lib/methods/' + ProcessID + '/' + ProcessID + 'Wrapper' + executable2
            command2 = ProjectRoot + ' ' + ' '.join(Paths) + ' ' + ' '.join(Args) #don't think lists need to be flattened, since these should refer to process not the individual methods

            if not(isinstance(MethodID, list)): #this will be a list if there are multiple methods being passed 
                command2 = command2 + ' method1 ' + MethodID + ' ' + Params + ' ' + paramsNames
            else:
                loopVar = 1
                for h in range(len(MethodID)):
                    IDit = "method" + str(loopVar)
                    command2 = command2 + ' ' + IDit + ' ' + MethodID[h] + ' ' + Params[h] + ' ' + paramsNames[h]

        #schema for wrapper will be 1: project root, 2: Paths 3: Args 4: "method1" 5:Method1 name 6: method1 params 7: "method2" 8: method2 name 9: method2 params... etc
        #this way, wrapper can get load in relevant paths, but keep methods parsing dynamic for individual methods. Find method params logic is look after methodx for each method to find name and
        #then pass params to method. 
        #paths and args determined by process, such as 

        command = command1 + ' ' + command2

        if(venv!=None):
            print("******************************\nActivating virtual environment " + venvName + " with " + venv + "\n******************************")
            if(venv=='Conda'):
                command_venv = venv + ' activate ' + venvName + ' & '
                command = command_venv + command #append venv call to start of command. This assumes conda venv is set up to work on command line.
            else:
                print("VENV NOT YET CONFIGURED TO WORK ON BASE VENV")
        
        print("******************************\nRunning " + Program + " method " + MethodID + " for process " + ProcessID + "\n******************************")

        print("******************************\nCommand params (can copy and paste): " + command2 +"\n******************************")

        subprocess.run(command)
            

#make this class to avoid repetition above when settled on parser. Do this to add to linux as well 

#class makeCommand2:
    #this will determine if needs 
#    def run(MethodID,Paths,Args,Params):

########################
#instinct base fxns
########################

class INSTINCT_Task(luigi.Task):

    ProjectRoot=luigi.Parameter()
    #standard outpath
    def outpath(self):
        return self.upstream_task1.outpath() + '/' + self.hashProcess()
    def requires(self):
        return self.upstream_task1
    #Add in the below before too long. Will be useful to direct Cache to NAS in some cases. 
    #CacheRoot=luigi.Parameter()

class INSTINCT_detTask(INSTINCT_Task): #for task types that modifying detection .csvs

    def output(self):
        return luigi.LocalTarget(self.outpath() + '/DETx.csv.gz')

class INSTINCT_Rmethod_Task(INSTINCT_Task):

    system= luigi.Parameter()

class Comb4Standard(luigi.Task):
    loopVar = luigi.Parameter()
    CacheRoot = luigi.Parameter()

    def hashProcess(self):
        #this is just composed of the component hashes (PE1, method being run here, is accounted for in pipeline).
        hashStrings = [None] * self.loopVar
        for l in range(self.loopVar):
            tasks = self.pipelineMap(l)
            taskStr = []
            for f in range(len(tasks)):
                taskStr.extend([tasks[f].hashProcess()])
            
            hashStrings[l] = ' '.join(taskStr)
    
        return Helper.getParamHash2(' '.join(hashStrings),6)
    def requires(self):
        for l in range(self.loopVar):
            tasks = self.pipelineMap(l)

            yield tasks[len(tasks)-1]
            #concatenate outputs and summarize
    def outpath(self):
        return self.CacheRoot + 'Cache/' + self.hashProcess()
    def output(self):
        return luigi.LocalTarget(self.outpath() + '/' + self.fileName)   
    def run(self):
        
        dataframes = [None] * self.loopVar
        for k in range(self.loopVar):
            tasks=self.pipelineMap(k)
            dataframes[k] = pd.read_csv(tasks[len(tasks)-1].outpath() + '/' + self.fileName,compression='gzip')
        Dat = pd.concat(dataframes,ignore_index=True)

        resultPath = self.outpath()

        if not os.path.exists(resultPath):
            os.mkdir(resultPath)

        Dat.to_csv(resultPath + '/' + self.fileName,index=False,compression="gzip")

class Dummy(INSTINCT_Task):

    CacheRoot = luigi.Parameter()

    def hashProcess(self):
        return None
    def outpath(self):
        return self.CacheRoot + 'Cache/'
    def requires(self):
        return None
    def complete(self):
        return True #this just tells luigi not to run the task
    def invoke(obj):
        return(Dummy(ProjectRoot=obj.ProjectRoot,CacheRoot=obj.CacheRoot))
    
########################
#format metadata
########################
        
class FormatFG(INSTINCT_Task):

    upstream_task1 = luigi.Parameter()
    FGfile = luigi.Parameter()
    SoundFileRootDir_Host_Raw=luigi.Parameter()
    decimatedata = luigi.Parameter()
    FGparamString = luigi.Parameter()
    FGmethodID = luigi.Parameter()
    CacheRoot = luigi.Parameter()

    def hashProcess(self):
        hashLength = 12
        if os.path.isfile(self.FGfile):
            return Helper.hashfile(self.FGfile,hashLength)
        else: 
            return "not yet created" #I think this should just tell the scheduler the task is incomplete, without causing errors...?
    #do not use upstream outpath(), breaking convention. Use Cache. 
    def outpath(self):
        return self.CacheRoot + 'Cache/' + self.hashProcess()
    def output(self):
        return luigi.LocalTarget(self.outpath() + '/FileGroupFormat.csv.gz')
    def run(self):
        #supports additional metadata fields
        FG_dict = Helper.peek(self.FGfile,fn_type = object,fp_type = object,st_type = object,dur_type = 'float64')
        FG = pd.read_csv(self.FGfile, dtype=FG_dict)
        FG['StartTime'] = pd.to_datetime(FG['StartTime'], format='%y%m%d-%H%M%S')
        #import code
        #code.interact(local=locals())
        FG=Helper.getDifftime(FG)

        if not os.path.exists(self.outpath()):
            os.mkdir(self.outpath())

        if self.decimatedata == 'y':
            #if decimating, run decimate. Check will matter in cases where MATLAB supporting library is not installed.
            #note that this can be pretty slow if not on a VM! Might want to figure out another way to perform this
            #to speed up if running on slow latency.

            FullFilePaths = FG['FullPath'].astype('str') + FG['FileName'].astype('str')

            #remove duplicates
            FullFilePaths=pd.Series(FullFilePaths.unique())

            ffpPath=self.outpath() + '/FullFilePaths.csv'

            FullFilePaths.to_csv(ffpPath,index=False,header = None) #don't do gz since don't want to deal with it in MATLAB!

            #at a later date, integrate this with argparse
            command = self.ProjectRoot + "bin/FormatFG/" + self.FGmethodID + "/" + self.FGmethodID + ".exe" + ' ' + self.SoundFileRootDir_Host_Raw + ' ' + ffpPath + ' ' + self.FGparamString 
            print(command)
            
            os.system(command)

            os.remove(ffpPath)

            FG.to_csv(self.outpath() + '/FileGroupFormat.csv.gz',index=False,compression='gzip')
        else:
            #do it this way, so that task will not 'complete' if decimation is on and doesn't work
            FG.to_csv(self.outpath() + '/FileGroupFormat.csv.gz',index=False,compression='gzip')
        

    def invoke(obj,upstream1=None,n='default',src="GT"): #shortcut to call this without specifying parameters which typically stay fixed.
        if src == "GT":
            FGfile=obj.FGfile
        elif src == "n_":
            FGfile=obj.n_FGfile
        FGfile = Helper.tplExtract(FGfile,n=n)
        if upstream1==None:
            upstream1=Dummy.invoke(obj)
        return(FormatFG(upstream_task1 = upstream1, FGfile = FGfile,ProjectRoot=obj.ProjectRoot,SoundFileRootDir_Host_Raw=obj.SoundFileRootDir_Host_Raw,\
                        FGparamString=obj.FGparamString,FGmethodID=obj.FGmethodID,decimatedata=obj.decimatedata,CacheRoot=obj.CacheRoot))

class FormatGT(INSTINCT_Task):
    
    GTfile = luigi.Parameter()
    upstream_task1 = luigi.Parameter()

    def hashProcess(self):
        hashLength = 6
        #this is a little goofy...
        if os.path.isfile(self.GTfile):
            return Helper.hashfile(self.GTfile,hashLength)
        else: 
            GT = pd.DataFrame(columns = ["StartTime","EndTime","LowFreq","HighFreq","StartFile","EndFile","label","Type","SignalCode"])
            #this is a side effect... be careful...
            GT.to_csv(self.GTfile,index=False)
            return Helper.hashfile(self.GTfile,hashLength)
    def output(self):
        return luigi.LocalTarget(self.outpath() + '/DETx.csv.gz')
    def run(self):

        #change this to make a new GT if one doesn't exist. 
        if not os.path.exists(self.outpath()):
            os.mkdir(self.outpath())

        GT = pd.read_csv(self.GTfile)

        GT.to_csv(self.outpath() + '/DETx.csv.gz',index=False,compression='gzip')
    def invoke(obj,upstream1,n='default',src="GT"):
        if src == "GT":
            GTfile=obj.GTfile
        elif src == "n_":
            GTfile=obj.n_GTfile
        GTfile = Helper.tplExtract(GTfile,n=n)
        return(FormatGT(upstream_task1=upstream1,GTfile=GTfile,ProjectRoot=obj.ProjectRoot)) 
        
###############################################################################
#Defined event detection with abilty to split into chunks w/o affecting outputs
###############################################################################

class SplitED(luigi.Task):
    
    upstream_task1 = luigi.Parameter() #FG

    EDsplits = luigi.IntParameter()
    splitNum = luigi.IntParameter()

    def requires(self):
        return self.upstream_task1
    def outpath(self):
        return self.upstream_task1.outpath()
    def output(self):
        return luigi.LocalTarget(self.outpath() + '/FileGroupFormat' + str(self.splitNum+1) + '.csv.gz')
    def run(self):

        inFile = self.upstream_task1.outpath() + '/FileGroupFormat.csv.gz'
        
        FG_dict = Helper.peek(inFile,fn_type = object,fp_type = object,st_type = object,\
                              dur_type = 'float64',comp_type = 'gzip')
        FG = pd.read_csv(inFile, dtype=FG_dict,compression='gzip')
        if self.EDsplits == 1:
            FG.to_csv(self.outpath() + '/FileGroupFormat1.csv.gz',index=False,compression='gzip')
        #need to test this section to ensure forking works 
        else:
            row_counts = len(FG['DiffTime'])
            breaks = int(row_counts/self.EDsplits)
            blist = numpy.repeat(range(0,self.EDsplits),breaks)
            bdiff = row_counts - len(blist)
            extra = numpy.repeat(self.EDsplits-1,bdiff)
            flist = list(blist.tolist() + extra.tolist())
            FG.loc[[x==self.splitNum for x in flist]].to_csv(self.outpath() + '/FileGroupFormat' + str(self.splitNum+1)\
                                                + '.csv.gz',index=False,compression='gzip')
    def invoke(obj):
        return(SplitED(upstream_task1=obj.upstream_task1,EDsplits=obj.EDsplits,splitNum=obj.splitNum))
        
class RunED(SplitED,INSTINCT_Rmethod_Task):
    
    EDcpu = luigi.Parameter()
    EDchunk  = luigi.Parameter()
    
    SoundFileRootDir_Host_Dec = luigi.Parameter()
    EDparamString = luigi.Parameter()
    EDparamNames =luigi.Parameter()

    EDmethodID = luigi.Parameter()
    EDprocess = luigi.Parameter()

    def hashProcess(self):
        hashLength = 6 
        EDparamsHash = Helper.getParamHash2(self.EDparamString + ' ' + self.EDmethodID,hashLength)
        return EDparamsHash
    def outpath(self):
        return self.upstream_task1.outpath() + '/' + self.hashProcess()
    def requires(self):
        return SplitED.invoke(self)
    def output(self):
        return luigi.LocalTarget(self.outpath() + '/DETx' + str(self.splitNum+1) + 'ED.csv.gz') #change this to not be same output as FE input. Change made July 12, backwards incompatible with naming before. 
    def run(self):
        #define volume arguments
        FGpath = self.upstream_task1.outpath() +'/'
        ReadFile = 'FileGroupFormat' + str(self.splitNum+1) + '.csv.gz'
        DataPath = self.SoundFileRootDir_Host_Dec
        resultPath =  self.outpath()
        if not os.path.exists(resultPath):
            os.mkdir(resultPath)

        Paths = [DataPath,FGpath,resultPath]
        Args = [ReadFile,'1',self.EDcpu,self.EDchunk]

        argParse.run(Program='R',cmdType=self.system,ProjectRoot=self.ProjectRoot,ProcessID=self.EDprocess,MethodID=self.EDmethodID,Paths=Paths,Args=Args,Params=self.EDparamString,\
                     paramsNames=self.EDparamNames,Wrapper=True)
        
    def invoke(obj,n):
        return(RunED(upstream_task1=obj.upstream_task1,EDsplits=obj.EDsplits,splitNum=n,SoundFileRootDir_Host_Dec=obj.SoundFileRootDir_Host_Dec,\
                     EDmethodID=obj.EDmethodID,EDprocess=obj.EDprocess,EDparamNames=obj.EDparamNames,EDparamString=obj.EDparamString,EDcpu=obj.EDcpu,EDchunk=obj.EDchunk,\
                     ProjectRoot=obj.ProjectRoot,system=obj.system))
        
class UnifyED(RunED):

    splitNum = None

    def requires(self):
        for k in range(self.EDsplits):
            yield RunED.invoke(self,k)
    def output(self):
        return luigi.LocalTarget(self.outpath() + '/DETx.csv.gz')
    def run(self):
        #this task will perform several functions:
        #combine the split data,
        #assess where difftimes were broken,
        #subset data to these times,
        #pass subset data to ED container to rerun
        #merge outputs with original df to make final df
        #save df

        #define data types
        EDdict = {'StartTime': 'float64', 'EndTime': 'float64','LowFreq': 'float64', 'HighFreq': 'float64', 'StartFile': 'category','EndFile': 'category','ProcessTag': 'category'}
        
        dataframes = [None] * self.EDsplits
        for k in range(self.EDsplits):
            dataframes[k] = pd.read_csv(self.outpath() +'/DETx' + str(k+1) + 'ED.csv.gz',dtype=EDdict)
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
            ED.to_csv(self.outpath() + '/DETx.csv.gz',index=False,compression='gzip')
        else:

            #awesome code for debuging below###
            
            ###

            #This section uses logic that Startfile corresponds to the data that needs to be redone, which it does not. Would need to
            #add in a character column which is startfile + startseg and uSse the same logic.
            #But since I am not using this right now anyways, save for later since it's not worth testing.


            EDfin = ED.loc[indecesED._values]
            #reduce this to just file names to pass to Energy detector (FG style)


            #load in the processed FG


            FG_cols = ['FileName','FullPath','StartTime','Duration','DiffTime','Deployment','SegStart','SegDur']

            FG_dict = {'FileName': 'string','FullPath': 'category', 'StartTime': 'string','Duration': 'int','Deployment':'string','SegStart':'int','SegDur':'int','DiffTime':'int'}
            FG = pd.read_csv(self.upstream_task1.outpath() +'/FileGroupFormat.csv.gz', dtype=FG_dict, usecols=FG_cols)

            #retain only the file names, and use these to subset original FG
            #subset with DiffTime
            FG = FG[FG.DiffTime.isin(EDfin['ProcessTag2'].astype('int32'))&FG.FileName.isin(EDfin['StartFile'])] #subset based on both of these: if a long difftime, will only
            #take the relevant start files, but will also go shorter than two files in the case of longer segments.
            
            #recalculate difftime based on new files included. <- metacomment: not sure why we need to do this? 
            FG['StartTime'] = pd.to_datetime(FG['StartTime'], format='%Y-%m-%d %H:%M:%S')
            FG = Helper.getDifftime(FG)
            
            #save FG
            FG.to_csv(self.outpath() + '/EDoutCorrect.csv.gz',index=False,compression='gzip')

            FGpath = self.outpath()
            ReadFile = 'EDoutCorrect.csv.gz'
            DataPath = self.SoundFileRootDir_Host_Dec
            resultPath =  self.outpath()

            Paths = [DataPath,FGpath,resultPath]
            Args = [ReadFile,'2',self.EDcpu,self.EDchunk]

            argParse.run(Program='R',cmdType=self.system,ProjectRoot=self.ProjectRoot,ProcessID=self.EDprocess,MethodID=self.EDmethodID,Paths=Paths,Args=Args,Params=self.EDparamString,\
                     paramsNames=self.EDparamNames,Wrapper=True)

            #drop process data from ED
            ED = ED.drop(columns="ProcessTag")
            ED = ED.drop(columns="ProcessTag2")

            EDdict2 = {'StartTime': 'float64', 'EndTime': 'float64','LowFreq': 'float64', 'HighFreq': 'float64', 'StartFile': 'category','EndFile': 'category','DiffTime': 'int'}
            #now load in result,
            EDpatches = pd.read_csv(FGpath+'/DETx.csv.gz',dtype=EDdict2)
            PatchList = [None] * len(EDpatches['DiffTime'].unique().tolist())

            #import code
            #code.interact(local=locals())
            
            for n in range(len(EDpatches['DiffTime'].unique().tolist())):
                #EDpatchN= EDpatches[[x == EDpatches['DiffTime'].unique().tolist()[n] for x in EDpatches['DiffTime']]] #this line looks like a problem for larger datasets. Try 1st answer on this thread instead of this line:
                #https://stackoverflow.com/questions/21738882/fast-pandas-filtering. This appears much faster, replaced. 

                nPatch = [EDpatches['DiffTime'].unique().tolist()[n]]
                EDpatchN=EDpatches.loc[EDpatches['DiffTime'].isin(nPatch),]
                FGpatch = FG[FG['DiffTime']==(n+1)]
                FirstFile = EDpatchN.iloc[[0]]['StartFile'].astype('string').iloc[0]
                LastFile = EDpatchN.iloc[[-1]]['StartFile'].astype('string').iloc[0]

                BeginRangeStart= FGpatch.iloc[0]['SegStart']
                BeginRangeEnd = BeginRangeStart+FGpatch.iloc[0]['SegDur']/2

                LastRangeStart= FGpatch.iloc[-1]['SegStart']
                LastRangeEnd = LastRangeStart+FGpatch.iloc[-1]['SegDur']/2

                EDpatchN = EDpatchN[((EDpatchN['StartTime'] > BeginRangeEnd) & (EDpatchN['StartFile'] == FirstFile)) | (EDpatchN['StartFile'] != FirstFile)]
                EDpatchN = EDpatchN[((EDpatchN['StartTime'] < LastRangeEnd) & (EDpatchN['StartFile'] == LastFile)) | (EDpatchN['StartFile'] != LastFile)]

                EDpatchN=EDpatchN.drop(columns="DiffTime")

                #import code
                #code.interact(local=locals())

                #subset ED
                ED1 = ED.copy()[(ED['StartTime'] <= BeginRangeEnd) & (ED['StartFile'] == FirstFile)] #get all before patch
                ED2 = ED.copy()[(ED['StartTime'] >= LastRangeEnd) & (ED['StartFile'] == LastFile)]         #get all after patch
                ED3 = ED.copy()[(ED['StartFile'] != FirstFile) & (ED['StartFile'] != LastFile)]

                ED = pd.concat([ED1,ED2,ED3],ignore_index=True)

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
            ED = ED.sort_values(['StartFile','StartTime'], ascending=[True,True])

            os.remove(FGpath + '/DETx.csv.gz') 

            ED.to_csv(FGpath + '/DETx.csv.gz',index=False,compression='gzip')

            os.remove(FGpath + '/EDoutCorrect.csv.gz')
            
        for n in range(self.EDsplits):
            os.remove(self.outpath() + '/DETx' + str(n+1) + 'ED.csv.gz') 
        for n in range(self.EDsplits):
            os.remove(self.upstream_task1.outpath() + '/FileGroupFormat' + str(n+1) + '.csv.gz') #might want to rework this to make the files load in the current directory instead of the upstream directory. makes more atomic

    def invoke(obj,upstream1):
        return(UnifyED(upstream_task1 = upstream1,EDsplits = obj.EDsplits,SoundFileRootDir_Host_Dec=obj.SoundFileRootDir_Host_Dec,EDparamNames=obj.EDparamNames,\
                       EDparamString=obj.EDparamString,EDmethodID=obj.EDmethodID,EDprocess=obj.EDprocess,EDcpu=obj.EDcpu,\
                       EDchunk=obj.EDchunk,system=obj.system,ProjectRoot=obj.ProjectRoot))
                
        #merge outputs with ED:
        #pseudo code: for each difftime in patches, eliminate detections in starting in the first half of 1st file and ending in the last half of last file
        #for same file in ED, eliminate detections in first patches file ending in the
        #finally, merge, and eliminate any redundant detections            

####################################################################
#Feature extraction with horizontal scaling and containerized method
####################################################################

class SplitFE(luigi.Task):
    upstream_task1 = luigi.Parameter()
    upstream_task2 = luigi.Parameter()


    FEsplits = luigi.IntParameter()
    splitNum = luigi.IntParameter()

    def outpath(self):
        return self.upstream_task1.outpath()
    def requires(self):
        yield self.upstream_task1
        return self.upstream_task2
    def output(self):
        return luigi.LocalTarget(self.upstream_task1.outpath() + '/DETx' + str(self.splitNum+1) + '.csv.gz')
    def run(self):
        #pseudocode:
        #split dataset into num of workers

        DETdict = {'StartTime': 'float64', 'EndTime': 'float64','LowFreq': 'float64', 'HighFreq': 'float64', 'StartFile': 'category','EndFile': 'category'}
        DET = pd.read_csv(self.upstream_task1.outpath() + '/DETx.csv.gz', dtype=DETdict,compression='gzip')
        
        if self.FEsplits == 1:
            DET.to_csv(self.upstream_task1.outpath() + '/DETx1.csv.gz',index=False,compression='gzip')
        #need to test this section to ensure forking works 
        else:
            row_counts = len(DET['StartTime'])
            breaks = int(row_counts/self.FEsplits)
            blist = numpy.repeat(range(0,self.FEsplits),breaks)
            bdiff = row_counts - len(blist)
            extra = numpy.repeat(self.FEsplits-1,bdiff)
            flist = list(blist.tolist() + extra.tolist())
            DET.loc[[x==self.splitNum for x in flist]].to_csv(self.upstream_task1.outpath() + '/DETx' + str(self.splitNum+1)\
                                                + '.csv.gz',index=False,compression='gzip')
    def invoke(obj):
        return(SplitFE(upstream_task1=obj.upstream_task1,upstream_task2=obj.upstream_task2,FEsplits=obj.FEsplits,splitNum=obj.splitNum))
        
class RunFE(SplitFE,INSTINCT_Rmethod_Task):

    FEcpu = luigi.Parameter()
    
    SoundFileRootDir_Host_Dec = luigi.Parameter()

    FEparamString = luigi.Parameter()
    FEparamNames = luigi.Parameter()

    FEprocess = luigi.Parameter()
    FEmethodID = luigi.Parameter()
    
    def hashProcess(self):
        hashLength = 6 
        FEparamsHash = Helper.getParamHash2(self.FEparamString + ' ' + self.FEmethodID + ' ' + self.upstream_task2.hashProcess(),hashLength)
        return FEparamsHash
    def outpath(self):
        return self.upstream_task1.outpath() + '/' + self.hashProcess()
    def requires(self):
        return SplitFE.invoke(self)
    def output(self):
        return luigi.LocalTarget(self.outpath() + '/DETx' + str(self.splitNum+1) + '.csv.gz')
    def run(self):
        #define volume arguments
        FGpath = self.upstream_task2.outpath() + '/'
        DETpath = self.upstream_task1.outpath()
        DataPath = self.SoundFileRootDir_Host_Dec
        resultPath = self.outpath()
        if not os.path.exists(resultPath):
            os.mkdir(resultPath)

        Paths = [FGpath,DETpath,DataPath,resultPath]
        Args = [str(self.splitNum+1),str(self.FEcpu)]

        argParse.run(Program='R',cmdType=self.system,ProjectRoot=self.ProjectRoot,ProcessID=self.FEprocess,MethodID=self.FEmethodID,Paths=Paths,Args=Args,Params=self.FEparamString,\
                     paramsNames=self.FEparamNames,Wrapper=True)
        
    def invoke(obj,n):
        return(RunFE(upstream_task1=obj.upstream_task1,upstream_task2=obj.upstream_task2,FEsplits=obj.FEsplits,splitNum=n,\
                     SoundFileRootDir_Host_Dec=obj.SoundFileRootDir_Host_Dec,FEparamString=obj.FEparamString,\
                     FEparamNames=obj.FEparamNames,FEmethodID=obj.FEmethodID,FEprocess=obj.FEprocess,FEcpu=obj.FEcpu,\
                     ProjectRoot=obj.ProjectRoot,system=obj.system))

class UnifyFE(RunFE):

    splitNum=None

    def requires(self):
        for k in range(self.FEsplits):
            return RunFE.invoke(self,k)
    def output(self):
        return luigi.LocalTarget(self.outpath() + '/DETx.csv.gz')
    def run(self):
        #maybe should try to specify data types, but should assume numeric for these?
        dataframes = [None] * self.FEsplits
        for k in range(self.FEsplits):
            dataframes[k] = pd.read_csv(self.outpath() + '/DETx' + str(k+1)+ '.csv.gz')
        FE = pd.concat(dataframes,ignore_index=True)
        FE.to_csv(self.outpath() + '/DETx.csv.gz',index=False,compression='gzip')

        for n in range(self.FEsplits):
            os.remove(self.outpath() + '/DETx' + str(n+1) + '.csv.gz')
        for n in range(self.FEsplits):
            os.remove(self.upstream_task1.outpath() + '/DETx' + str(n+1) + '.csv.gz')
            
    def invoke(obj,upstream1,upstream2):
        return(UnifyFE(upstream_task1 = upstream1,upstream_task2 = upstream2,\
                       FEparamNames=obj.FEparamNames,FEmethodID=obj.FEmethodID,FEprocess=obj.FEprocess,FEparamString=obj.FEparamString,FEsplits=obj.FEsplits,FEcpu=obj.FEcpu,\
                       SoundFileRootDir_Host_Dec=obj.SoundFileRootDir_Host_Dec,system=obj.system,ProjectRoot=obj.ProjectRoot)) 

############################################################
#Label detector outputs with GT using containerized method 
############################################################

class AssignLabels(INSTINCT_Rmethod_Task):

    upstream_task1 = luigi.Parameter() #DETx
    upstream_task2 = luigi.Parameter() #GT
    upstream_task3 = luigi.Parameter() #FG 

    ALparamString=luigi.Parameter()
    ALprocess = luigi.Parameter()
    ALmethodID = luigi.Parameter()
    
    def hashProcess(self):
        hashLength = 6 
        ALparamsHash = Helper.getParamHash2(self.ALparamString + ' ' + self.ALmethodID + ' ' + self.upstream_task2.hashProcess()+ ' ' + self.upstream_task3.hashProcess(),hashLength)
        return ALparamsHash
    def requires(self):
        yield self.upstream_task1
        yield self.upstream_task2
        yield self.upstream_task3
    def output(self):
        return(luigi.LocalTarget(self.outpath() + '/DETx.csv.gz'))
    def run(self):

        FGpath = self.upstream_task3.outpath() + '/'
        DETpath = self.upstream_task1.outpath() + '/'
        GTpath = self.upstream_task2.outpath() + '/'
        resultPath =  self.outpath()
        if not os.path.exists(resultPath):
            os.mkdir(resultPath)

        Paths = [FGpath,GTpath,DETpath,resultPath]
        Args = ''

        argParse.run(Program='R',cmdType=self.system,ProjectRoot=self.ProjectRoot,ProcessID=self.ALprocess,MethodID=self.ALmethodID,Paths=Paths,Args=Args,Params=self.ALparamString)
        
    def invoke(obj,upstream1,upstream2,upstream3):
        return(AssignLabels(upstream_task1 = upstream1,upstream_task2 = upstream2,upstream_task3 = upstream3,\
                            ALmethodID=obj.ALmethodID,ALprocess=obj.ALprocess,ALparamString=obj.ALparamString,\
                            system=obj.system,ProjectRoot=obj.ProjectRoot))

########################
#Det with labels and FE 
########################

class MergeFE_AL(INSTINCT_Rmethod_Task):

    upstream_task1 = luigi.Parameter()
    upstream_task2 = luigi.Parameter()
    
    MFAprocess = luigi.Parameter()
    MFAmethodID = luigi.Parameter()
    
    def hashProcess(self):
        hashLength = 6 
        MFAparamsHash = Helper.getParamHash2(self.MFAmethodID + ' ' + self.upstream_task1.hashProcess()+ ' ' + self.upstream_task2.hashProcess(),hashLength)
        return MFAparamsHash
    def requires(self):
        yield self.upstream_task1
        yield self.upstream_task2
    def output(self):
        return luigi.LocalTarget(self.outpath() + '/DETx.csv.gz')
    def run(self):

        DETwALpath = self.upstream_task1.outpath()+ '/'
        DETwFEpath = self.upstream_task2.outpath() + '/'
        resultPath =  self.outpath()

        if not os.path.exists(resultPath):
            os.mkdir(resultPath)

        Paths = [DETwFEpath,DETwALpath,resultPath]

        argParse.run(Program='R',cmdType=self.system,ProjectRoot=self.ProjectRoot,ProcessID=self.MFAprocess,MethodID=self.MFAmethodID,Paths=Paths,Args='',Params='')
        
    def invoke(obj,upstream1,upstream2):
        return(MergeFE_AL(upstream_task1 = upstream1,upstream_task2 = upstream2,\
                          MFAprocess=obj.MFAprocess,MFAmethodID=obj.MFAmethodID,system=obj.system,ProjectRoot=obj.ProjectRoot))


############################################################
#Performance stats based on specification dependent labels. Split into two stage, one that computes stats and one that averages stats
############################################################

class PerfEval1_s1(INSTINCT_Rmethod_Task):

    upstream_task1 = luigi.Parameter()
    upstream_task2 = luigi.Parameter()#FG
    upstream_task3 = luigi.Parameter()#AC (Doesn't care what value but used to hash) 
    
    FileGroupID = luigi.Parameter()
    
    PE1process = luigi.Parameter()
    PE1methodID = luigi.Parameter()

    def hashProcess(self):
        hashLength = 6 
        PE1paramsHash = Helper.getParamHash2(self.PE1methodID + ' ' + self.upstream_task1.hashProcess()+ ' ' + self.upstream_task2.hashProcess()+\
                                             ' ' + self.upstream_task3.hashProcess(),hashLength)
        return PE1paramsHash
    def requires(self):
        yield self.upstream_task1
        yield self.upstream_task2
        yield self.upstream_task3
    def output(self):
        return luigi.LocalTarget(self.outpath() + '/Stats.csv.gz')
    def run(self):

        LABpath = self.upstream_task1.outpath()
        FGpath = self.upstream_task2.outpath() + '/'
        INTpath = 'NULL'
            
        resultPath =  self.outpath()
        if not os.path.exists(resultPath):
            os.mkdir(resultPath)

        Paths = [FGpath,LABpath,INTpath,resultPath]
        Args = [self.FileGroupID,"FG"]

        argParse.run(Program='R',cmdType=self.system,ProjectRoot=self.ProjectRoot,ProcessID=self.PE1process,MethodID=self.PE1methodID,Paths=Paths,Args=Args,Params='')

    def invoke(obj,upstream1,upstream2,upstream3,n='default',src=None):
        if src == "GT":
            FileGroupID=obj.FileGroupID
        elif src == "n_":
            FileGroupID=obj.n_FileGroupID
        FileGroupID = Helper.tplExtract(FileGroupID,n=n)
        return(PerfEval1_s1(upstream_task1=upstream1,upstream_task2=upstream2,upstream_task3=upstream3,\
                         FileGroupID=FileGroupID,PE1methodID=obj.PE1methodID,PE1process=obj.PE1process,system=obj.system,ProjectRoot=obj.ProjectRoot))

class PerfEval1_s2(INSTINCT_Rmethod_Task):

    upstream_task1 = luigi.Parameter() #csv of PE1_s1
    
    PE1process = luigi.Parameter()
    PE1methodID = luigi.Parameter()

    def hashProcess(self):
        hashLength = 6 
        PE1paramsHash = Helper.getParamHash2(self.PE1methodID,hashLength)
        return PE1paramsHash
    def output(self):
        return luigi.LocalTarget(self.outpath() + '/Stats.csv.gz')
    def run(self):

        resultPath =  self.outpath()
        if not os.path.exists(resultPath):
            os.mkdir(resultPath)

        FGpath = 'NULL'
        LABpath = 'NULL'
        INTpath =  self.upstream_task1.outpath() + '/Stats.csv.gz'
        resultPath2 =   resultPath + '/Stats.csv.gz'
        FGID = 'NULL'

        Paths = [FGpath,LABpath,INTpath,resultPath2]
        Args = [FGID,'All'] #run second stage of R script 

        argParse.run(Program='R',cmdType=self.system,ProjectRoot=self.ProjectRoot,ProcessID=self.PE1process,MethodID=self.PE1methodID,Paths=Paths,Args=Args,Params='')

    def invoke(obj,upstream1):
        return(PerfEval1_s2(upstream_task1=upstream1,PE1methodID=obj.PE1methodID,PE1process=obj.PE1process,\
                            system=obj.system,ProjectRoot=obj.ProjectRoot))

############################################################
#Performance stats based on specification independent labels
############################################################

class PerfEval2(INSTINCT_Rmethod_Task):

    upstream_task1 = luigi.Parameter() 
    upstream_task2 = luigi.Parameter() 

    #u1:model output (CV)
    #u2:pe1 or edperfeval output

    PE2process = luigi.Parameter()
    PE2methodID = luigi.Parameter()

    PE2datType = luigi.Parameter()
    
    def hashProcess(self):
        hashLength = 6 
        PE2paramsHash = Helper.getParamHash2(self.PE2methodID + ' ' + self.upstream_task1.hashProcess()+ ' ' + self.upstream_task2.hashProcess(),hashLength)
        return PE2paramsHash
    def requires(self):
        yield self.upstream_task1
        yield self.upstream_task2
    def output(self):
        #there are other outputs here, but the output here is the final saved one (so if R script crashes beforehand should prevent this file being saved) 
        return luigi.LocalTarget(self.outpath() + '/PRcurve_auc.txt')
    def run(self):

        StatsPath = self.upstream_task2.outpath()
        resultPath = self.outpath()

        DETpath = self.upstream_task1.outpath()

        if not os.path.exists(resultPath):
            os.mkdir(resultPath)

        Paths = [DETpath,resultPath,StatsPath,self.PE2datType]

        argParse.run(Program='R',cmdType=self.system,ProjectRoot=self.ProjectRoot,ProcessID=self.PE2process,MethodID=self.PE2methodID,Paths=Paths,Args='',Params='')

    def invoke(obj,upstream1,upstream2,PE2datTypeDef=None):
        return(PerfEval2(upstream_task1=upstream1,upstream_task2=upstream2,PE2process=obj.PE2process,PE2methodID=obj.PE2methodID,\
                         PE2datType=PE2datTypeDef,system=obj.system,ProjectRoot=obj.ProjectRoot))

#############################################################
#apply cutoff on DETwProbs object, stage represents if it is in FG paths in cache (2) or in job in cache (1)
#############################################################

class ApplyCutoff(INSTINCT_Task):
    
    upstream_task1 = luigi.Parameter()

    ACcutoffString = luigi.Parameter()

    def hashProcess(self):
        hashLength = 6 
        ACcutoffHash = Helper.getParamHash2(self.ACcutoffString,hashLength)
        return ACcutoffHash
    def output(self):
        return luigi.LocalTarget(self.outpath() + '/DETx.csv.gz')
    def run(self):

        DETwProbs = pd.read_csv(self.upstream_task1.outpath() + '/DETx.csv.gz',compression='gzip')

        if not os.path.exists(self.outpath()):
            os.mkdir(self.outpath())

        DwPcut = DETwProbs[DETwProbs.probs>=float(self.ACcutoffString)]
        DwPcut.to_csv(self.outpath() + '/DETx.csv.gz',index=False,compression='gzip')
    def invoke(obj,upstream1):
        return(ApplyCutoff(upstream_task1=upstream1,ACcutoffString=obj.ACcutoffString,\
                    ProjectRoot=obj.ProjectRoot))

######################################################################
#Apply a model to data with features, generate probability. 
######################################################################

class ApplyModel(INSTINCT_Rmethod_Task):

    upstream_task1 = luigi.Parameter() #DETx (needs features!) 
    upstream_task2 = luigi.Parameter() #Train model
    upstream_task3 = luigi.Parameter() #FG
    
    TMprocess = luigi.Parameter()
    TMmethodID = luigi.Parameter()

    def hashProcess(self):
        TM_hash = self.upstream_task2.hashProcess() #steals previous TM hash 
        return TM_hash
    def requires(self):
        yield self.upstream_task1
        yield self.upstream_task2
        yield self.upstream_task3
    def output(self):
        return luigi.LocalTarget(self.outpath() + '/DETx.csv.gz')
    def run(self):

        DETpath= self.upstream_task1.outpath()+ '/DETx.csv.gz' #R fixes this if incorrect, ok to leave hardcoded like this 
        FGpath = self.upstream_task3.outpath() + '/FileGroupFormat.csv.gz'
        resultPath = self.outpath()
        Mpath= self.upstream_task2.outpath() + '/RFmodel.rds'

        if not os.path.exists(resultPath):
            os.mkdir(resultPath)

        Paths = [DETpath,FGpath,resultPath,Mpath]
        Args = ['apply']

        argParse.run(Program='R',cmdType=self.system,ProjectRoot=self.ProjectRoot,ProcessID=self.TMprocess,MethodID=self.TMmethodID,Paths=Paths,Args=Args,Params='')
        
    def invoke(self,upstream1,upstream2,upstream3):
        return(ApplyModel(upstream_task1=upstream1,upstream_task2=upstream2,upstream_task3=upstream3,TMprocess=self.TMprocess,TMmethodID=self.TMmethodID,\
                          system=self.system,ProjectRoot=self.ProjectRoot))


class ServeModel(INSTINCT_Task):
    #right now very dependent on environment preconfigured for AFSC VM 161.55.120.110 (Windows 10 & Titan RTX GPU)

    upstream_task1 = luigi.Parameter() #FG

    SoundFileRootDir_Host_Dec = luigi.Parameter()
    SMvenv_type = luigi.Parameter() #conda or base
    SMvenv_name = luigi.Parameter() #venv name

    SMprocess = luigi.Parameter()
    SMmethodID = luigi.Parameter()
    SMparamString = luigi.Parameter() #parameters, for the existing tf-hub method, just consists of model location 

    def hashProcess(self):
        hashLength = 6 
        SMparamsHash = Helper.getParamHash2(self.SMparamString + ' ' + self.SMmethodID,hashLength)
        return SMparamsHash
    def requires(self):
        return self.upstream_task1
    def output(self):
        return luigi.LocalTarget(self.outpath() + '/DETx.csv.gz')
    def run(self):

        FGpath = self.upstream_task1.outpath() + '/FileGroupFormat.csv.gz'
        resultPath = self.outpath()

        if not os.path.exists(resultPath):
            os.mkdir(resultPath)

        Paths = [FGpath,resultPath]
        Args = [self.SoundFileRootDir_Host_Dec]
        Params = [self.SMparamString]

        #needs to first activate venv and then run model, then deactivate venv in same command. 
        argParse.run(Program='Python',cmdType=None,ProjectRoot=self.ProjectRoot,ProcessID=self.SMprocess,MethodID=self.SMmethodID,Paths=Paths,Args=Args,Params=Params,venv=self.SMvenv_type,venvName=self.SMvenv_name)
        
    def invoke(self,upstream1):
        return(ServeModel(upstream_task1=upstream1,SMprocess=self.SMprocess,SMmethodID=self.SMmethodID,SMvenv_type=self.SMvenv_type,SMvenv_name=self.SMvenv_name,\
                                  SMparamString=self.SMparamString,SoundFileRootDir_Host_Dec=self.SoundFileRootDir_Host_Dec,ProjectRoot=self.ProjectRoot))

####################################################################################
#Split data with probs and labels back into FG components (used for perf eval 2) 
#####################################################################################

class SplitForPE(INSTINCT_Task):

    upstream_task1 = luigi.Parameter() #FG

    upstream_task2 = luigi.Parameter() #will refer to train model


    FileGroupID = luigi.Parameter() #single value

    def hashProcess(self):
        TM_hash = self.upstream_task2.hashProcess() #steals previous TM hash 
        return TM_hash
    def requires(self):
        return self.upstream_task2 #don't need to requires UP1, because it only continues path and doesn't rely on outputs. 
    def output(self):
        return luigi.LocalTarget(self.outpath() +'/DETx.csv.gz')
    def run(self): 
        #pseudocode:
        #split dataset into num of workers

        DETwProbs = pd.read_csv(self.upstream_task2.outpath() + '/DETx.csv.gz',compression='gzip')

        if not os.path.exists(self.outpath()):
            os.makedirs(self.outpath(),exist_ok=True)
            
        #This will save subset file in original path. 
        DwPsubset = DETwProbs[DETwProbs.FGID == self.FileGroupID]
        DwPsubset.to_csv(self.outpath() + '/DETx.csv.gz',index=False,compression='gzip')
        #this is kind of a weird one. Might not be very modular to other applications as is. 
    def invoke(obj,upstream1,upstream2,n='default'):
        
        FileGroupID = Helper.tplExtract(obj.FileGroupID,n=n)
        return(SplitForPE(upstream_task1=upstream1,upstream_task2=upstream2,FileGroupID=FileGroupID,\
                          ProjectRoot=obj.ProjectRoot))

##################################################################################
#train a model based on detections, extracted features, and GT labels 
##################################################################################

class TrainModel(INSTINCT_Rmethod_Task):

    upstream_task1 = luigi.Parameter() #C4FT

    TMprocess = luigi.Parameter()
    TMmethodID = luigi.Parameter()
    TMparamString = luigi.Parameter()

    TMstage=luigi.Parameter()
    TM_outName=luigi.Parameter()

    TMcpu=luigi.Parameter()

    def hashProcess(self):
        hashLength = 6 
        TM_hash = Helper.getParamHash2(self.TMparamString + ' ' + self.TMmethodID + ' ' + self.upstream_task1.hashProcess(),hashLength)
        return TM_hash
    def output(self):
        #conditional on what this task is doing. 
        return luigi.LocalTarget(self.outpath() + '/' + self.TM_outName)
    def run(self):

        FGpath = self.upstream_task1.outpath() + '/FileGroupFormat.csv.gz'
        TMpath = self.upstream_task1.outpath() + '/DETx.csv.gz'
        Mpath = 'NULL'

        resultPath=self.outpath()

        if not os.path.exists(resultPath):
            os.mkdir(resultPath)

        Paths = [TMpath,FGpath,resultPath,Mpath]
        Args = [self.TMstage,self.TMcpu]

        argParse.run(Program='R',cmdType=self.system,ProjectRoot=self.ProjectRoot,ProcessID=self.TMprocess,MethodID=self.TMmethodID,Paths=Paths,Args=Args,Params=self.TMparamString)
    def invoke(self,upstream1):
        return(TrainModel(upstream_task1=upstream1,TMprocess=self.TMprocess,TMmethodID=self.TMmethodID,TMparamString=self.TMparamString,\
                          TMstage=self.TMstage,TM_outName=self.TM_outName,TMcpu=self.TMcpu,system=self.system,ProjectRoot=self.ProjectRoot))

#####################################################################################
#convert detx type to a format understandable by Raven Pro
#####################################################################################

class RavenViewDETx(INSTINCT_Rmethod_Task):
    #outputs in format RAVENx.txt
    
    upstream_task1 = luigi.Parameter() #DETx
    upstream_task2 = luigi.Parameter() #FG

    SoundFileRootDir_Host_Dec = luigi.Parameter()

    RVmethodID = luigi.Parameter()

    RavenFill = luigi.Parameter()

    def hashProcess(self):
        hashLength = 6
        return Helper.getParamHash2(self.RVmethodID + ' ' + self.RavenFill + ' ' + self.upstream_task1.hashProcess(),hashLength)
    def output(self):
        return luigi.LocalTarget(self.outpath() + '/RAVENx.txt')
    def run(self):
        
        DETpath = self.upstream_task1.outpath() 
        FGpath = self.upstream_task2.outpath() 

        resultPath=self.outpath()

        if not os.path.exists(resultPath):
            os.mkdir(resultPath)

        Paths = [DETpath,FGpath,resultPath]
        Args= [self.SoundFileRootDir_Host_Dec,self.RavenFill]
        
        argParse.run(Program='R',cmdType=self.system,ProjectRoot=self.ProjectRoot,ProcessID="RavenViewDETx",MethodID=self.RVmethodID,Paths=Paths,Args=Args,Params='')
        
    def invoke(self,upstream1,upstream2,RavenFillDef="F"):
        return(RavenViewDETx(upstream_task1=upstream1,upstream_task2=upstream2,RVmethodID=self.RVmethodID,system=self.system,ProjectRoot=self.ProjectRoot,\
                             RavenFill=RavenFillDef,SoundFileRootDir_Host_Dec=self.SoundFileRootDir_Host_Dec))

class RavenToDETx(INSTINCT_Rmethod_Task):
    upstream_task1 = luigi.Parameter() #RAVx
    upstream_task2 = luigi.Parameter() #FG

    fileName = luigi.Parameter()

    RDmethodID = luigi.Parameter()

    def hashProcess(self):
        hashLength = 6
        #hash the previous file to see if it was edited
        fileHash = Helper.hashfile(self.upstream_task1.outpath() + self.fileName,hashLength)

        return Helper.getParamHash2(self.RDmethodID + ' ' + fileHash+ ' ' + self.upstream_task1.hashProcess(),hashLength)
    def output(self):
        #conditional on what this task is doing. 
        return luigi.LocalTarget(self.outpath() + '/DETx.csv.gz')
    def run(self):
        
        RAVpath = self.upstream_task1.outpath() 
        FGpath = self.upstream_task2.outpath() 

        resultPath=self.outpath()

        if not os.path.exists(resultPath):
            os.mkdir(resultPath)

        Paths = [RAVpath,FGpath,resultPath]
        Args= [self.fileName]
        
        argParse.run(Program='R',cmdType=self.system,ProjectRoot=self.ProjectRoot,ProcessID="RavenToDETx",MethodID=self.RDmethodID,Paths=Paths,Args=Args,Params='')
        
    def invoke(self,upstream1,upstream2,fileName='RAVENx.txt'):
        self.fileName=fileName
        return(RavenToDETx(upstream_task1=upstream1,upstream_task2=upstream2,RDmethodID=self.RDmethodID,fileName=self.fileName,\
                           system=self.system,ProjectRoot=self.ProjectRoot))

class QueryData(INSTINCT_Rmethod_Task):

    upstream_task1= luigi.Parameter()
    QDmethodID = luigi.Parameter()
    QDparamString = luigi.Parameter()
    SoundFileRootDir_Host_Raw=luigi.Parameter()
    FileGroupID = luigi.Parameter()
    
    def hashProcess(self):
        hashLength = 6
        return Helper.getParamHash2(self.QDmethodID + ' ' + self.QDparamString,hashLength)
    def outpath(self):
        return self.ProjectRoot + 'Data/FileGroups'
    def output(self):
        return luigi.LocalTarget(self.outpath() + "/" + self.FileGroupID)
    def run(self):
                
        resultPath=self.outpath()

        if not os.path.exists(resultPath):
            os.mkdir(resultPath)

        Paths = [resultPath]
        Args = [self.SoundFileRootDir_Host_Raw,self.FileGroupID]
        Params = self.QDparamString
        
        argParse.run(Program='R',cmdType=self.system,ProjectRoot=self.ProjectRoot,ProcessID="QueryData",MethodID=self.QDmethodID,Paths=Paths,Args=Args,Params=Params)
        
    def invoke(self,upstream1=None):
        if upstream1==None:
            upstream1=Dummy.invoke(self)
        FileGroupID = Helper.tplExtract(self.FileGroupID,n=0)
        return(QueryData(upstream_task1=upstream1,QDmethodID=self.QDmethodID,QDparamString=self.QDparamString,SoundFileRootDir_Host_Raw=self.SoundFileRootDir_Host_Raw,\
                         system=self.system,ProjectRoot=self.ProjectRoot,FileGroupID=FileGroupID))


class ReduceByGT(INSTINCT_Rmethod_Task):

    upstream_task1= luigi.Parameter()
    upstream_task2= luigi.Parameter()
    RGmethodID = luigi.Parameter()
    RGparamString = luigi.Parameter()
    
    def hashProcess(self):
        hashLength = 6
        return Helper.getParamHash2(self.RGmethodID + ' ' + self.RGparamString + ' ' + self.upstream_task2.hashProcess(),hashLength)
    def output(self):
        yield luigi.LocalTarget(self.outpath() + '/FileGroupFormat.csv.gz')
        return luigi.LocalTarget(self.outpath() + '/DETx.csv.gz')
    def run(self):

        DETpath = self.upstream_task1.outpath() 
        FGpath = self.upstream_task2.outpath() 

        resultPath=self.outpath()

        if not os.path.exists(resultPath):
            os.mkdir(resultPath)

        Paths = [DETpath,FGpath,resultPath]
        Params = self.RGparamString
        
        argParse.run(Program='R',cmdType=self.system,ProjectRoot=self.ProjectRoot,ProcessID="ReduceByGT",MethodID=self.RGmethodID,Paths=Paths,Args='',Params=Params)
        
    def invoke(self,upstream1,upstream2):
        return(ReduceByGT(upstream_task1=upstream1,upstream_task2=upstream2,RGmethodID=self.RGmethodID,RGparamString=self.RGparamString,\
                         system=self.system,ProjectRoot=self.ProjectRoot))

