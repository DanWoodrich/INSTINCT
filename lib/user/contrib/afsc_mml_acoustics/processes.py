#can choose to import in global namespace
from classes import INSTINCT_process,Split_process,SplitRun_process,Unify_process,INSTINCT_userprocess
from getglobals import PARAMSET_GLOBALS
from misc import file_peek,get_difftime,get_param_names

import hashlib
import pandas as pd
import os

from pipe_shapes import *

#custom modification of process to hash files (I use this for FormatFG and FormatGT) 
 #############
#this will load in the attributes that are shared by both INSTINCT processes and jobs.
class HashableFile:

    def getfilehash(self):
        
        if self.__class__.__name__=="FormatFG":
            
            path = PARAMSET_GLOBALS['project_root']+ "lib/user/Data/FileGroups/" + self.parameters['file_groupID']
            
        elif self.__class__.__name__=="FormatGT":
            dirpath = PARAMSET_GLOBALS['project_root']+ "lib/user/Data/GroundTruth/"+self.parameters['signal_code']
            
            path = dirpath + "/"+self.parameters['signal_code']+"_" + self.ports[0].parameters['file_groupID']
            
            if not os.path.exists(path): #if GT file doesn't exist, create an empty file
                
                GT = pd.DataFrame(columns = ["StartTime","EndTime","LowFreq","HighFreq","StartFile","EndFile","label","Type","SignalCode"])
                #import code
                #code.interact(local=locals())
                os.makedirs(dirpath,exist_ok=True)
                GT.to_csv(path,index=False)

       

def hashfile(path):
    buff_size = 65536
    sha1 = hashlib.sha1()
    with open(path, 'rb') as f:
        while True:
            data = f.read(buff_size)
            if not data:
                break
            sha1.update(data)
    return sha1.hexdigest()

#####

####custom function to find the right decimation level from format FG. Usually just the port, but if it is looped, it won't be.. 
def find_decimation_level(obj,portnum):

    if obj.ports[portnum].__class__.__name__ == "CombineExtLoop":
        return obj.ports[portnum].ports[0].parameters['target_samp_rate']
    else:
        return obj.ports[portnum].parameters['target_samp_rate']
    
class SplitED(Split_process,INSTINCT_process):
    outfile = 'FileGroupFormat.csv.gz'

    def run(self):
        inFile = self.ports[0].outfilegen() 
        
        FG_dict = file_peek(inFile,fn_type = object,fp_type = object,st_type = object,\
                              dur_type = 'float64',comp_type = 'gzip')
        FG = pd.read_csv(inFile, dtype=FG_dict,compression='gzip')

        if self.splits == 1:
            FG.to_csv(self.outfilegen(),index=False,compression='gzip')
        else:
            row_counts = len(FG['DiffTime'])
            breaks = int(row_counts/self.splits)
            blist = numpy.repeat(range(0,self.splits),breaks)
            bdiff = row_counts - len(blist)
            extra = numpy.repeat(self.splits-1,bdiff)
            flist = list(blist.tolist() + extra.tolist())
            FG.loc[[x==self.split_ID-1 for x in flist]].to_csv(self.outfilegen(),index=False,compression='gzip')

class RunED(Split_process,SplitRun_process,INSTINCT_process):

    #need to define an outpath that is based on splitED... default goes off of last port. 
    outfile = 'DETx.csv.gz'
    SplitInitial=SplitED
    
    def run(self):

        #import code
        #code.interact(local=locals())
        #param_names grandfathered in, should just have R parse dictionaries as a standard
        self.cmd_args=[PARAMSET_GLOBALS['SF_foc'] + "/" + find_decimation_level(self,0),self.outpath(),self.outpath(),\
                        os.path.basename(self.input().path),'1',self.arguments['cpu'],self.arguments['file_chunk_size'],\
                       'method1',self.parameters['methodID'] + '-' + self.parameters['methodvers'],self.param_string,get_param_names(self.parameters)] #params
        
        self.run_cmd()
        #do this manually instead of using run_cmd to be compatible with prvs method
        #rework ED wrapper to work with python dict before reworking run_cmd to work with wrapper
        
class EventDetector(Unify_process,INSTINCT_process):

    outfile = 'DETx.csv.gz'
    SplitRun = RunED

    def run(self):
        EDdict = {'StartTime': 'float64', 'EndTime': 'float64','LowFreq': 'float64', 'HighFreq': 'float64', 'StartFile': 'category','EndFile': 'category','ProcessTag': 'category'}
        
        dataframes = [None] * int(self.arguments['splits'])
        for k in range(int(self.arguments['splits'])):
            dataframes[k] = pd.read_csv(self.outpath() +'/DETx' + str(k+1) + "_" + self.arguments['splits'] + '.csv.gz',dtype=EDdict)
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
        #import code
        #code.interact(local=locals())
        
        if indecesED.empty:
            EDfin = ED[['StartTime','EndTime','LowFreq','HighFreq','StartFile','EndFile']]
            ED.to_csv(self.outfilegen(),index=False,compression='gzip')
        else:
            EDfin = ED.loc[indecesED._values]
            #reduce this to just file names to pass to Energy detector (FG style)

            FG_cols = ['FileName','FullPath','StartTime','Duration','DiffTime','Deployment','SegStart','SegDur']

            FG_dict = {'FileName': 'string','FullPath': 'category', 'StartTime': 'string','Duration': 'int','Deployment':'string','SegStart':'int','SegDur':'int','DiffTime':'int'}
            FG = pd.read_csv(self.ports[0].outpath() +'/FileGroupFormat.csv.gz', dtype=FG_dict, usecols=FG_cols)

            FG = FG[FG.DiffTime.isin(EDfin['ProcessTag2'].astype('int32'))&FG.FileName.isin(EDfin['StartFile'])] #subset based on both of these: if a long difftime, will only
            #take the relevant start files, but will also go shorter than two files in the case of longer segments.
            
            #recalculate difftime based on new files included. <- metacomment: not sure why we need to do this? 
            FG['StartTime'] = pd.to_datetime(FG['StartTime'], format='%Y-%m-%d %H:%M:%S')
            FG = get_difftime(FG)
            
            #save FG
            FG.to_csv(self.outpath() + '/EDoutCorrect.csv.gz',index=False,compression='gzip')

            #run second stage of EventDetector method
            self.cmd_args=[PARAMSET_GLOBALS['SF_foc'] + "/" + find_decimation_level(self,0),self.outpath(),self.outpath(),\
                        'EDoutCorrect.csv.gz','2',self.arguments['cpu'],self.arguments['file_chunk_size'],\
                       'method1',self.parameters['methodID'] + '-' + self.parameters['methodvers'],self.param_string,get_param_names(self.parameters)]

            self.process_ID = self.__class__.__name__ #needs to be specified here since it's a wrapper, otherwise assumed as class name
            
            self.run_cmd()

            ED = ED.drop(columns="ProcessTag")
            ED = ED.drop(columns="ProcessTag2")

            EDdict2 = {'StartTime': 'float64', 'EndTime': 'float64','LowFreq': 'float64', 'HighFreq': 'float64', 'StartFile': 'category','EndFile': 'category','DiffTime': 'int'}
            #now load in result,
            EDpatches = pd.read_csv(self.outpath()+'/DETx_int.csv.gz',dtype=EDdict2)
            PatchList = [None] * len(EDpatches['DiffTime'].unique().tolist())

            for n in range(len(EDpatches['DiffTime'].unique().tolist())):

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

            os.remove(self.outpath() + '/DETx_int.csv.gz')
            os.remove(self.outpath() + '/EDoutCorrect.csv.gz')

            ED.to_csv(self.outfilegen(),index=False,compression='gzip')

class SplitFE(Split_process,INSTINCT_process):
    outfile = 'DETx.csv.gz'

    def run(self):

        inFile = self.ports[0].outfilegen() 

        DETdict = {'StartTime': 'float64', 'EndTime': 'float64','LowFreq': 'float64', 'HighFreq': 'float64', 'StartFile': 'category','EndFile': 'category'}
        DET = pd.read_csv(inFile, dtype=DETdict,compression='gzip')
        
        if self.splits == 1:
            DET.to_csv(self.outfilegen(),index=False,compression='gzip')
        #need to test this section to ensure forking works 
        else:
            row_counts = len(DET['StartTime'])
            breaks = int(row_counts/self.splits)
            blist = numpy.repeat(range(0,self.splits),breaks)
            bdiff = row_counts - len(blist)
            extra = numpy.repeat(self.splits-1,bdiff)
            flist = list(blist.tolist() + extra.tolist())
            DET.loc[[x==self.split_ID-1 for x in flist]].to_csv(self.outfilegen(),index=False,compression='gzip')
        
class RunFE(Split_process,SplitRun_process,INSTINCT_process):

    outfile = 'DETx_int.csv.gz'
    SplitInitial=SplitFE
    
    def run(self):

        self.cmd_args= [self.ports[1].outpath(),os.path.dirname(self.input().path),PARAMSET_GLOBALS['SF_foc'] + "/" + find_decimation_level(self,1),\
                        self.outpath(),str(self.split_ID) + '_' + str(self.splits),str(self.arguments['cpu']),\
                        'method1',self.parameters['methodID'] + '-' + self.parameters['methodvers'],self.param_string,get_param_names(self.parameters)]
        
        self.run_cmd()

class FeatureExtraction(Unify_process,INSTINCT_process):

    pipeshape = TwoUpstream
    upstreamdef = ['GetFG','GetDETx']

    outfile = 'DETx.csv.gz'
    SplitRun = RunFE

    def run(self):
        
        dataframes = [None] * int(self.arguments['splits'])
        for k in range(int(self.arguments['splits'])):
            dataframes[k] = pd.read_csv(self.outpath() + '/DETx_int' + str(k+1) + "_" + self.arguments['splits'] + '.csv.gz')
        FE = pd.concat(dataframes,ignore_index=True)

        
        FE.to_csv(self.outfilegen(),index=False,compression='gzip')

class RavenViewDETx(INSTINCT_process):
    outfile = 'RAVENx.txt'

    def run(self):
        #import code
        #code.interact(local=locals())
        self.cmd_args=[self.ports[0].outpath(),self.ports[1].outpath(),self.outpath(),\
                       PARAMSET_GLOBALS['SF_foc'] + "/" + find_decimation_level(self,1),\
                       " ".join(self.arguments.values())," ".join(self.parameters.values())]
        
        
        self.run_cmd()

class RavenToDETx(INSTINCT_process):
    outfile = 'DETx.csv.gz'

    def run(self):
        #import code
        #code.interact(local=locals())
        self.cmd_args=[self.ports[0].outpath(),self.ports[1].outpath(),self.outpath(),\
                       self.ports[0].outfile]
        
        self.run_cmd()

class ReduceByField(INSTINCT_process):

    outfile = 'DETx.csv.gz'

    def run(self):
        
        self.cmd_args=[self.ports[0].outpath(),self.ports[1].outpath(),self.outpath(),self.param_string]
        
        self.run_cmd()

class ApplyCutoff(INSTINCT_process):

    pipeshape = OneUpstream
    upstreamdef = ["GetDETx"]

    outfile = 'DETx.csv.gz'

    def run(self):

        #import code
        #code.interact(local=locals())
        
        DETwProbs = pd.read_csv(self.ports[0].outpath() + '/DETx.csv.gz',compression='gzip')

        DwPcut = DETwProbs[DETwProbs.probs>=float(self.parameters['cutoff'])]
        DwPcut.to_csv(self.outfilegen(),index=False,compression='gzip')
    
class AssignLabels(INSTINCT_process):

    pipeshape =ThreeUpstream_bothUpTo1
    upstreamdef = ["GetFG","GetDETx","GetGT"]

    outfile = 'DETx.csv.gz'

    def run(self):
    
        #import code
        #code.interact(local=locals())
        
        self.cmd_args=[self.ports[2].outpath(),self.ports[0].outpath(),self.ports[1].outpath(),self.outpath(),self.param_string]
        
        self.run_cmd()

class PerfEval1_s1(INSTINCT_process):

    pipeshape = TwoUpstream
    upstreamdef = ["GetFG","GetAL"]

    outfile = 'Stats.csv.gz'
    #FG,LAB,AC
    def run(self):

        #import code
        #code.interact(local=locals())
        
        self.cmd_args=[self.ports[1].outpath(),self.ports[0].outpath(),'NULL',self.outpath(),self.ports[1].parameters['file_groupID'],"FG"]
        
        self.run_cmd()

class PerfEval1_s2(INSTINCT_process):

    pipeshape = TwoUpstream_noCon
    upstreamdef = ["GetFG","GetPE1_S1"]

    outfile = 'Stats.csv.gz'

    def run(self):

        #import code
        #code.interact(local=locals())
        
        self.cmd_args=['NULL','NULL',self.input()[0].path,self.outfilegen(),'NULL','All']
        
        self.run_cmd()

class PerfEval2(INSTINCT_process):

    pipeshape = TwoUpstream_noCon
    upstreamdef = ["EDperfEval","GetModel_w_probs"]
    
    outfile = 'PRcurve.png'

    def run(self):
        
        self.cmd_args=[self.ports[0].outpath(),self.outpath(),self.ports[1].outpath()]
        
        self.run_cmd()

class TrainModel_RF_CV(INSTINCT_process):

    pipeshape = ThreeUpstream_noCon
    upstreamdef = ["GetFG","GetDETx_w_FE","GetDETx_w_AL"]

    outfile = 'DETx.csv.gz'

    def run(self):
        
        self.cmd_args=[self.input()[0].path,self.input()[1].path,self.input()[2].path,self.outpath(),"NULL","CV",self.arguments['cpu'],self.param_string]
        
        self.run_cmd()

class TrainModel_RF_obj(INSTINCT_process):

    pipeshape = ThreeUpstream_noCon
    upstreamdef = ["GetFG","GetDETx_w_FE","GetDETx_w_AL"]
    
    outfile = 'RFmodel.rds'

    def run(self):

        self.cmd_args=[self.input()[0].path,self.input()[1].path,self.input()[2].path,self.outpath(),"NULL","train",self.arguments['cpu'],self.param_string]
        
        self.run_cmd()

class TrainModel_RF_apply(INSTINCT_process):

    pipeshape = ThreeUpstream
    upstreamdef = ["GetFG","GetDETx_w_FE","GetModel_obj"]

    outfile = 'DETx.csv.gz'

    def run(self):
        #import code
        #code.interact(local=locals())
        self.cmd_args=["NULL",self.input()[1].path,self.input()[2].path,self.outpath(),self.input()[0].path,"apply",self.arguments['cpu'],self.param_string]
        
        self.run_cmd()
        
class SplitForPE(INSTINCT_process):

    pipeshape = TwoUpstream_noCon
    upstreamdef = ["GetFG","GetModel_w_probs"]
    
    outfile = "DETx.csv.gz"
    
    def run(self):
        DETwProbs = pd.read_csv(self.ports[0].outfilegen(),compression='gzip')
        
        #import code
        #code.interact(local=locals())
        
        DwPsubset = DETwProbs[DETwProbs.FGID == self.ports[1].parameters['file_groupID']]
        DwPsubset.to_csv(self.outfilegen(),index=False,compression='gzip')
        
class StatsTableCombine_ED_AM(INSTINCT_process):

    pipeshape = TwoUpstream_noCon
    upstreamdef = ["EDperfEval","AMperfEval"]

    #this one's hardcoded, no elegant way I could find to extract the pipeline history in an elegant/readible way. 
    outfile = "Stats.csv.gz"

    stagename = 'ED/AM'
        
    def run(self):

        #import code
        #code.interact(local=locals())
    
        #import datasets, tag with new ID column, rbind them. 
        Stats1 = pd.read_csv(self.ports[0].outfilegen(),compression='gzip') #AM
        Stats2 = pd.read_csv(self.ports[1].outfilegen(),compression='gzip') #ED

        Stats1[self.stagename] = self.upstreamdef[1]
        Stats2[self.stagename] = self.upstreamdef[0]

        StatsOut = pd.concat([Stats2,Stats1])

        StatsOut.to_csv(self.outfilegen(),index=False,compression='gzip')

class StatsTableCombine_TT(StatsTableCombine_ED_AM):

    upstreamdef = ["MPE_perfEval","TT_perfEval"]

    stagename = 'MPE/TT'
    
class AddFGtoDETx(INSTINCT_process):

    pipeshape = TwoUpstream
    upstreamdef = ['GetFG','GetDETx']

    outfile = "DETx.csv.gz"
    
    def run(self):
        DETx = pd.read_csv(self.ports[0].outfilegen(),compression='gzip')
        DETx['FGID'] =self.ports[1].parameters['file_groupID']
        DETx.to_csv(self.outfilegen(),index=False,compression='gzip')

class QueryData(INSTINCT_process):

    pipeshape = NoUpstream
    upstreamdef = [None]

    outfile = 'table.csv.gz' #could be FG, detx, etc, who knows

    def run(self):
        #import code
        #code.interact(local=locals())
        self.cmd_args=[self.outpath(),PARAMSET_GLOBALS['SF_raw'],self.outfile,self.param_string]
        
        self.run_cmd()


class FormatGT(INSTINCT_process):

    pipeshape = OneUpstream
    upstreamdef = ['GetFG']
    
    outfile = 'DETx.csv.gz'

    def infile(self):
        _dir = PARAMSET_GLOBALS['project_root']+ "lib/user/Data/GroundTruth/"+self.parameters['signal_code']
        path = _dir + "/"+ self.parameters['signal_code']+"_" + self.ports[0].parameters['file_groupID']
        return _dir,path

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        #add infile to hash
        _dir,path = self.infile()

        #import code
        #code.interact(local=locals())
        
        if not os.path.exists(path): #if GT file doesn't exist, create an empty file
            GT = pd.DataFrame(columns = ["StartTime","EndTime","LowFreq","HighFreq","StartFile","EndFile","label","Type","SignalCode"])
            #import code
            #code.interact(local=locals())
            os.makedirs(_dir,exist_ok=True)
            GT.to_csv(path,index=False)

        self._Task__hash = hash(str(self._Task__hash)+ hashfile(path))
        self._Task__hash = int(str(self._Task__hash)[1:(1+self.hash_length)])

    def run(self):
        #import code
        #code.interact(local=locals())
        GT = pd.read_csv(self.infile()[1])

        GT.to_csv(self.outfilegen(),index=False,compression='gzip')


class FormatFG(INSTINCT_process):
    
    pipeshape = OneUpstream
    upstreamdef = ['GetFG']

    outfile = 'FileGroupFormat.csv.gz'

    def infile(self):
        if self.ports[0]!=None:
            file =self.ports[0].outfilegen()
        else:
            file = PARAMSET_GLOBALS['project_root'] + "lib/user/Data/FileGroups/" + self.parameters['file_groupID']
        return file

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        #add infile to hash

        if self.ports[0]!=None:
            self._Task__hash = hash(str(self._Task__hash)+ hashfile(self.infile()))

    def run(self):

        #here, insert a conditional to see if there is an upstream. if so, have the upstream file take precedence over the specified one. And, insert the upstream parameter
        #into the parameters of FormatFG.

        #import code
        #code.interact(local=locals())
        #need to catch if querydata has been run or not. question is how I want it to behave? Default to saved FG, or newly run FG? (newly run -if querying, likely doing it for
        #a reason!)

        #catch by looking for upstream
        file = self.infile()
        #supports additional metadata fields
        FG_dict = file_peek(file,fn_type = object,fp_type = object,st_type = object,dur_type = 'float64')
        FG = pd.read_csv(file, dtype=FG_dict)
        FG['StartTime'] = pd.to_datetime(FG['StartTime'], format='%y%m%d-%H%M%S')
        #import code
        #code.interact(local=locals())
       # FG['FullPath']="/" + self.parameters['target_samp_rate'] +pd.Series(FG["FullPath"], dtype="string")
        #import code
        #code.interact(local=locals())
        FG=get_difftime(FG)

        if self.parameters['decimate_data'] == 'y':
            #if decimating, run decimate. Check will matter in cases where MATLAB supporting library is not installed.
            #note that this can be pretty slow if not on a VM! Might want to figure out another way to perform this
            #to speed up if running on slow latency.

            FullFilePaths = FG['FullPath'].astype('str') + FG['FileName'].astype('str')

            #remove duplicates
            FullFilePaths=pd.Series(FullFilePaths.unique())

            ffpPath=self.outpath() + '/FullFilePaths.csv'

            FullFilePaths.to_csv(ffpPath,index=False,header = None) #don't do gz since don't want to deal with it in MATLAB!

            
            #wrap it into run cmd later.. will need to change it so that matlab recieves args in order of paths, args, parameters 
            command = PARAMSET_GLOBALS['project_root'] + "bin/FormatFG/" + self.parameters['methodID'] + self.parameters['methodvers']+ "/" + self.parameters['methodID']\
                      + self.parameters['methodvers'] + ".exe" + ' ' + PARAMSET_GLOBALS['SF_raw'] + ' ' + ffpPath + ' ' + self.parameters['target_samp_rate']
            print(command)
            
            os.system(command)

            os.remove(ffpPath)

            FG.to_csv(self.outfilegen(),index=False,compression='gzip')
        else:
            #do it this way, so that task will not 'complete' if decimation is on and doesn't work
            FG.to_csv(self.outfilegen(),index=False,compression='gzip')

class EditRAVENx(INSTINCT_userprocess):
    #pipeshape = OneUpstream
    #upstreamdef = ['GetViewFile']
    
    outfile = 'RAVENx.txt'
    userfile_append = "_edit"

    def get_info(self): #this injects some formatFG info into the manifest
        return self.ports[1].parameters['file_groupID']
