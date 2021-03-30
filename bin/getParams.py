import hashlib
import configparser
from instinct import Helper

####################
#define general fxns
####################

def getM_Param(self,param):
    return self.Master[self.ID][param]

def readP_Params(self):
    p_ini = configparser.ConfigParser()
    p_ini.read(self.ParamsRoot + self.methodID + '.ini')
    return p_ini

def getParamDeets(self,index):
    pList = sorted(self.p_ini.items(self.process))
    paramList = getParam2(pList,index)
    return paramList

def getParam2(self,index):
    paramList = [None]*len(self) 
    for p in range(len(self)):
        paramList[p] = self[p][index]
    return paramList

def getParamString(self,otherInput=''):
    string_out = str(' '.join(self.paramList) + ' ' + self.methodID + otherInput).lstrip(' ')
    return string_out 

def getParamHash(self):
    hashval = str(hashlib.sha1(self.paramString.encode('utf8')).hexdigest())
    return hashval

def hashJob(FileGroupHashes,GTHashes,otherHashes):
    IDlength = len(FileGroupHashes)
    strings = [None] * IDlength
    for l in range(IDlength):
        strings[l] = ''.join(otherHashes) + FileGroupHashes[l] + GTHashes[l]
    strings = sorted(strings).__str__()
    jobHash = strings.encode('utf8')
    jobHash = str(hashlib.sha1(jobHash).hexdigest())
    return jobHash

###########################
#define process classes
###########################


###
#generic class
###

class gpClass:
    def __init__(self,Master,ID):
        self.Master = Master
        self.ID = ID

    def getParams(self):
        return(self)

#want to abstract some of this into generic classes
#ID and process need to stay seperate, due to how params are being read (ID signals param section in ini, process signals which routine to 

####
#slightly less generic classes
####

    #class gpC_default(gpClass):
#    def __init__(self,Master,ID,ParamsRoot):
#        super().__init__(Master,ID)
#        self.methodID = getM_Param(self,'MethodID')
#        self.ParamsRoot = ParamsRoot
#
#        self.p_ini = readP_Params(self)
#        self.paramList = getParamList(self)
#        self.paramString = getParamString(self)
#        self.paramHash = getParamHash(self)

#class FE(gpC_default):
#    def __init__(self,Master,ID,ParamsRoot,uTaskpath):
#        super().__init__(Master,ID,ParamsRoot)
#        self.uTaskpath = uTaskpath
#        self.process = 'FeatureExtraction'
#        self.Splits = int(getM_Param(self,'Splits'))
#        self.CPUNeed = getM_Param(self,'CPUNeed')

#

#class gpC_twoUpstream:
#class gpC_noParams:

    
class AC(gpClass):
    def __init__(self,Master,ID,uTaskpath):
        super().__init__(Master,ID)
        self.process = 'ApplyCutoff'
        self.uTaskpath = uTaskpath

        self.methodID = ''#placeholder to make it work 
        self.cutoff = str(getM_Param(self,'cutoff'))
        
        self.paramList = str(self.cutoff) #only 1 param
        self.paramString = getParamString(self)
        self.paramHash = getParamHash(self)
        self.paramString = ''

class AL(gpClass):
    def __init__(self,Master,ID,ParamsRoot,uTask1path,uTask2path,hashinc):
        super().__init__(Master,ID)
        self.process = 'AssignLabels'
        self.ParamsRoot = ParamsRoot
        self.uTask1path = uTask1path 
        self.uTask2path = uTask2path
        self.hashinc = hashinc

        self.methodID = getM_Param(self,'MethodID')

        self.p_ini = readP_Params(self)
        self.paramList = getParamDeets(self,1)
        self.paramString = getParamString(self,self.hashinc)
        self.paramHash = getParamHash(self) #hashes the modified string
        self.paramString = getParamString(self) #rewrites string to correct form
        
class ED(gpClass):
    def __init__(self,Master,ID,ParamsRoot):
        super().__init__(Master,ID)
        self.process = 'EventDetector'
        self.ParamsRoot = ParamsRoot

        self.methodID = getM_Param(self,'MethodID')
        self.Splits = int(getM_Param(self,'Splits'))
        self.CPUNeed = getM_Param(self,'CPUNeed')
        self.sf_chunk_size = getM_Param(self,'sf_chunk_size')

        self.p_ini = readP_Params(self)
        self.paramList = getParamDeets(self,1)
        self.paramString = getParamString(self)
        self.paramHash = getParamHash(self)
        
        self.paramNames = getParamDeets(self,0)
        self.paramNames=' '.join(self.paramNames)#pass the dict IDs, useful for methods wrappers when need to
        #pull out certain params at different stages

class FE(gpClass):
    def __init__(self,Master,ID,ParamsRoot,uTaskpath):
        super().__init__(Master,ID)
        self.process = 'FeatureExtraction'
        self.ParamsRoot = ParamsRoot
        self.uTaskpath = uTaskpath

        self.methodID = getM_Param(self,'MethodID')
        self.Splits = int(getM_Param(self,'Splits'))
        self.CPUNeed = getM_Param(self,'CPUNeed')

        self.p_ini = readP_Params(self)
        self.paramList = getParamDeets(self,1)
        self.paramString = getParamString(self)
        self.paramHash = getParamHash(self)

        self.paramNames = getParamDeets(self,0)
        self.paramNames=' '.join(self.paramNames)

class FG(gpClass):
    def __init__(self,Master,ID,ProjectRoot):
        super().__init__(Master,ID)
        self.process = 'FormatFG'
        self.ProjectRoot = ProjectRoot 

        self.FileGroupID = getM_Param(self,'FileGroupID')
        self.SoundFileRootDir_Host = getM_Param(self,'SoundFileRootDir_Host')
        self.FileGroupID = sorted(self.FileGroupID.split(','))
        self.IDlength = len(self.FileGroupID)
        self.FGfile = [None] * self.IDlength
        self.FileGroupHashes = [None] * self.IDlength

        for l in range(self.IDlength):
            self.FGfile[l] = self.ProjectRoot +'Data/' + 'FileGroups/' + self.FileGroupID[l]
            self.FileGroupHashes[l] = Helper.hashfile(self.FGfile[l])
    
class GT(gpClass):
    def __init__(self,Master,ID,ProjectRoot,FileGroupID):
        super().__init__(Master,ID)
        self.process = 'FormatGT'
        self.ProjectRoot = ProjectRoot
        self.FileGroupID = FileGroupID

        self.GT_signal_code = getM_Param(self,'GT_signal_code')
        self.methodID ='' #placeholder to make it work 
        self.p_ini = self.Master
        self.paramList = getParamDeets(self,1)
        self.paramString = getParamString(self)
        self.paramHash = getParamHash(self)
        IDlength = len(self.FileGroupID)
        
        self.GTfile = [None] * IDlength
        self.GTHashes = [None] * IDlength

        for l in range(IDlength):
            self.GTfile[l] = self.ProjectRoot +'Data/' + 'GroundTruth/' + self.GT_signal_code + '_' +self.FileGroupID[l]
            self.GTHashes[l] = Helper.hashfile(self.GTfile[l])

class MFA(gpClass):
    def __init__(self,Master,ID,uTask1path,uTask2path,hashinc):
        super().__init__(Master,ID)
        self.process = 'MergeFE_AL'
        self.uTask1path = uTask1path 
        self.uTask2path = uTask2path 
        self.hashinc = hashinc

        self.methodID = getM_Param(self,'MethodID')

        self.paramList= '' 
        self.paramString = getParamString(self,self.hashinc)
        self.paramHash = getParamHash(self)
        self.paramString = ''

class TM(gpClass):
    def __init__(self,Master,ID,ParamsRoot,stage):
        super().__init__(Master,ID)
        self.process = 'TrainModel'
        self.ParamsRoot = ParamsRoot
        self.stage = stage
        if self.stage == 'CV':
            self.outName = 'DETwProbs.csv.gz'
        elif self.stage == 'train':
            self.outName = 'RFmodel.rds'

        self.methodID = getM_Param(self,'MethodID')
        self.CPUNeed = getM_Param(self,'CPUNeed')
        
        self.p_ini = readP_Params(self)
        if(self.stage=='CV'):
            self.paramList = sorted(self.p_ini.items(self.process)+ self.p_ini.items(self.stage))
        else:
            self.paramList = sorted(self.p_ini.items(self.process)+ [('cv_it', '1'), ('cv_split', '1')])
        self.paramList = getParam2(self.paramList,1) #reformat as usual
        self.paramString = getParamString(self)
        self.paramHash = getParamHash(self)

class PE1(gpClass):
    def __init__(self,Master,ID,uTaskpath):
        super().__init__(Master,ID)
        self.process = 'PerfEval1'
        self.uTaskpath = uTaskpath

        self.methodID = getM_Param(self,'MethodID')
        self.paramList = '' #no params
        self.paramString = getParamString(self) #just hashes methodID
        self.paramHash = getParamHash(self)
        self.paramString = ''



class PE2(gpClass):
    def __init__(self,Master,ID,rootpath,uTask1path,uTask2path,hashinc):
        super().__init__(Master,ID)
        self.process = 'PerfEval2'
        self.rp = rootpath
        self.uTask1path = uTask1path 
        self.uTask2path = uTask2path 
        self.hashinc = hashinc
        
        self.methodID = getM_Param(self,'MethodID')

        self.paramList = '' #no params
        self.paramString = getParamString(self,self.hashinc)
        self.paramHash = getParamHash(self) 
        self.paramString = ''


class PR(gpClass):
    def __init__(self,Master,ID):
        super().__init__(Master,ID)
        self.process = 'PerformanceReport'

        self.methodID = getM_Param(self,'MethodID')
        self.paramList = '' #no params
        self.paramString = getParamString(self) #just hashes methodID
        self.paramHash = getParamHash(self)
        self.paramString = ''


######################
#define job classes
######################

#do EDpe1 vars

#TM params















    



        
