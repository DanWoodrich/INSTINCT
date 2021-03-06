import hashlib
import configparser
from supporting.instinct import Helper

####################
#define general fxns
####################

def readP_Params(ParamsRoot,methodID):
    p_ini = configparser.ConfigParser()
    p_ini.read(ParamsRoot+ methodID+ '.ini')
    return p_ini

def getParamDeets(p_ini,process,index):
    pList = sorted(p_ini.items(process))
    paramList = getParam2(pList,index)
    return paramList

def getParam2(self,index):
    paramList = [None]*len(self) 
    for p in range(len(self)):
        paramList[p] = self[p][index]
    return paramList

def getParamString(paramList,methodID,otherInput=''):
    string_out = str(' '.join(paramList) + ' ' + methodID + otherInput).lstrip(' ')
    return string_out 

def AC(self,ID):
    self.ACprocess = 'ApplyCutoff'
    self.ACcutoffString = str(self.MasterINI[ID]['cutoff'])
    return self

def AL(self,ID):
    self.ALprocess = 'AssignLabels'
    self.ALmethodID = self.MasterINI[ID]['MethodID']

    p_ini = readP_Params(self.ParamsRoot,self.ALmethodID)
    paramList = getParamDeets(p_ini,self.ALprocess,1)
    self.ALparamString = getParamString(paramList,self.ALmethodID)
    return self


def ED(self,ID):
    self.EDprocess = 'EventDetector'
    self.EDmethodID = self.MasterINI[ID]['MethodID']
    self.EDsplits = int(self.MasterINI[ID]['Splits'])
    self.EDcpu = self.MasterINI[ID]['CPUneed']
    self.EDchunk = self.MasterINI[ID]['sf_chunk_size']

    p_ini = readP_Params(self.ParamsRoot,self.EDmethodID)
    paramList = getParamDeets(p_ini,self.EDprocess,1)
    self.EDparamString = getParamString(paramList,self.EDmethodID)

    paramNames = getParamDeets(p_ini,self.EDprocess,0)
    self.EDparamNames=' '.join(paramNames)#pass the dict IDs, useful for methods wrappers when need to
    #pull out certain params at different stages
    return self

def FE(self,ID):
    self.FEprocess = 'FeatureExtraction'
    self.FEmethodID = self.MasterINI[ID]['MethodID']
    self.FEsplits = int(self.MasterINI[ID]['Splits'])
    self.FEcpu = self.MasterINI[ID]['CPUneed']
    p_ini = readP_Params(self.ParamsRoot,self.FEmethodID)
    paramList = getParamDeets(p_ini,self.FEprocess,1)
    self.FEparamString = getParamString(paramList,self.FEmethodID)
    paramNames = getParamDeets(p_ini,self.FEprocess,0)
    self.FEparamNames=' '.join(paramNames)
    return self

def FG(self,ID,FGovr=None):
    self.FGprocess = 'FormatFG'
    FileGroupID = self.MasterINI[ID]['FileGroupID']
    self.SoundFileRootDir_Host_Raw = self.MasterINI[ID]['SoundFileRootDir_Host']  #this is a little ugly, but make it convention for now
    self.FileGroupID = sorted(FileGroupID.split(','))
    self.IDlength = len(self.FileGroupID)
    self.FGmethodID = self.MasterINI[ID]['MethodID']
    self.decimatedata = self.MasterINI[ID]['DecimateData'] 
    
    self.FGfile = [None] * self.IDlength
    for l in range(self.IDlength):
        self.FGfile[l] = self.ProjectRoot +'Data/' + 'FileGroups/' + self.FileGroupID[l]

    if FGovr!=None:
        self.FileGroupID=[FGovr]
        self.FGfile=[self.ProjectRoot +'Data/' + 'FileGroups/' + FGovr]
        #note that overriding IDlength can cause other effects in proccess that are using this var for training: recommend using params.topLoop instead
        #for more complex jobs (see ViewFGfromCV Job) 
        self.IDlength = len([FGovr])
        
    p_ini = readP_Params(self.ParamsRoot,self.FGmethodID)

    paramList = getParamDeets(p_ini,self.FGprocess,1)
    self.FGparamString = getParamString(paramList,self.FGmethodID)

    p_ini_dct = dict(p_ini.items(self.FGprocess))

    decVal = p_ini_dct.get('targetsamprate')
    
    #calculate Sf _Dec path here, based on if decimateData is y (if not, set to _Raw value), and if so, the sample rate. 
    if self.decimatedata == 'y':
        self.SoundFileRootDir_Host_Dec = self.SoundFileRootDir_Host_Raw + "/DecimatedWaves/" + decVal
    elif self.decimatedata == 'n':
        self.SoundFileRootDir_Host_Dec = self.SoundFileRootDir_Host_Raw + "Waves"

    
    return self

def GT(self,ID):

    self.GTprocess = 'FormatGT'
    self.GT_signal_code = self.MasterINI[ID]['GT_signal_code']
    self.GTfile = [None] * self.IDlength

    for l in range(self.IDlength):
        self.GTfile[l] = self.ProjectRoot +'Data/' + 'GroundTruth/' + self.GT_signal_code + '/' + self.GT_signal_code + '_' +self.FileGroupID[l]
    return self


def MFA(self,ID):
    self.MFAprocess = 'MergeFE_AL'
    self.MFAmethodID = self.MasterINI[ID]['MethodID']
    paramList= ''
    self.MFAparamString = getParamString(paramList,self.MFAmethodID)
    return self

def TM(self,ID,stage):
    self.TMprocess = 'TrainModel'
    self.TMstage = stage
    if self.TMstage == 'CV':
        self.TM_outName = 'DETx.csv.gz'
    elif self.TMstage == 'train':
        self.TM_outName = 'RFmodel.rds'

    self.TMmethodID = self.MasterINI[ID]['MethodID']
    self.TMcpu = self.MasterINI[ID]['CPUneed']

    p_ini = readP_Params(self.ParamsRoot,self.TMmethodID)
    if(self.TMstage=='CV'):
        paramList = sorted(p_ini.items(self.TMprocess)+ p_ini.items(self.TMstage))
    else:
        paramList = sorted(p_ini.items(self.TMprocess)+ [('cv_it', '1'), ('cv_split', '1')])
    paramList = getParam2(paramList,1) #reformat as usual
    self.TMparamString = getParamString(paramList,self.TMmethodID)
    return self

def PE1(self,ID):
    self.PE1process = 'PerfEval1'
    self.PE1methodID = self.MasterINI[ID]['MethodID']
    paramList = '' #no params
    self.paramString = getParamString(paramList,self.PE1methodID) #just hashes methodID
    return self

def PE2(self,ID):
    self.PE2process = 'PerfEval2'
    self.PE2methodID = self.MasterINI[ID]['MethodID']
    paramList = ''  # no params
    self.paramString = getParamString(paramList, self.PE2methodID)
    return self

def PR(self,ID):
    self.PRprocess = 'PerformanceReport'
    self.PRmethodID = self.MasterINI[ID]['MethodID']
    paramList = ''  # no params
    paramString = getParamString(paramList, self.PRmethodID)  # just hashes methodID
    self.PRparamString = ''
    return self

def RV(self,ID):
    self.RVmethodID = self.MasterINI[ID]['MethodID']
    return self

def RD(self,ID):
    self.RDmethodID = self.MasterINI[ID]['MethodID']
    return self

def QD(self,ID):
    self.QDprocess = 'QueryData'
    self.QDmethodID = self.MasterINI[ID]['MethodID']

    p_ini = readP_Params(self.ParamsRoot,self.QDmethodID)
    paramList = getParamDeets(p_ini,self.QDprocess,1)
    self.QDparamString = getParamString(paramList,self.QDmethodID)

    return self

def RG(self,ID):
    self.RGprocess = 'ReduceByGT'
    self.RGmethodID = self.MasterINI[ID]['MethodID']

    p_ini = readP_Params(self.ParamsRoot,self.RGmethodID)
    paramList = getParamDeets(p_ini,self.RGprocess,1)
    self.RGparamString = getParamString(paramList,self.RGmethodID)

    return self

def SM(self,ID):
    self.SMprocess = 'ServeModel'
    self.SMmethodID = self.MasterINI[ID]['MethodID']
    self.SMvenv_type = self.MasterINI[ID]['venv_type']
    self.SMvenv_name = self.MasterINI[ID]['venv_name']

    p_ini = readP_Params(self.ParamsRoot,self.SMmethodID)
    paramList = getParamDeets(p_ini,self.SMprocess,1)
    self.SMparamString = getParamString(paramList,self.SMmethodID)
    
    return self




    



        
