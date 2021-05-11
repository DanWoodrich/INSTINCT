#functions related to running jobs 
import time
import luigi
import configparser
from supporting.instinct import Helper

def secToDHMS(time):
    day = time // (24 * 3600)
    time = time % (24 * 3600)
    hour = time // 3600
    time %= 3600
    minutes = time // 60
    time %= 60
    seconds = time
    return("%d:%d:%d:%d" % (day, hour, minutes, seconds))

def deployJob(self,args):
    
    start = time.time()
    Params =self.getParams(args)
    inv = self.invoke(Params)
    luigi.build([inv], local_scheduler=True)
    end = time.time()
    

    
    print("                          Output file location path:\n" + "                   " +inv.outpath())
    print("                     elapsed time (d:h:m:s): " + str(secToDHMS(round(end-start,0))))
    print(r"""
                                 ','. '. ; : ,','
                                   '..'.,',..'
                                    ';.'  ,'
                                       ;;
                                       ;'
                      :._   _.------------.__
              __      |  :-'              ## '\
       __   ,' .'    .'                      ##\ 
     /__ '.-   \___.'              o  .----.  # |
       '._                  ~~     ._/   ## \__/
         '----'.____           \      ##     .'
                    '------.    \._____.----' 
             INSTINCT       \.__/  
    """)

class Load_Job:
    def __init__(self,Name,args):

        self.ProjectRoot=Helper.getProjRoot()
        ##if there are 3 args: if one is a ., it is considered the default
        ##1st indicates param path, 2nd indicates job name
        self.JobName = Name
        
        if len(args)>=3:
            ParamPath = args[2]
            if args[2] == ".":
                ParamPath = Name
            else:
                ParamPath = args[2]
            if args[1] == ".":
                self.ParamsRoot=self.ProjectRoot + 'etc/' + ParamPath + '/'
            else:
                self.ParamsRoot=self.ProjectRoot + 'etc/Projects/' + args[1]+ '/' + ParamPath + '/' 
        else:
            self.ParamsRoot=self.ProjectRoot + 'etc/' + self.JobName + '/'
        
        MasterINI = configparser.ConfigParser()
        MasterINI.read(self.ParamsRoot + 'Master.ini')
        self.MasterINI = MasterINI
        self.system=self.MasterINI['Global']['system']
        self.r_version=self.MasterINI['Global']['r_version']
