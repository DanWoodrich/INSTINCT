#functions related to running jobs 
import time
import luigi
import configparser
import random
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

def deployJob(self,args,dType="Job"):

    #I should have this also output params to outpath, to refer to later! 
    
    start = time.time()
    Params =self.getParams(args)
    inv = self.invoke(Params)
    result=luigi.build([inv], local_scheduler=True)
    randNum=random.randint(1,10)
    end = time.time()

    if result:
        if(dType=="Job"):
            print("                          Output file location path:\n" + "                   " +inv.outpath())
            
        elif(dType=="Wrapper"):
            print("                             Wrapper Job Successful!                   ")
        print("                        elapsed time (d:h:m:s): " + str(secToDHMS(round(end-start,0))))
    else:
        print("                               === Job failed ===")
    print(getArt(str(Params.GT_signal_code),result,num=randNum))

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
        self.CacheRoot=self.MasterINI['Global']['CacheRoot']

def getArt(signal,result,num=1):
    if signal in ["GS","RW","LM"]:
        #template for art by artist 'snd' https://ascii.co.uk/art/whale
        #modified by dfw!
        if result==True and num < 10: 
            art =r"""                                ','. '. ; : ,','
                                   '..'.,',..'
                                    ';.'  ,'
                                       ;;
                                       ;'
                      :._   _.------------.__
              __      |  :-'              ## '\
       __   ,' .'    .'             /        ##\ 
     /__ '.-   \___.'                ^  .--.  # |
       '._                  ~~      \__/    \__/
         '----'.____           \      ##     .'
                    '------.    \._____.----' 
             INSTINCT       \.__/  
            """
        elif result==True and num==10:
            art =r"""                                ','. '. ; : ,','
                                   '..'.,',..'
                                    ';.'  ,'
                                        ;;
                                       _____
                                      /     \___
                      :._   _.-------+=======+--`
              __      |  :-'              ## '\       )
       __   ,' .'    .'            __~       ##\     (
     /__ '.-   \___.'                D  .--.  # |     )
       '._                  ~~      \__/    \__[======#
         '----'.____           \      ##     .'
                    '------.    \._____.----' 
             INSTINCT       \.__/  
            """
        else:    
            art =r"""                                ','. '. ; : ,','
                                   '..'.,',..'
                                    ';.'  ,'
                                       ;;
                                       ;'
                      :._   _.------------.__
              __      |  :-'              ## '\
       __   ,' .'    .'            /         ##\ 
     /__ '.-   \___.'              o  .----.  # |
       '._                  ~~       /   ## \__/
         '----'.____           \    / ##     .'
                    '------.    \._____.----' 
             INSTINCT       \.__/  
            """
        return art
    elif signal=="HB":
        if result==True and num < 10:
            art =r"""                                 ','. '. ; : ,','
                              '..'.,',..'
                                   ';.'  ,'
                                    ;;
                                    ;'
                     :._   _.------------.___
             __      :  :-'                  '-._
      __   ,' .'    .'       /      _____________'.
     /__ '.-   \___.'          ^  .' .'  .'  _.-_.'
        '._              ~      .-': .' _.' _.'_.'
            '----'._____.|   /._.'_'._:_:_.-'--'
                        /   /  \  \ 
        INSTINCT       / ./'    '\ \
                      /./         \'
                      '
            """
        elif result==True and num==10:
            art =r"""                                 ','. '. ; : ,','
                              '..'.,',..'
                                   ';.'  ,'
                                    ;;
                                    _____
                                   /     \___           )
                      :._   _.----+=======+--<         (
             __      :  :-'                   '-.       )
      __   ,' .'    .'       __~   ______________[======#
     /__ '.-   \___.'          D .' .'  .'  _.-_.'
        '._              ~      .-': .' _.' _.'_.'
            '----'._____.|   /._.'_'._:_:_.-'--'
                        /   /  \  \ 
        INSTINCT       / ./'    '\ \
                      /./         \'
                      '
             """
        else:
            art =r"""                                 ','. '. ; : ,','
                              '..'.,',..'
                                   ';.'  ,'
                                    ;;
                                          ;'
                     :._   _.------------.___
             __      :  :-'                  '-.
      __   ,' .'    .'          /    ___________'.
     /__ '.-   \___.'           o   /.'  .'  _.-_.'
        '._              ~         / .' _.' _.'_.'
            '----'._____.|   /______':_:_.-'--'
                        /   /  \  \ 
        INSTINCT       / ./'    '\ \
                      /./         \'
                      '
            """
        return art
    else:
        print("Signal art not yet defined")
