#functions related to running jobs 
import time
import luigi

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

