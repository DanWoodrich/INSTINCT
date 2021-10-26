MethodID="serve-tensorhub-v1-0"

import sys
import subprocess
import pandas as pd 
import pickle

#######import args###########


args="serve-tensorhub-v1-0.py //akc0ss-n086/NMML_CAEP_Acoustics/Detector/google_HB/FileGroupFormat.csv.gz //161.55.120.117/NMML_AcousticsData/Audio_Data/DecimatedWaves/10000 //akc0ss-n086/NMML_CAEP_Acoustics/Detector/google_HB/ https://tfhub.dev/google/humpback_whale/1"
argss=args.split()
resultpath=argss[3]

#args=sys.argv

#make subprocess that executes method, loads in the data, repeat.
#start building list 


start_val=0
IS_DONE=False

import code
code.interact(local=locals())


while IS_DONE == False:
    print(start_val)
    args1 = args + " " + str(start_val)
    subprocess.run([sys.executable, "serve-tensorhub-v1-0-execute.py",args1])

    #import code
    #code.interact(local=locals())
    #load in output
    with open(resultpath+'outfile', 'rb') as fp:
        outlist = pickle.load(fp)

    if start_val == 0:
        df = [outlist[2]]
    else:
        df.append(outlist[2])

    start_val=outlist[0]+1

    if outlist[0] == outlist[1]: #something bugged about this. Doesn't matter, since this is geting depreciated
        IS_DONE=True
    
import code
code.interact(local=locals())

df=pd.concat(df,axis=0)

#save output 
