MethodID="serve-tensorhub-v1-0"

#Works on 161.55.120.110 with the conda venv tf-gpu activated (7/6/20)

#Designed for model 'https://tfhub.dev/google/humpback_whale/1'. Unknown if this method is general to other tensorhub models as written

import sys
import pandas as pd 
import tensorflow.compat.v1 as tf
import tensorflow_hub as tfhub
import time
import math
import numpy
#import pickle

from numba import cuda

from matplotlib import pyplot as plt

#######define functions########


#######import params###########


#args="C:/Apps/INSTINCT/Cache/441a6e733244/FileGroupFormat.csv.gz C:/Apps/INSTINCT/Cache/441a6e733244/8a148e //161.55.120.117/NMML_AcousticsData/Audio_Data/DecimatedWaves/10000 https://tfhub.dev/google/humpback_whale/1 serve-tensorhub-v1-0"
#args=args.split()

args=sys.argv
#args=args[1]
#args=args.split()

FGpath=args[1]
resultpath=args[2]
datapath=args[3]
model_name=args[4]

#start_ind = args[5]

###############################

#print(tf.config.list_physical_devices('GPU'))

model = tfhub.load(model_name)

FG = pd.read_csv(FGpath)

#Loop through difftime. After getting through difftime, throw out dets prior to the start of
#window, and after the end of window. Will result in occasional double reads of files, but not
#that big of a deal.

df_list = [None]*len(FG.DiffTime.unique())
#for i in [45,46]:

samples_total = 0

for i in range(len(FG.DiffTime.unique())-1):
    #untab
    print(i)
    FGin = FG[FG.DiffTime==FG.DiffTime.unique()[i]]
    FGin = FGin.reset_index(drop=True)
    tensors = [None] * len(FGin)

    print('load files')

    for p in range(len(FGin)):
        FILENAME= datapath + FGin.FullPath[p] +FGin.FileName[p]
        #for simplicities sake, and to retain same processing, not going to try to load in segments, just
        #load in full wav file and throw out unneeded values later. 
        tensors[p], sample_rate = tf.audio.decode_wav(tf.io.read_file(FILENAME))
        #test that tensors[p] is a clean integer of sample rate. If not, 0 pad it
        samples = tensors[p].get_shape().as_list()[0]

        if samples % sample_rate.numpy() != 0:
            rem = sample_rate.numpy() - (samples % sample_rate.numpy())
            tensors[p] = tf.concat([tensors[p], tf.zeros([rem,1])],0)        

    #looks like my GPU can only handle tensors of under 42000000 samples. So need to chunk.
    #pretty unfortunate that I have go through a big repeat of ED function here.
    #the pseudo code is to chunk the tensor into smaller tensors, process each, process the 'breaks',
    #and splice back in the breaks. Should be easy to predict: just remove detectios which are <3.92
    #secs away from break end, and only process those exact sections.

    #steps to test: figure out how to splice tensor
    #               make new list object of tensors (pattern big, small, big small big) remove ends <3.92 seconds for big tensors
    #               make corresponding list of results
    #               apply model and store results 
    #               concatenate results in order
    #               convert and store as DETx

    #define model parameters
    context_step_samples = tf.cast(sample_rate, tf.int64)
    score_fn = model.signatures['score']

    waveform=tf.concat(tensors, 0)

    samples_all = waveform.get_shape().as_list()[0]

    samples_total = samples_total+samples_all

    #if this exceeds maximum, break the script.
    #if samples_total>9905000000: #1105000000
    #    if i==int(start_ind):
    #        print("segment unrunnable with current configuration.")
     #   break

    print('samples to process;' + str(samples_total))

    max_samples = 24000000
    abbreviate_big_chunks = 30000
    #score_abbreviate = int(abbreviate_big_chunks/sample_rate.numpy())
    big_chunks = math.ceil(samples_all / max_samples)

    score_list = [None] * (big_chunks + big_chunks-1)

    #so it looks like the model behavior is to not run windows which are < context window size. So don't have to abbreviate scores
    #but wait: too many scores for the larger chunks. Where are these scores coming from? 

    print('chunk data and apply model')

    #if i ==1:
    #    import code
    #   code.interact(local=locals())

    for f in range(len(score_list)):
        stage = math.floor(f / 2)
        IS_EVEN = (f % 2) == 0 #even are big chunks, odd small chunks
        IS_LAST = f==len(score_list)-1  #last is remainder and not abbreviated

        if IS_EVEN & (IS_LAST==False):
            chunk= tf.expand_dims(tf.slice(waveform, [stage*max_samples, 0], [max_samples, 1]),0) #eliminate last dets later
            scores = score_fn(waveform=chunk, context_step_samples=context_step_samples)['scores'] #["scores"].numpy().ravel()
            
        elif IS_EVEN & IS_LAST:
            pos = stage*max_samples
            if pos !=0:
                rem = samples_all % pos
            else: #only should occur in single file case 
                rem = samples_all
            chunk= tf.expand_dims(tf.slice(waveform, [pos, 0], [rem, 1]),0)
            
            scores = score_fn(waveform=chunk, context_step_samples=context_step_samples)['scores']

        else:
            chunk = tf.expand_dims(tf.slice(waveform, [((stage+1)*max_samples)-abbreviate_big_chunks, 0], [abbreviate_big_chunks*2, 1]),0) #eliminate last dets later
            scores = score_fn(waveform=chunk, context_step_samples=context_step_samples)['scores']

        score_list[f]=scores

    #cuda.select_device(0)
    #cuda.close()

    print('assemble df')

    scores = tf.concat(score_list,1)

    score_len = scores.get_shape()[1]



    StartTime = pd.Series(range(score_len))*0.99 #Matt's fix is to multiply StartTime by 0.99. 
    EndTime = StartTime+ 3.9124 #window length is exactly 3.9124 secs




    LowFreq = pd.Series([0]*score_len)
    HighFreq = pd.Series([5000]*score_len)

    probs=pd.Series(sum(scores[0].numpy().tolist(),[]))

    cums = pd.Series([0])
    ser = pd.Series(numpy.cumsum(FGin.Duration))

    cums=cums.append(ser,ignore_index=True)

    #if(i ==13):
    #    import code
    #    code.interact(local=locals())

        #before calculating other stuff, use these cumulative times to exclude times not in the difftime:
    compStart = StartTime<FGin.SegStart[0]
    compEnd = StartTime>(max(cums)-FGin.Duration[(len(FGin)-1)]+FGin.SegStart[(len(FGin)-1)]+FGin.SegDur[(len(FGin)-1)])
    ExcludeStart = numpy.where(compStart==True)[0]#indeces to exclude from the final table
    ExcludeEnd = numpy.where(compEnd==True)[0]#indeces to exclude from the final table

    StartFile=pd.cut(StartTime, cums, labels=FGin.FileName,right=False) #this breaks with labels smaller than wav files (pngs). 
    EndFile=pd.cut(EndTime, cums, labels=FGin.FileName,right=False)

    DeploymentID = pd.cut(StartTime, cums, labels=FGin.Deployment,right=False) #since this processes difftimes, this also should
    #always be a fixed value. 

    StartTimeMod = pd.to_numeric(pd.cut(StartTime, cums, labels=cums[0:(len(cums)-1)],right=False))
    EndTimeMod = pd.to_numeric(pd.cut(EndTime, cums, labels=cums[0:(len(cums)-1)],right=False))

    StartTime=StartTime-StartTimeMod
    EndTime = EndTime-EndTimeMod

    list_of_series = [StartTime,EndTime,LowFreq,HighFreq,StartFile,EndFile,DeploymentID,probs]

    df = pd.concat(list_of_series, axis=1)

    #remove by index the Exlude sections.
    if ExcludeEnd.size !=0:
        ExcludeAfter = min(ExcludeEnd)

        df = df[:ExcludeAfter]
        
    if ExcludeStart.size !=0:
        ExcludeBefore = max(ExcludeStart)+1

        df = df[ExcludeBefore:]


    df_list[i]=df

    print("samples processed:" + str(samples_total))

#i_last = i 
#end indent
        
    
df=pd.concat(df_list,axis=0,ignore_index=True)

df.columns=["StartTime","EndTime","LowFreq","HighFreq","StartFile","EndFile","Deployment","probs"]

del df['Deployment']

df.to_csv(resultpath + '/DETx.csv.gz',index=False,compression='gzip')

#out = [i_last,len(FG.DiffTime.unique())-1,df]
