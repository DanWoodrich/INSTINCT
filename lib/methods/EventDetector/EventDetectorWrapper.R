#set these variables in all containers:

library(doParallel)
library(tuneR)
library(signal)
library(foreach)


#windows test values
#DataPath <-"//161.55.120.117/NMML_AcousticsData/Audio_Data/Waves/"        #docker volume
#FGpath <- "C:/Apps/INSTINCT/Cache/7f1040f41deafd01007a7cd0ad636c71fc686212"  #docker volume
#ParamPath <- "C:/Apps/INSTINCT/etc"
#resultPath<-"C:/Apps/INSTINCT/Out"
#ReadFile<-'FileGroupFormatSplit3.csv.gz'
#EDstage<-"1"

#crs<- as.numeric(99)
#chunkSize<- as.numeric(20)

#make sure these are alphabetical (alphabeterical to python variables that is)
#bandOvlp = as.numeric(0.5)
#combineMethod = "Stacked"
#dBadd<- as.numeric(2)
#highFreq<-as.numeric(50)
#lowFreq<-as.numeric(25)
#maxDur = as.numeric(100)
#minDur = as.numeric(1)
#minFreq = as.numeric(0)
#noiseHopLength<-as.numeric(2)
#noiseThresh<-as.numeric(0.25)
#noiseWinLength<-as.numeric(40)
#numBands <- as.numeric(1)
#targetSampRate<-128
#Overlap<-as.numeric(0)
#windowLength<-as.numeric(128)

ProjectRoot<-"C:/Apps/INSTINCT/"
DataPath <- "//161.55.120.117/NMML_AcousticsData/Audio_Data/DecimatedWaves/1024"
FGpath <-"C:/Apps/INSTINCT/Cache/8fe3bdf1da0e/"
resultPath <- "C:/Apps/INSTINCT/Cache/8fe3bdf1da0e/0f7df1"
ReadFile<-"FileGroupFormat1.csv.gz"
EDstage<-"1"

crs<-99
chunkSize<- 20

paramArgsPre<-"C:/Apps/INSTINCT/ //161.55.120.117/NMML_AcousticsData/Audio_Data/DecimatedWaves/1024 C:/Apps/INSTINCT/Cache/98ccbe6898da/ C:/Apps/INSTINCT/Cache/98ccbe6898da/73d874 FileGroupFormat1.csv.gz 1 99 20 method1 contour-w-slope-r-source-v1-10 Downsweep 500 -99999 -2 65 50 1.3 2 40 0.90 0.7 10 3 90 75 1024 100 contour-w-slope-r-source-v1-10 desired_slope high_freq hough_slope_max hough_slope_min img_thresh1 img_thresh2 isoblur_sigma1 isoblur_sigma2 low_freq noise_thresh noise_thresh_end noise_win_length noise_win_length_end overlap pix_thresh t_samp_rate window_length"
args<-strsplit(paramArgsPre,split=" ")[[1]]

#To make this general to ED, need to pass method params instead of hard defining here. 
args<-commandArgs(trailingOnly = TRUE)

#argument values
ProjectRoot<-args[1]
DataPath <- args[2]
FGpath <- args[3]
resultPath <- args[4]
ReadFile<-args[5]
EDstage<-args[6]

crs<- as.numeric(args[7])
chunkSize<- as.numeric(args[8])



MethodID<-args[10]

#after methodID, index @ methodID+1 to the end will be comprised of a methods Vector, and a parameter name vector of equal length

#11:(11+((length(args)-10)/2)) = methodArgs
#(11+((length(args)-10)/2))+1:length(args) = methodArgsNames

argsLen<-length(11:length(args))-1
argsSep<-argsLen/2

ParamArgs<-args[11:(11+argsSep)]
ParamNames<-args[(11+argsSep+1):length(args)]

targetSampRate<-as.integer(ParamArgs[which(ParamNames=="t_samp_rate")])

#not using these... yet. But this is how you get them 
windowLength<-as.integer(ParamArgs[which(ParamNames=="window_length")])
Overlap<-as.integer(ParamArgs[which(ParamNames=="overlap")])

#
mIDind<-gregexpr("-",MethodID)[[1]][length(gregexpr("-",MethodID)[[1]])-1]
MethodIDcut<-substr(MethodID,0,mIDind-1)

#populate with needed fxns for ED
SourcePath<-paste(ProjectRoot,"/lib/methods/EventDetector/",MethodIDcut,"/",MethodID,".R",sep="")
source(SourcePath) 

#load ED libraries: 
if(length(libraries)>0){
  for(l in 1:length(libraries)){
    library(libraries[l],character.only=TRUE)
  }
}

#and general fxns
source(paste(ProjectRoot,"/lib/supporting/instinct_fxns.R",sep="")) 

data<-read.csv(paste(FGpath,ReadFile,sep="/"))

#split dataset into difftime group to parallelize 
filez<-nrow(data)

#big process: do split chunks evenly to ensure close to equal processing times
if(EDstage=="1"){
  BigChunks<-ceiling(filez/(crs*chunkSize))
  #splitID<-2
  splitID<-as.integer(substr(ReadFile,16,nchar(ReadFile)-7)) #assumes this stays as FileGroupFormatSplitx.csv.gz (tolerant of more digits)
  
  crsRead<-crs
  
detOut<-foreach(i=1:BigChunks) %do% {
  #reload crs at start of every loop 

  crs<- crsRead
  if(crs>detectCores()){
    crs<-detectCores()
  }
  
  StartFile<-(1+i*(crs*chunkSize)-(crs*chunkSize))
  if(i!=BigChunks){
    EndFile<-i*(crs*chunkSize)
  }else{
    EndFile<-filez
  }
  
  FilezPerCr<-ceiling(length(StartFile:EndFile)/crs)
  
  FilezAssign<-rep(1:crs,each=FilezPerCr)
  FilezAssign<-FilezAssign[1:length(StartFile:EndFile)]
  
  #reassign crs based on crs which actually made it into file split (will be crs on each except possibly not on last BigChunk)
  crs<-length(unique(FilezAssign))
  
  #eventually go to effort to make a step here to presave data and grab it with each core. 
  
  
  #foreach into chunks 
  
  #probably should make packages loaded in dynamically
  
  startLocalPar(crs,"FilezAssign","data","EventDetectoR","specgram","splitID","StartFile","EndFile","ParamArgs","targetSampRate","decimateData","resampINST","decDo","prime.factor","readWave2",nameSpaceFxns)
  
  Detections<-foreach(n=1:crs,.packages=c("tuneR","doParallel","signal",librariesToLoad)) %dopar% {

    dataIn<-data[StartFile:EndFile,][which(FilezAssign==n),]
    
    #process per diffTime chunk
    outList <- vector(mode = "list")
    for(h in unique(dataIn$DiffTime)){
      #identifier for how file was processed
      processTag<-paste(h,splitID,i,n,sep="_")
      
      #load the sound file(s) into memory
      dataMini<-dataIn[which(dataIn$DiffTime==h),]
      if(nrow(dataMini)==1){
        dataMini$cumsum<-0
      }else{
        dataMini$cumsum<-c(0,cumsum(dataMini$SegDur)[1:(nrow(dataMini)-1)])
      }
      filePaths<-paste(DataPath,paste(dataMini$FullPath,dataMini$FileName,sep=""),sep="")

      SoundList <- vector(mode = "list", length = nrow(dataMini))
      
      #here, could decide to load in a little more context if available? Then trim detections at the end? might be nice, but 
      #may not be worth addressing yet. 
      
      #decided not to load in full wav and retain between loops. This means more reads, but shorter file length reads. Could change this. 
      for(g in 1:nrow(dataMini)){
        SoundList[[g]]<-readWave2(filePaths[g],from=dataMini$SegStart[g],to=dataMini$SegStart[g]+dataMini$SegDur[g],unit="seconds")
      }
      
      soundFile<-do.call(bind, SoundList)
      
      soundFile<-decimateData(soundFile,targetSampRate)
      
      #render spectrogram : Doesn't do much else here, but potentially useful option. 
      #spectrogram<- specgram(x = Wav@left,
      #                       Fs = Wav@samp.rate,
      #                       window=windowLength,
      #                       overlap=Overlap
      #)
      
      outputs<-EventDetectoR(soundFile,spectrogram=NULL,dataMini,ParamArgs)
      
      
      if(length(outputs)>0){

      Cums<-data.frame(cut(outputs[,1],breaks=c(0,cumsum(dataMini$SegDur)),labels=dataMini$cumsum),
                          cut(outputs[,2],breaks=c(0,cumsum(dataMini$SegDur)),labels=dataMini$cumsum),
                       cut(outputs[,1],breaks=c(0,cumsum(dataMini$SegDur)),labels=dataMini$FileName),
                       cut(outputs[,2],breaks=c(0,cumsum(dataMini$SegDur)),labels=dataMini$FileName),
                       cut(outputs[,1],breaks=c(0,cumsum(dataMini$SegDur)),labels=dataMini$SegStart),
                       cut(outputs[,2],breaks=c(0,cumsum(dataMini$SegDur)),labels=dataMini$SegStart))
      Cums[,1]<-as.numeric(levels(Cums[,1])[Cums[,1]])
      Cums[,2]<-as.numeric(levels(Cums[,2])[Cums[,2]])
      Cums[,5]<-as.numeric(levels(Cums[,5])[Cums[,5]])
      Cums[,6]<-as.numeric(levels(Cums[,6])[Cums[,6]])
      
      StartMod<-outputs[,1]+Cums[,5]-Cums[,1]
      EndMod<-outputs[,2]+Cums[,6]-Cums[,2]

      #convert outputs to have startfile, starttime, endfile, endtime. 
      outputs<-data.frame(StartMod,EndMod,outputs[,3],outputs[,4],Cums[,3],Cums[,4],processTag)
      colnames(outputs)<-c('StartTime','EndTime','LowFreq','HighFreq','StartFile',"EndFile","ProcessTag")
      
      }else{
        outputs<-NULL
      }
      
    outList[[h]]<-outputs
    }
    outList<-do.call('rbind',outList)
    return(outList)
  }
  stopCluster(cluz)
  
  Detections<-do.call('rbind',Detections)
  return(Detections)
}

#write to result
outName<-paste("DETx",splitID,".csv.gz",sep="")  


}else if(EDstage=="2"){
  #small process: use to unify breaks in larger process
  #keep difftimes together, but can still break into 
  #crs/chunk size batches to process 
  
  if(length(unique(data$DiffTime))<crs){
    crs<-length(unique(data$DiffTime))
  }
  
  startLocalPar(crs,"data","EventDetectoR","specgram","ParamArgs","targetSampRate","decimateData","resampINST","decDo","prime.factor","readWave2",nameSpaceFxns)
  
  detOut<-foreach(n=unique(data$DiffTime),.packages=c("tuneR","doParallel","signal",librariesToLoad)) %dopar% {
    dataMini<-data[which(data$DiffTime==n),]
    if(nrow(dataMini)==1){
      dataMini$cumsum<-0
    }else{
      dataMini$cumsum<-c(0,cumsum(dataMini$SegDur)[1:(nrow(dataMini)-1)])
    }
    filePaths<-paste(DataPath,paste(dataMini$FullPath,dataMini$FileName,sep=""),sep="")
    
    SoundList <- vector(mode = "list", length = nrow(dataMini))
    for(g in 1:nrow(dataMini)){
      SoundList[[g]]<-readWave2(filePaths[g],from=dataMini$SegStart[g],to=dataMini$SegStart[g]+dataMini$SegDur[g],unit="seconds")
    }
    soundFile<-do.call(bind, SoundList)
    
    #run detector
    
    soundFile<-decimateData(soundFile,targetSampRate)
    
    #render spectrogram : Doesn't do much else here, but potentially useful option. 
    #spectrogram<- specgram(x = Wav@left,
    #                       Fs = Wav@samp.rate,
    #                       window=windowLength,
    #                       overlap=Overlap
    #)
    
    outputs<-EventDetectoR(soundFile,spectrogram=NULL,dataMini,ParamArgs)
    
    if(length(outputs)>0){
      Cums<-data.frame(cut(outputs[,1],breaks=c(0,cumsum(dataMini$SegDur)),labels=dataMini$cumsum),
                       cut(outputs[,2],breaks=c(0,cumsum(dataMini$SegDur)),labels=dataMini$cumsum),
                       cut(outputs[,1],breaks=c(0,cumsum(dataMini$SegDur)),labels=dataMini$FileName),
                       cut(outputs[,2],breaks=c(0,cumsum(dataMini$SegDur)),labels=dataMini$FileName),
                       cut(outputs[,1],breaks=c(0,cumsum(dataMini$SegDur)),labels=dataMini$SegStart),
                       cut(outputs[,2],breaks=c(0,cumsum(dataMini$SegDur)),labels=dataMini$SegStart))
      Cums[,1]<-as.numeric(levels(Cums[,1])[Cums[,1]])
      Cums[,2]<-as.numeric(levels(Cums[,2])[Cums[,2]])
      Cums[,5]<-as.numeric(levels(Cums[,5])[Cums[,5]])
      Cums[,6]<-as.numeric(levels(Cums[,6])[Cums[,6]])
      
      StartMod<-outputs[,1]+Cums[,5]-Cums[,1]
      EndMod<-outputs[,2]+Cums[,6]-Cums[,2]
    
    #convert outputs to have startfile, starttime, endfile, endtime. 
    outputs<-data.frame(StartMod,EndMod,outputs[,3],outputs[,4],Cums[,3],Cums[,4],n)
    colnames(outputs)<-c('StartTime','EndTime','LowFreq','HighFreq','StartFile',"EndFile","DiffTime")
    
    }else{
      outputs<-NULL
    }
    
    return(outputs)
  #break data into 
  }

  outName<-paste("DETx.csv.gz",sep="")  
  
}

detOut<-do.call('rbind',detOut)


write.csv(detOut,gzfile(paste(resultPath,outName,sep="/")),row.names = FALSE)







