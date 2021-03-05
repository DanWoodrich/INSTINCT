#set these variables in all containers:

#To make this general to ED, need to pass method params instead of hard defining here. 
args<-commandArgs(trailingOnly = TRUE)

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
#Overlap<-as.numeric(0)
#windowLength<-as.numeric(128)

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

MethodArgs<-args[11:length(args)]

#populate with needed fxns. Determine by command arg (ProjectRoot and MethodID)
SourcePath<-paste(ProjectRoot,"/bin/EventDetector/",MethodID,"/",MethodID,".R",sep="")
source(SourcePath) 

data<-read.csv(paste(FGpath,ReadFile,sep="/"))

#split dataset into difftime group to parallelize 
filez<-nrow(data)


#big process: do split chunks evenly to ensure close to equal processing times
if(EDstage=="1"){
  
  BigChunks<-ceiling(filez/(crs*chunkSize))
  #splitID<-2
  splitID<-as.integer(substr(ReadFile,21,nchar(ReadFile)-7)) #assumes this stays as FileGroupFormatSplitx.csv.gz (tolerant of more digits)
  
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
  startLocalPar(crs,"FilezAssign","data","EventDetectoR","specgram","splitID","StartFile","EndFile","MethodArgs")
  
  Detections<-foreach(n=1:crs,.packages=c("tuneR","doParallel")) %dopar% {
    dataIn<-data[StartFile:EndFile,][which(FilezAssign==n),]
    #process per diffTime chunk
    outList <- vector(mode = "list")
    for(h in unique(dataIn$DiffTime)){
      #identifier for how file was processed
      processTag<-paste(h,splitID,i,n,sep="_")
      
      #load the sound file(s) into memory
      dataMini<-dataIn[which(dataIn$DiffTime==h),]
      dataMini$cumsum<-cumsum(dataMini$Duration)-dataMini$Duration[1]
      filePaths<-paste(DataPath,paste(dataMini$FullPath,dataMini$FileName,sep=""),sep="")
      if(nrow(dataMini)==1){
        soundFile=readWave(filePaths)
      }else{
        SoundList <- vector(mode = "list", length = nrow(dataMini))
        for(g in 1:nrow(dataMini)){
          SoundList[[g]]<-readWave(filePaths[g])
        }
        soundFile<-do.call(bind, SoundList)
      }
      
      #run detector
      
      outputs<-EventDetectoR(soundFile,dataMini,MethodArgs)
      if(length(outputs)>0){

      Cums<-data.frame(cut(outputs[,1],breaks=c(0,dataMini$cumsum+dataMini$Duration[1]),labels=dataMini$cumsum),
                          cut(outputs[,2],breaks=c(0,dataMini$cumsum+dataMini$Duration[1]),labels=dataMini$cumsum),
                       cut(outputs[,1],breaks=c(0,dataMini$cumsum+dataMini$Duration[1]),labels=dataMini$FileName),
                       cut(outputs[,2],breaks=c(0,dataMini$cumsum+dataMini$Duration[1]),labels=dataMini$FileName))
      Cums[,1]<-as.numeric(levels(Cums[,1])[Cums[,1]])
      Cums[,2]<-as.numeric(levels(Cums[,2])[Cums[,2]])
      
      StartMod<-outputs[,1]-Cums[,1]
      EndMod<-outputs[,2]-Cums[,2]

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
outName<-paste("EDSplit",splitID,".csv.gz",sep="")  


}else if(EDstage=="2"){
  #small process: use to unify breaks in larger process
  #keep difftimes together, but can still break into 
  #crs/chunk size batches to process 
  
  if(length(unique(data$DiffTime))<crs){
    crs<-length(unique(data$DiffTime))
  }
  
  startLocalPar(crs,"data","EventDetectoR","specgram","MethodArgs")
  
  detOut<-foreach(n=unique(data$DiffTime),.packages=c("tuneR","doParallel")) %dopar% {
    dataMini<-data[which(data$DiffTime==n),]
    dataMini$cumsum<-cumsum(dataMini$Duration)-dataMini$Duration[1]
    filePaths<-paste(DataPath,paste(dataMini$FullPath,dataMini$FileName,sep=""),sep="")
    
    SoundList <- vector(mode = "list", length = nrow(dataMini))
    for(g in 1:nrow(dataMini)){
      SoundList[[g]]<-readWave(filePaths[g])
    }
    soundFile<-do.call(bind, SoundList)
    
    outputs<-EventDetectoR(soundFile,dataMini,MethodArgs)
    
    if(length(outputs)>0){
    Cums<-data.frame(cut(outputs[,1],breaks=c(0,dataMini$cumsum+dataMini$Duration[1]),labels=dataMini$cumsum),
                     cut(outputs[,2],breaks=c(0,dataMini$cumsum+dataMini$Duration[1]),labels=dataMini$cumsum),
                     cut(outputs[,1],breaks=c(0,dataMini$cumsum+dataMini$Duration[1]),labels=dataMini$FileName),
                     cut(outputs[,2],breaks=c(0,dataMini$cumsum+dataMini$Duration[1]),labels=dataMini$FileName))
    Cums[,1]<-as.numeric(levels(Cums[,1])[Cums[,1]])
    Cums[,2]<-as.numeric(levels(Cums[,2])[Cums[,2]])
    
    StartMod<-outputs[,1]-Cums[,1]
    EndMod<-outputs[,2]-Cums[,2]
    
    #convert outputs to have startfile, starttime, endfile, endtime. 
    outputs<-data.frame(StartMod,EndMod,outputs[,3],outputs[,4],Cums[,3],Cums[,4],n)
    colnames(outputs)<-c('StartTime','EndTime','LowFreq','HighFreq','StartFile',"EndFile","DiffTime")
    }else{
      outputs<-NULL
    }
    
    return(outputs)
  #break data into 
  }

  outName<-paste("EDunify.csv.gz",sep="")  
  
}

detOut<-do.call('rbind',detOut)


write.csv(detOut,gzfile(paste(resultPath,outName,sep="/")),row.names = FALSE)







