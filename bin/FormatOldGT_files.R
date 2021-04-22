#convert Sfiles_and_durations to FG format: 
#this is a simple convenience script that could be recreated to work with any lab metadata formats. Not meant to be bugproof or 
#work for a variety of formats 

library(doParallel)

Species<-"RW"
Decimate<-"No_whiten_decimate_by_16"
DecimateShort<-"decimate_by_16"


folderName<-paste("//akc0ss-n086/NMML_CAEP_Acoustics/Detector/Combined_sound_files",Species,Decimate,sep="/")

#find .csv files in folder
files<-dir(folderName,pattern=".csv")
transferSF<-"n"

for(n in files){
  
  SfilesName<-n
  
  data<-read.csv(paste(folderName,SfilesName,sep="/"))
  data$SFsh<-gsub("_", "-", data$SFsh)
  
  #for the outData, need to calculate difference to actual file start (sec in last 3 digits)
  mss<-paste("0",substr(data$SFsh,nchar(data$SFsh)-6,nchar(data$SFsh)-4),sep="")
  vals<-as.POSIXlt(mss,format="%M%S")
  vals<- vals$sec+vals$min*60
  
  dash2<-gregexpr("-",data$SFsh[1])[[1]][2]
  dot1<-gregexpr("\\.",data$SFsh[1])[[1]][1]
  dateTimeFormat<-substr(data$SFsh,dash2+1,dot1-1)
  
  if(!any(!nchar(dateTimeFormat)==15)){
    dateTimeFormat<-paste(substr(dateTimeFormat,3,12),"000",sep="")
  }
  
  data$SFsh<-paste(substr(data$SFsh,1,dash2),dateTimeFormat,".wav",sep="")
  

  und3<-gregexpr("_",data$MooringName[1])[[1]][2]
  siteID<-substr(data$MooringName,und3+1,nchar(as.character(data$MooringName[1])))
  
  outData<-data.frame(cbind(as.character(data$SFsh),"/",dateTimeFormat,data$Duration,as.character(data$MooringName),siteID,vals))
  colnames(outData)<-c("FileName","FullPath","StartTime","Duration","Deployment","SiteID","pngFileDiff")
  
  outData$Duration<-as.numeric(as.character(outData$Duration))
  
  outData$FileName<-as.character(outData$FileName)
  
  outData$pngFileDiff<-vals
  
  und5<-gregexpr("_",SfilesName)[[1]][5]
  saveName<-substr(SfilesName,1,und5-1)
  
  #load in the GT data 
  
  GTfileName<-paste("//akc0ss-n086/NMML_CAEP_Acoustics/Detector/RavenBLEDscripts/Data/Selection tables/",Species,"/",data$MooringName[1],"Sum/",saveName,".txt",sep="")
  
  GTdata<-read.delim(GTfileName,row.names=NULL)
  GTdata<-GTdata[which(GTdata$View=="Spectrogram 1"),]
  GTdata$Begin.Time..s.<-as.numeric(GTdata$Begin.Time..s.)
  GTdata$End.Time..s.<-as.numeric(GTdata$End.Time..s.)
  
  
  outData$cumsum<- cumsum(outData$Duration)-outData$Duration[1]
  outData$cumsum<- c(0,cumsum(outData$Duration)[1:(nrow(outData)-1)])
  #outData$cumsum<- cumsum(outData$Duration)
  
  #this is a super jenk way to do this but this part doesn't need to scale so I don't care

  
  
  values<-foreach(i=1:nrow(GTdata)) %do% {
  #for(i in 1:nrow(GTdata)){
    startFile<-"UK"
    k=1
    while(startFile=="UK"&k<=nrow(outData)){
      if(GTdata$Begin.Time..s.[i]<outData$cumsum[k]){
        startFile<-outData$FileName[k-1]
        startTime<-GTdata$Begin.Time..s.[i]-outData$cumsum[k-1]
        startTime2<-startTime+outData$pngFileDiff[k-1]
      }else{
        k=k+1
      }
    }
    if(k>nrow(outData)){
      startFile<-outData$FileName[k-1]
      startTime<-GTdata$Begin.Time..s.[i]-outData$cumsum[k-1]
      startTime2<-startTime+outData$pngFileDiff[k-1]
    }
    endFile<-"UK"
    k=1
    while(endFile=="UK"&k<=nrow(outData)){
      if(GTdata$End.Time..s.[i]<outData$cumsum[k]){
        endFile<-outData$FileName[k-1]
        endTime<-GTdata$End.Time..s.[i]-outData$cumsum[k-1]
        endTime2<-endTime+outData$pngFileDiff[k-1]
      }else{
        k=k+1
      }
    }
    if(k>nrow(outData)){
      endFile<-outData$FileName[k-1]
      endTime<-GTdata$End.Time..s.[i]-outData$cumsum[k-1]
      endTime2<-endTime+outData$pngFileDiff[k-1]
      
    }
    
    return(c(startTime2,endTime2,startFile,endFile))
    
  }
  
  values<-do.call("rbind",values)
  
  GTout<-data.frame(values[,c(1,2)],GTdata$Low.Freq..Hz.,GTdata$High.Freq..Hz.,values[,c(3,4)])
  
  colnames(GTout)<-c("StartTime","EndTime","LowFreq","HighFreq","StartFile","EndFile")
  
  GTout$label<-"y"
  GTout$Type<-"i_neg"
  GTout$SignalCode<-Species


  #modify stuff to be compatable with new NAS naming. change mooring name (save name), and wav names
  
  MooringName<-substr(SfilesName,1,gregexpr("_",SfilesName)[[1]][3]-1)
  
  #also, rename wavs if applicable:
  if(substr(GTout$StartFile[1],1,5)=="AU-BS"){
    
    MooringSite<-substr(GTout$StartFile[1],6,7)
    
    #if a BS mooring, find the #, then 
    newWavName<-paste("AU-BSPM",MooringSite,sep="")
    
    MooringName<-paste(substr(data$MooringName[1],1,8),"PM",MooringSite,sep="")
    
    if((substr(GTout$StartFile[1],8,8)=="a"|substr(GTout$StartFile[1],8,8)=="b")&MooringSite=="02"){
      newWavName<-paste(newWavName,substr(GTout$StartFile[1],8,8),sep="_")
      
      MooringName<-paste(MooringName,substr(GTout$StartFile[1],8,8),sep="-")
    }
    
    dash2<-gregexpr("-",GTout$StartFile[1])[[1]][2]
    dot1<-gregexpr("\\.",GTout$StartFile[1])[[1]][1]
    dateTimeFormatStart<-substr(GTout$StartFile,dash2+1,dot1-1)
    dateTimeFormatEnd<-substr(GTout$EndFile,dash2+1,dot1-1)
    
    
    GTout$StartFile<-paste(newWavName,"-",dateTimeFormatStart,".wav",sep="")
    GTout$EndFile<-paste(newWavName,"-",dateTimeFormatEnd,".wav",sep="")

  }
  
  if(substr(data$SFsh[1],1,5)=="AU-AL"|substr(data$SFsh[1],1,5)=="AU-AW"){
    
    #do not need to change .wav names
    
    #only works with single digit, but that's ok I think just for backwards conversion of this limited set
    MooringSite<-substr(MooringName,11,12)
    
    #if a BS mooring, find the #, then 
    MooringName<-paste(substr(MooringName[1],1,10),"0",MooringSite,sep="")
    
  }
  
  saveName<-paste(Species,MooringName,substr(SfilesName,gregexpr("_",SfilesName)[[1]][3]+1,und5-1),sep="_")
  
  write.csv(GTout,paste("C:/Apps/INSTINCT/Data/GroundTruth/",saveName,".csv",sep=""),row.names = FALSE)
  

}
