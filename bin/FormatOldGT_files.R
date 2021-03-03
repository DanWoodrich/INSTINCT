#convert Sfiles_and_durations to FG format: 
#this is a simple convenience script that could be recreated to work with any lab metadata formats. Not meant to be bugproof or 
#work for a variety of formats 

library(doParallel)

Species<-"LM"
Decimate<-"No_whiten_decimate_by_128"
DecimateShort<-"decimate_by_128"


folderName<-paste("//akc0ss-n086/NMML_CAEP_Acoustics/Detector/Combined_sound_files",Species,Decimate,sep="/")

#find .csv files in folder
files<-dir(folderName,pattern=".csv")
transferSF<-"n"

for(n in files){
  
  SfilesName<-n
  
  data<-read.csv(paste(folderName,SfilesName,sep="/"))
  
  dash2<-gregexpr("-",data$SFsh[1])[[1]][2]
  dot1<-gregexpr("\\.",data$SFsh[1])[[1]][1]
  dateTimeFormat<-substr(data$SFsh,dash2+1,dot1-1)
  
  und3<-gregexpr("_",data$MooringName[1])[[1]][2]
  siteID<-substr(data$MooringName,und3+1,nchar(as.character(data$MooringName[1])))
  
  outData<-data.frame(cbind(as.character(data$SFsh),"/",dateTimeFormat,data$Duration,as.character(data$MooringName),siteID))
  colnames(outData)<-c("FileName","FullPath","StartTime","Duration","Deployment","SiteID")
  
  outData$Duration<-as.numeric(as.character(outData$Duration))
  outData$FileName<-as.character(outData$FileName)
  
  
  und5<-gregexpr("_",SfilesName)[[1]][5]
  saveName<-substr(SfilesName,1,und5-1)
  
  #load in the GT data 
  
  GTfileName<-paste("//akc0ss-n086/NMML_CAEP_Acoustics/Detector/RavenBLEDscripts/Data/Selection tables/",Species,"/",data$MooringName[1],"Sum/",saveName,".txt",sep="")
  
  GTdata<-read.delim(GTfileName,row.names=NULL)
  GTdata<-GTdata[which(GTdata$View=="Spectrogram 1"),]
  GTdata$Begin.Time..s.<-as.numeric(GTdata$Begin.Time..s.)
  GTdata$End.Time..s.<-as.numeric(GTdata$End.Time..s.)
  
  outData$cumsum<- cumsum(outData$Duration)-outData$Duration[1]
  #outData$cumsum<- cumsum(outData$Duration)
  
  #this is a super jenk way to do this but this part doesn't need to scale so I don't care
  
  values<-foreach(i=1:nrow(GTdata)) %do% {
    startFile<-"UK"
    k=1
    while(startFile=="UK"&k<=nrow(outData)){
      if(GTdata$Begin.Time..s.[i]<outData$cumsum[k]){
        startFile<-outData$FileName[k-1]
        startTime<-GTdata$Begin.Time..s.[i]-outData$cumsum[k-1]
      }else{
        k=k+1
      }
    }
    if(k>nrow(outData)){
      startFile<-outData$FileName[k-1]
      startTime<-GTdata$Begin.Time..s.[i]-outData$cumsum[k-1]
    }
    endFile<-"UK"
    k=1
    while(endFile=="UK"&k<=nrow(outData)){
      if(GTdata$End.Time..s.[i]<outData$cumsum[k]){
        endFile<-outData$FileName[k-1]
        endTime<-GTdata$End.Time..s.[i]-outData$cumsum[k-1]
      }else{
        k=k+1
      }
    }
    if(k>nrow(outData)){
      endFile<-outData$FileName[k-1]
      endTime<-GTdata$End.Time..s.[i]-outData$cumsum[k-1]
    }
    
    return(c(startTime,endTime,startFile,endFile))
    
  }
  
  values<-do.call("rbind",values)
  
  GTout<-data.frame(values[,c(1,2)],GTdata$Low.Freq..Hz.,GTdata$High.Freq..Hz.,values[,c(3,4)])
  
  colnames(GTout)<-c("StartTime","EndTime","LowFreq","HighFreq","StartFile","EndFile")
  
  GTout$label<-"y"
  GTout$Type<-"i_neg"
  GTout$SignalCode<-Species
  
  saveName<-paste(Species,saveName,sep="_")
  
  write.csv(GTout,paste("C:/instinct_dt/Data/GroundTruth/",saveName,".csv",sep=""),row.names = FALSE)
  

}