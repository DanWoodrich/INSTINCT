MethodID<-"rv-simple-w-metadata-v1-1"

DETpath<-"C:/Apps/INSTINCT/Cache/760e06eaabd4/a04a78/f16d98"
FGpath<-"C:/Apps/INSTINCT/Cache/760e06eaabd4/"

dataPath<-"//161.55.120.117/NMML_AcousticsData/Audio_Data/DecimatedWaves/128"

args<-commandArgs(trailingOnly = TRUE)

DETpath <- args[1]
FGpath <-args[2]
Resultpath <- args[3]
dataPath <- args[4]

#transform into Raven formatted data, retain data in other columns besides mandated 6. 


Dets<-read.csv(paste(DETpath,"DETx.csv.gz",sep="/"))
FG<-read.csv(paste(FGpath,"FileGroupFormat.csv.gz",sep="/"))


#mandatory column names
reqCols<-c("StartTime","EndTime","LowFreq","HighFreq","StartFile","EndFile")

if(any(!reqCols %in% colnames(Dets))){
  stop("Not a valid DETx object")
}

colnames(FG)[which(colnames(FG)=="FileName")]<-"StartFile"

FG$StartTime<-NULL

FG$cumsum=cumsum(FG$Duration)-FG$Duration[1]

DetsFG<-merge(Dets,FG,by="StartFile")

#calculate delta time for each detection
#process these seperately

DetsFGSameFile <-DetsFG[which(DetsFG$StartFile==DetsFG$EndFile),]
DetsFGdiffFile <-DetsFG[which(!DetsFG$StartFile==DetsFG$EndFile),]

#for same time, just end - start
DetsFGSameFile$DeltaTime<-DetsFGSameFile$EndTime-DetsFGSameFile$StartTime

DetsFGdiffFile$DeltaTime<-(DetsFGdiffFile$Duration-DetsFGdiffFile$StartTime)+DetsFGdiffFile$EndTime

DetsFG<-rbind(DetsFGSameFile,DetsFGdiffFile)

DetsFG<-DetsFG[order(DetsFG$StartFile,DetsFG$StartTime),]

DetsFG$FileOffset<-DetsFG$StartTime

#Raven friendly start and end times. 
DetsFG$StartTime<-DetsFG$FileOffset+DetsFG$cumsum
DetsFG$EndTime<-DetsFG$StartTime+DetsFG$DeltaTime

#calculate fullpath for the end file as well 

colnames(FG)[which(colnames(FG)=="StartFile")]<-"EndFile"
EFFP<-merge(Dets,FG,by="EndFile")
EFFP<-EFFP[order(EFFP$StartFile,EFFP$StartTime),]

if(nrow(DetsFG)>=1){
  DetsFG$StartFile<-paste(dataPath,DetsFG$FullPath,DetsFG$StartFile,sep="")
  DetsFG$EndFile<-paste(dataPath,EFFP$FullPath,DetsFG$EndFile,sep="") 
}


#strike several metadata fields
dropCols<-c("DiffTime","FullPath","Deployment","SiteID","cumsum","Duration")

DetsFG<-DetsFG[,which(!colnames(DetsFG) %in% dropCols)]

keepCols<-c("StartTime","EndTime","LowFreq","HighFreq","StartFile","EndFile","FileOffset","DeltaTime")
DetsFGxtra<-data.frame(DetsFG[,which(!colnames(DetsFG) %in% keepCols)])

colnames(DetsFGxtra)<-colnames(DetsFG)[which(!colnames(DetsFG) %in% keepCols)]

if(nrow(DetsFG)>=1){
out<-data.frame(1:nrow(DetsFG),"Spectrogram 1",1,DetsFG$StartTime,DetsFG$EndTime,DetsFG$LowFreq,DetsFG$HighFreq,DetsFG$StartFile,
                DetsFG$EndFile,DetsFG$FileOffset,DetsFG$DeltaTime,DetsFGxtra)
}else{
  out<-data.frame(matrix(ncol = 11+length(DetsFGxtra), nrow = 0))
  if(length(DetsFGxtra)>0){
    colnames(out)[12:(11+length(DetsFGxtra))]<-names(DetsFGxtra)
  }
}

colnames(out)[1:11]<-c("Selection","View","Channel","Begin Time (s)","End Time (s)","Low Freq (Hz)","High Freq (Hz)",
                      "Begin Path","End Path","File Offset (s)","Delta Time (s)")

write.table(out,paste(Resultpath,'/RAVENx.txt',sep=""),quote=FALSE,sep = "\t",row.names=FALSE)
