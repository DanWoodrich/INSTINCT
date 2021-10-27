MethodID<-"rd-simple-w-metadata-v1-2"

#1.1: make it so any extra metadata is retained
#1.2: include a filename arg

args="//akc0ss-n086/NMML_CAEP_Acoustics/Detector/LF moan project/Cole Summer 2021 materials and analysis/Random sample review/Cole data product/RAVENx.txt //akc0ss-n086/NMML_CAEP_Acoustics/Detector/LF moan project/Cole Summer 2021 materials and analysis/Random sample review/File Group used/oneHRonePerc.csv C:/Users/daniel.woodrich/Desktop/database"

args<-strsplit(args,split=" ")[[1]]

args<-commandArgs(trailingOnly = TRUE)

RAVpath <- args[1]
FGpath <-args[2]
resultPath <- args[3]
fileName <- args[4]

RavGT<-read.delim(paste(RAVpath,fileName,sep="/"))
FG<-read.csv(paste(FGpath,"FileGroupFormat.csv.gz",sep="/"))

#throw out the segment info. 
FG<-FG[which(!duplicated(FG$FileName)),]

#reduce RavGT files to just names, not locations. 

RavGT<-RavGT[which(RavGT$View!="Waveform 1"),]

RavGT<-RavGT[,which(!colnames(RavGT) %in% c("Selection","View","Channel"))]

RavGT<-RavGT[order(RavGT$Begin.Time..s.),]

#get rid of not considered, and placeholder

RavGT<-RavGT[which(RavGT$SignalCode!="Not Considered"&RavGT$SignalCode!="Placeholder"),]

#changed this from backslash to forward slash, but not sure why it is coming out different...
for(i in 1:nrow(RavGT)){
  slashes<-length(gregexpr("/|\\\\",RavGT$Begin.Path[i])[[1]])
  lastSlash<-gregexpr("/|\\\\",RavGT$Begin.Path[i])[[1]][slashes]
  RavGT$Begin.Path[i]<-substr(RavGT$Begin.Path[i],lastSlash+1,nchar(RavGT$Begin.Path[i]))
}

for(i in 1:nrow(RavGT)){
  slashes<-length(gregexpr("/|\\\\",RavGT$End.Path[i])[[1]])
  lastSlash<-gregexpr("/|\\\\",RavGT$End.Path[i])[[1]][slashes]
  RavGT$End.Path[i]<-substr(RavGT$End.Path[i],lastSlash+1,nchar(RavGT$End.Path[i]))
}

#convert RavGT names back to DETx standard
colnames(RavGT)[1:8]<-c("StartTime","EndTime","LowFreq","HighFreq","StartFile","EndFile","FileOffset","DeltaTime")


colnames(FG)[which(colnames(FG)=="FileName")]<-"StartFile"
FG$StartTime<-NULL

#merge RavGT with FG
RaVGTFG<-merge(RavGT,FG,by="StartFile")

#start becomes file offset
RaVGTFG$StartTime<-RaVGTFG$FileOffset

#end becomes file offset + delta time
RaVGTFG$EndTime<-RaVGTFG$StartTime+RaVGTFG$DeltaTime

#at the end, do a check if any end times are > duration (from FG). If so, subtract them by FG duration. 
RaVGTFG$EndTime[which(RaVGTFG$EndTime>RaVGTFG$Duration)]<-RaVGTFG$EndTime-RaVGTFG$Duration

#remove unnecessary non metadata columns. 

out<-data.frame(RaVGTFG[,2:5],RaVGTFG[,1],RaVGTFG[,6],RaVGTFG[,9:11])

if(length(RavGT)>11){
  out<-cbind(out,RavGT[,12:length(RavGT)])
  colnames(out)[10:(length(RavGT)-2)]<-colnames(RavGT)[12:length(RavGT)]
}

colnames(out)[5:6]<-c("StartFile","EndFile")

write.csv(out,gzfile(paste(resultPath,"DETx.csv.gz",sep="/")),row.names = FALSE)


