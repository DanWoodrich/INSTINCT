MethodID<-"rd-simple-w-metadata-v1-0"

args="C:/Apps/INSTINCT/Outputs/EditGTwRaven/9af322 C:/Apps/INSTINCT/Cache/f3b760ad8bec C:/Apps/INSTINCT/Outputs/EditGTwRaven/9af322/70d959"

args<-strsplit(args,split=" ")[[1]]

args<-commandArgs(trailingOnly = TRUE)

RAVpath <- args[1]
FGpath <-args[2]
resultPath <- args[3]

RavGT<-read.delim(paste(RAVpath,"RAVENx.txt",sep="/"))
FG<-read.csv(paste(FGpath,"FileGroupFormat.csv.gz",sep="/"))

#reduce RavGT files to just names, not locations. 

RavGT<-RavGT[which(RavGT$View!="Waveform 1"),]

RavGT<-RavGT[,which(!colnames(RavGT) %in% c("Selection","View","Channel"))]

RavGT<-RavGT[order(RavGT$Begin.Time..s.),]

for(i in 1:nrow(RavGT)){
  slashes<-length(gregexpr("\\\\",RavGT$Begin.Path[i])[[1]])
  lastSlash<-gregexpr("\\\\",RavGT$Begin.Path[i])[[1]][slashes]
  RavGT$Begin.Path[i]<-substr(RavGT$Begin.Path[i],lastSlash+1,nchar(RavGT$Begin.Path[i]))
}

for(i in 1:nrow(RavGT)){
  slashes<-length(gregexpr("\\\\",RavGT$End.Path[i])[[1]])
  lastSlash<-gregexpr("\\\\",RavGT$End.Path[i])[[1]][slashes]
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

colnames(out)[5:6]<-c("StartFile","EndFile")

write.csv(out,gzfile(paste(resultPath,"DETx.csv.gz",sep="/")),row.names = FALSE)


