MethodID<-"rd-simple-w-metadata-v1-4"

#1.1: make it so any extra metadata is retained
#1.2: include a filename arg
#1.3: make general to any DETx, not just GT types. And, don't truncate # of columns provided. 
#1.4: Ok do truncate, just do it based on the FG data columns remaining. 

args="C:/Apps/INSTINCT/Outputs/EditGTwRaven/04f89c C:/Apps/INSTINCT/Cache/5564e23d3c9c C:/Apps/INSTINCT/Outputs/EditGTwRaven/04f89c/9bc076 RAVENx.txt"

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
if("SignalCode" %in% names(RavGT)&any(RavGT$SignalCode!="Not Considered"|RavGT$SignalCode!="Placeholder")){
RavGT<-RavGT[which(RavGT$SignalCode!="Not Considered"&RavGT$SignalCode!="Placeholder"),]
}

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

out<-data.frame(RaVGTFG[,2:5],RaVGTFG[,1],RaVGTFG[,6],RaVGTFG[,9:ncol(RaVGTFG)])

if(length(RavGT)>11){
  out<-cbind(out,RavGT[,12:length(RavGT)])
  colnames(out)[10:(ncol(out))]<-colnames(RavGT)[12:length(RavGT)]
}

colnames(out)[5:6]<-c("StartFile","EndFile")

out<-out[,-which(colnames(out) %in% colnames(FG))]

write.csv(out,gzfile(paste(resultPath,"DETx.csv.gz",sep="/")),row.names = FALSE)


