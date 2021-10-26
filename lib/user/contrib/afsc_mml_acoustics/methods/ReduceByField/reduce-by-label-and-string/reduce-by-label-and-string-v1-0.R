MethodID<-"rd-simple-w-metadata-v1-1"

library("foreach")
#1.1: make it so any extra metadata is retained

args="C:/Apps/INSTINCT/Outputs/EditGTwRaven/cc8590/740837 C:/Apps/INSTINCT/Cache/c4a8525f118b C:/Apps/INSTINCT/Outputs/EditGTwRaven/cc8590/740837/498fa0  Cycle None Union #patternA,#patternB Union n y reduce-by-label-and-string-v1-0"

args<-strsplit(args,split=" ")[[1]]

args<-commandArgs(trailingOnly = TRUE)

GTpath <- args[1]
FGpath <-args[2]
resultPath <- args[3]

ByFileOrCycle<-args[4]
Label<-args[5]
LabelString_IorU<-args[6]
Strings<-args[7]
String_IorU<-args[8]
UseLabel<-args[9]
UseString<-args[10]

Strings<-strsplit(Strings,split=",")[[1]]

source(paste("C:/Apps/INSTINCT/lib/supporting/instinct_fxns.R",sep="")) 

GT<-read.csv(paste(GTpath,"DETx.csv.gz",sep="/"))
FG<-read.csv(paste(FGpath,"FileGroupFormat.csv.gz",sep="/"))

FG$StartTimePOSIXct<-as.POSIXct(FG$StartTime,format="%Y-%m-%d %H:%M:%S",tz="UTC")
FG$EndTimePOSIXct<-FG$StartTimePOSIXct+FG$SegStart+FG$SegDur

FG<-addCycle(FG,"Deployment","StartTime","StartTimePOSIXct","EndTimePOSIXct")

#find rows to keep

keepboolLab<-rep(FALSE,nrow(GT))

if(UseLabel=='y'){
  GTlabels<-GT$label==Label
  GTlabels[is.na(GTlabels)]<-FALSE
  
  keepboolLab<-keepboolLab|GTlabels
}

keepboolString<-matrix(FALSE,nrow=length(Strings),ncol=nrow(GT))

if(UseString=='y'){
  for(n in 1:length(Strings)){
    keepboolString[n,]<-grepl(Strings[n],GT$Comments)
  }
  
  if(String_IorU=="Intersection"){
    keepboolString<-apply(keepboolString,2,all)
  }else if(String_IorU=="Union"){
    keepboolString<-apply(keepboolString,2,any)
  }
}else{
  keepboolString<-rep(FALSE,nrow(GT))
}

keepbool<-rbind(keepboolLab,keepboolString)

if(LabelString_IorU=="Intersection"){
  keepbool<-apply(keepbool,2,all)
}else if(LabelString_IorU=="Union"){
  keepbool<-apply(keepbool,2,any)
}else{
  stop("LabelString_IorU must be 'Intersection' or 'Union'")
}


#subset GT. 
GT<-GT[which(keepbool),]

GTfiles<-unique(c(GT$StartFile,GT$EndFile))

#subset FG, by GT. 
if(ByFileOrCycle=="File"){
  FG<-FG[which(FG$FileName %in% GTfiles),]
}else if(ByFileOrCycle=="Cycle"){
  FGtemp<-FG[which(FG$FileName %in% GTfiles),]
  #calculate cycles in FGtemp
  cycle<-unique(FGtemp$Cycle)
  FG<-FG[which(FG$Cycle %in% cycle),]
}else{
  stop("ByFileOrCycle must be 'File' or 'Cycle'")
}

FG<-FG[,1:8]

FG$StartTime<-paste(substr(FG$StartTime,3,4),substr(FG$StartTime,6,7),substr(FG$StartTime,9,10),"-",substr(FG$StartTime,12,13),substr(FG$StartTime,15,16),substr(FG$StartTime,18,19),sep="")
#print out the files 
write.csv(GT,gzfile(paste(resultPath,"/DETx.csv.gz",sep="")),row.names = FALSE)
write.csv(FG,gzfile(paste(resultPath,"/FileGroupFormat.csv.gz",sep="")),row.names = FALSE)


