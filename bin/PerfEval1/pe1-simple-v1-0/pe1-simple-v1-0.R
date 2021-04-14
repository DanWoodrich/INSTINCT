#performance statistics based on labels
#use IoU criteria: area of overlap over area of union 

#set these variables in all containers:
MethodID<-"pe1-simple-v1-0"

#test folder
FGpath<-"C:/Apps/INSTINCT/Cache/d7cc1b394b7e"
LABpath<-"C:/Apps/INSTINCT/Cache/d7cc1b394b7e/f636f8/bdedc3/b04cdf"
#SignalCode="LM"
resultPath="C:/Apps/INSTINCT/Cache/d7cc1b394b7e/f636f8/bdedc3/b04cdf/8875a4"

args<-commandArgs(trailingOnly = TRUE)

#docker values
FGpath <- args[1]
LABpath <- args[2]
INTpath <- args[3]
resultPath <- args[4]
FGID <- args[5]
PE1stage <- args[6]


if(PE1stage=="FG"){

#don't really have to retain the signal code label, it is basically just a parameter (which influence hashes, but not)
LABdata<-read.csv(paste(LABpath,"DETx.csv.gz",sep="/"))
FGdata<-read.csv(paste(FGpath,"FileGroupFormat.csv.gz",sep="/"))

#change in OB/MB statistic: now a # corresponding to ratio of GT and out TPs (larger ratio means more overbox, smaller ratio means more multibox)
#'OMB ratio'
#this comparison doesn't allow for cases where both OB and MB were both high- but this was not seen a lot in practice. 

detTotal<-nrow(LABdata[which(LABdata$SignalCode=="out"),])
numTPtruth<-nrow(LABdata[which(LABdata$SignalCode!="out"),])
numTP <- nrow(LABdata[which(LABdata$SignalCode!="out"&LABdata$label=="TP"),])
numTPout <- nrow(LABdata[which(LABdata$SignalCode=='out'&LABdata$label=="TP"),])
numFP <- nrow(LABdata[which(LABdata$label=="FP"),])
numFN <- nrow(LABdata[which(LABdata$SignalCode!="out"&LABdata$label=="FN"),])

Recall <- numTP/(numTP+numFN)                   
Precision <- numTP/(detTotal)
F1<- (2 * Precision * Recall) / (Precision + Recall)
OMB<-(numTP/numTPout) #values over one indicate 

#correlate the FN and TP sf bin totals. Values near 1 imply less time phenomena based and more random distribution of FN, values closer to 1 imply less random distributed negatives and more related to phenomena
#make vector of sound file bins
allFiles<-unique(LABdata$StartFile)

TPvec<-as.numeric(table(c(LABdata[which(LABdata$SignalCode!="out"&LABdata$label=="TP"),"StartFile"],allFiles)))-1
FNvec<-as.numeric(table(c(LABdata[which(LABdata$SignalCode!="out"&LABdata$label=="FN"),"StartFile"],allFiles)))-1

HitMissCor<-cor(TPvec,FNvec)

#stats related to dispersion of calls over time 
EffortHours<-sum(FGdata$Duration)/3600
TPperHour<-numTPtruth/(sum(FGdata$Duration)/3600)
TPdetperHour<-numTP/(sum(FGdata$Duration)/3600)
FPperHours<-numFP/(sum(FGdata$Duration)/3600)
numFNperHOur<-numFN/(sum(FGdata$Duration)/3600)

Stats<-data.frame(cbind(FGID,detTotal,numTPtruth,numTP,numTPout,numFP,numFN,Recall,Precision,F1,OMB,HitMissCor,EffortHours,TPperHour,TPdetperHour,FPperHours,numFNperHOur))

write.csv(Stats,gzfile(paste(resultPath,"Stats.csv.gz",sep="/")),row.names = FALSE)

}else if(PE1stage=="All"){
  
  INTdata<-read.csv(paste(INTpath))
  
  #INTdata<-read.csv(C:/instinct_dt/Outputs/EDperfeval/63b5f9cc7a7fdf504a8e2fe50cd2661e97019e41/EDperfeval_Intermediate.csv)
  
  detTotal<-sum(INTdata$detTotal)
  numTPtruth<-sum(INTdata$numTPtruth)
  numTP <- sum(INTdata$numTP)
  numTPout <- sum(INTdata$numTPout)
  numFP <- sum(INTdata$numFP)
  numFN <- sum(INTdata$numFN)
  
  Recall <- numTP/(numTP+numFN)                   
  Precision <- numTP/(detTotal)
  F1<- (2 * Precision * Recall) / (Precision + Recall)
  OMB<-(numTP/numTPout) #values over one indicate 
  
  #correlate the FN and TP sf bin totals. Values near 1 imply less time phenomena based and more random distribution of FN, values closer to 1 imply less random distributed negatives and more related to phenomena
  HitMissCor<-mean(INTdata$HitMissCor,na.rm=TRUE)
  
  #stats related to dispersion of calls over time 
  EffortHours<-sum(INTdata$EffortHours)
  TPperHour<-numTPtruth/EffortHours
  TPdetperHour<-numTP/EffortHours
  FPperHours<-numFP/EffortHours
  numFNperHOur<-numFN/EffortHours
  
  FGID<-"all"
  
  StatsMean<-data.frame(cbind(FGID,detTotal,numTPtruth,numTP,numTPout,numFP,numFN,Recall,Precision,F1,OMB,HitMissCor,EffortHours,TPperHour,TPdetperHour,FPperHours,numFNperHOur))

  write.csv(rbind(INTdata,StatsMean),gzfile(resultPath),row.names = FALSE)
  
  #summarize above statistics
  
}


