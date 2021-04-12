#apply labels to detector outputs
#use IoU criteria: area of overlap over area of union 

#set these variables in all containers:
MethodID<-"labels-w-time-oldstyle-v1-0"

#should be same container method that runs on probabalistic outputs when a cutoff is provided. 

#test folder
#FGpath<-"C:/instinct_dt/Cache/2bf717aef81044ab2e655e4351d4342cc08de10a"
#GTpath<-paste(FGpath,"/30d8a30d601cd9c9a9a8cd3d25b2dac37bbe9e7f",sep="")
#DETpath<-paste(FGpath,"/8a757bbec32b5b6a56bc98ac2748d2df64deaa0f/2cc09ef5daa22a410caf96a2b15d10537bd1a4b5/9b83e604c10167e92547431790a3008f3db64157/b7223d9f9cb98b690649bad120eb492adc667d3a",sep="")
#resultPath<-paste(FGpath,"/8a757bbec32b5b6a56bc98ac2748d2df64deaa0f/2cc09ef5daa22a410caf96a2b15d10537bd1a4b5/9b83e604c10167e92547431790a3008f3db64157/b7223d9f9cb98b690649bad120eb492adc667d3a/2cc09ef5daa22a410caf96a2b15d10537bd1a4b5",sep="")
#IoUThresh<-0.50
#SignalCode="LM"

args<-commandArgs(trailingOnly = TRUE)

#docker values
FGpath <- args[1]
GTpath <- args[2]
DETpath <- args[3]
resultPath <- args[4]

GTdata<-read.csv(paste(GTpath,"GTFormat.csv.gz",sep="/"))
FGdata<-read.csv(paste(FGpath,"FileGroupFormat.csv.gz",sep="/"))

outData<-read.csv(paste(DETpath,"DETx.csv.gz",sep="/"))

#extract necessary info
#outData<-outData[,c("StartTime","EndTime","LowFreq","HighFreq","StartFile","EndFile")]



#Probabalistic<-FALSE
#introduce this in most sensible way once we get to 

#order datasets: 
GTdata<-GTdata[order(GTdata$StartFile,GTdata$StartTime),]
outData<-outData[order(outData$StartFile,outData$StartTime),]

FGdata$FileName<-as.character(FGdata$FileName)
GTdata$StartFile<-as.character(GTdata$StartFile)
GTdata$EndFile<-as.character(GTdata$EndFile)

FGdata$cumsum<-cumsum(FGdata$Duration)-FGdata$Duration[1]

#convert each dataset into time from FG start instead of offset 

GTlong<-GTdata

for(i in 1:nrow(GTlong)){
  GTlong$StartTime[i]<-GTlong$StartTime[i]+FGdata$cumsum[which(FGdata$FileName==GTlong$StartFile[i])]
  GTlong$EndTime[i]<-GTlong$EndTime[i]+FGdata$cumsum[which(FGdata$FileName==GTlong$EndFile[i])]
}

GTdata$StartFile<-as.factor(GTdata$StartFile)
GTdata$EndFile<-as.factor(GTdata$EndFile)

outLong<-outData

for(i in 1:nrow(outLong)){
  outLong$StartTime[i]<-outLong$StartTime[i]+FGdata$cumsum[which(FGdata$FileName==outLong$StartFile[i])]
  outLong$EndTime[i]<-outLong$EndTime[i]+FGdata$cumsum[which(FGdata$FileName==outLong$EndFile[i])]
}

GTlong$Dur<-GTlong$EndTime-GTlong$StartTime

#order datasets: 
GTlong<-GTlong[order(GTlong$StartTime),]
outLong<-outLong[order(outLong$StartTime),]

GTlong$label<-""
outLong$label<-""

GTlong$meantime<-GTlong$EndTime-((GTlong$EndTime-GTlong$StartTime)/2)
outLong$meantime<-outLong$EndTime-((outLong$EndTime-outLong$StartTime)/2)

#calculate whether labels is TP FP or FN 

for(h in 1:nrow(outLong)){
  gvec <- which(GTlong$meantime<(outLong$meantime[h]+max(GTlong$Dur)+1)&GTlong$meantime>(outLong$meantime[h]-max(GTlong$Dur)-1))
  if(length(gvec)>0){
    for(g in min(gvec):max(gvec)){
      if(((outLong$meantime[h]>GTlong$StartTime[g]) & (outLong$meantime[h]<GTlong$EndTime[g]))|((GTlong$meantime[g]>outLong$StartTime[h]) & (GTlong$meantime[g]<outLong$EndTime[h]))){
          outLong$label[h]<-"TP"
        }
      }
    }
  }


outLong$label[which(outLong$label!="TP")]<-"FP"


for(h in 1:nrow(GTlong)){
  gvec <- which(outLong$meantime<(GTlong$meantime[h]+max(GTlong$Dur)+1)&outLong$meantime>(GTlong$meantime[h]-max(GTlong$Dur)-1))
  if(length(gvec)>0){
    for(g in min(gvec):max(gvec)){
      if(((GTlong$meantime[h]>outLong$StartTime[g]) & (GTlong$meantime[h]<outLong$EndTime[g]))|((outLong$meantime[g]>GTlong$StartTime[h]) & (outLong$meantime[g]<GTlong$EndTime[h]))){
        GTlong$label[h]<-"TP"
      }
    }
  }
}


outLong$label[which(outLong$label!="TP")]<-"FP"
GTlong$label[which(GTlong$label!="TP")]<-"FN"





GTlong$StartTime<-GTdata$StartTime
GTlong$EndTime<-GTdata$EndTime

outLong$StartTime<-outData$StartTime
outLong$EndTime<-outData$EndTime

GTlong$label<-as.factor(GTlong$label)
outLong$label<-as.factor(outLong$label)

GTlong$Dur<-NULL
GTlong$Label<-NULL #I may want this later? But relevant labels past this point should be TP FP FN... 

outLong$SignalCode<-'out'

CombineTab<-rbind(GTlong,outLong)

CombineTab<-CombineTab[order(CombineTab$StartFile,CombineTab$StartTime),]

CombineTab$meantime<-NULL

outName<-paste("DETx.csv.gz",sep="_")

write.csv(CombineTab,gzfile(paste(resultPath,outName,sep="/")),row.names = FALSE)

