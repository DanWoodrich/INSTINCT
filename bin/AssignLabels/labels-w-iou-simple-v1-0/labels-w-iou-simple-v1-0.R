#apply labels to detector outputs
#use IoU criteria: area of overlap over area of union 

#set these variables in all containers:
MethodID<-"labels-w-iou-simple-v1-0"

#should be same container method that runs on probabalistic outputs when a cutoff is provided. 

#test folder
#FGpath<-"C:/Apps/INSTINCT/Cache/2e77bc96796a/"
#GTpath<-"C:/Apps/INSTINCT/Cache/2e77bc96796a/50ae7a/"
#DETpath<-"C:/Apps/INSTINCT/Cache/2e77bc96796a/af5c26/3531e3/"
#resultPath<-"C:/Apps/INSTINCT/Cache/2e77bc96796a/af5c26/3531e3/8bbfbd"
#IoUThresh<-0.15
#SignalCode="LM"

args<-commandArgs(trailingOnly = TRUE)

#docker values
FGpath <- args[1]
GTpath <- args[2]
DETpath <- args[3]
resultPath <- args[4]
IoUThresh<-args[5]

GTdata<-read.csv(paste(GTpath,"GTFormat.csv.gz",sep="/"))
FGdata<-read.csv(paste(FGpath,"FileGroupFormat.csv.gz",sep="/"))

outDataAll<-read.csv(paste(DETpath,"DETx.csv.gz",sep="/"))

#retain probs
if("probs" %in% colnames(outDataAll)){
  outDataAll<-outDataAll[,c("StartTime","EndTime","LowFreq","HighFreq","StartFile","EndFile","probs")]
  mergeProbsBack =TRUE
}else{
  mergeProbsBack=FALSE
}

#extract necessary info
outData<-outDataAll[,c("StartTime","EndTime","LowFreq","HighFreq","StartFile","EndFile")]

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

GTlong$iou<-0
outLong$iou<-0

GTlong$Dur<-GTlong$EndTime-GTlong$StartTime

#order datasets: 
GTlong<-GTlong[order(GTlong$StartTime),]
outLong<-outLong[order(outLong$StartTime),]


#calculate iou GT

for(i in 1:nrow(GTlong)){
  klow<-min(which((GTlong$StartTime[i]-(5*GTlong$Dur[i]))<outLong$EndTime))
  khigh<-max(which((GTlong$EndTime[i]+(5*GTlong$Dur[i]))>outLong$StartTime))
  
  k=klow
  while(GTlong$iou[i]<IoUThresh&k<=khigh){
    #test for intersection
    if((((GTlong$StartTime[i]<outLong$EndTime[k] & GTlong$StartTime[i]>=outLong$StartTime[k])|+
       (GTlong$EndTime[i]>outLong$StartTime[k] & GTlong$StartTime[i]<outLong$StartTime[k]))|+
       (GTlong$EndTime[i]>=outLong$EndTime[k] & GTlong$StartTime[i]<=outLong$StartTime[k])) &+ 
       ((GTlong$LowFreq[i]<outLong$HighFreq[k] & GTlong$LowFreq[i]>=outLong$LowFreq[k])|+
        (GTlong$HighFreq[i]>outLong$LowFreq[k] & GTlong$LowFreq[i]<outLong$LowFreq[k])) |+
        (GTlong$HighFreq[i]>=outLong$HighFreq[k] & GTlong$LowFreq[i]<=outLong$EndTime[k]))
        {

      #test for IoU
      #x1,y1,x2,y2
      box1<-c(GTlong$StartTime[i],GTlong$LowFreq[i],GTlong$EndTime[i],GTlong$HighFreq[i])
      box2<-c(outLong$StartTime[k],outLong$LowFreq[k],outLong$EndTime[k],outLong$HighFreq[k])
      
      intBox<-c(max(box1[1],box2[1]),max(box1[2],box2[2]),min(box1[3],box2[3]),min(box1[4],box2[4]))
      
      intArea = abs(intBox[3]-intBox[1]) * abs(intBox[4]-intBox[2])
      
      box1Area = abs(box1[3] - box1[1]) * abs(box1[4] - box1[2])
      box2Area = abs(box2[3] - box2[1]) * abs(box2[4] - box2[2])
      
      totArea = box1Area + box2Area - intArea
      
      iou = intArea / totArea

      if(GTlong$iou[i]<iou){
        GTlong$iou[i]<-iou
      }
      if(outLong$iou[k]<iou){
        outLong$iou[k]<-iou
      }
      
      #plot(min(box1[1],box2[1]):(min(box1[1],box2[1])+15),min(box1[2],box2[2]):(min(box1[2],box2[2])+15))
      #rect(box1[1],box1[2],box1[3],box1[4])
      #Sys.sleep(1)
      #rect(box2[1],box2[2],box2[3],box2[4])
      #Sys.sleep(1)
      #rect(intBox[1],intBox[2],intBox[3],intBox[4],col="red")
      #text(min(box1[1],box2[1])+10,min(box1[2],box2[2])+1,iou)
      #Sys.sleep(2)
      
      k=k+1
      
    }else{
      k=k+1
    }
  }
}

GTlong$StartTime<-GTdata$StartTime
GTlong$EndTime<-GTdata$EndTime

outLong$StartTime<-outData$StartTime
outLong$EndTime<-outData$EndTime

GTlong$label<-ifelse(GTlong$iou<IoUThresh,"FN","TP")
GTlong$label<-as.factor(GTlong$label)
outLong$label<-ifelse(outLong$iou<IoUThresh,"FP","TP")
outLong$label<-as.factor(outLong$label)

GTlong$Dur<-NULL
GTlong$Label<-NULL #I may want this later? But relevant labels past this point should be TP FP FN... 
GTlong$iou<-NULL
outLong$iou<-NULL

outLong$SignalCode<-'out'

#add back in probs if present
if(mergeProbsBack){
  outDataAll<-outDataAll[order(outDataAll$StartFile,outDataAll$StartTime),]
  outLong$probs<-outDataAll$probs
  
  GTlong$probs<-NA
}

#make sure columns match (throw out other metdata not relevant here)
cols <- intersect(colnames(outLong), colnames(GTlong))

CombineTab<-rbind(GTlong[,cols],outLong[,cols])

CombineTab<-CombineTab[order(CombineTab$StartFile,CombineTab$StartTime),]

outName<-paste("DETx.csv.gz",sep="_")

write.csv(CombineTab,gzfile(paste(resultPath,outName,sep="/")),row.names = FALSE)

