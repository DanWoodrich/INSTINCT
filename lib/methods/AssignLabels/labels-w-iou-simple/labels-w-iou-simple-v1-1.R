#apply labels to detector outputs
#use IoU criteria: area of overlap over area of union 

#set these variables in all containers:
MethodID<-"labels-w-iou-simple-v1-1"

#v1-1
#change how FG duration is calculated
#clean up condition to test for IOU (still could do a lot of work here to optimize)
#fix bug in determining if intersection is present.
#

args<-"C:/Apps/INSTINCT/Cache/1fecac0f763c/ C:/Apps/INSTINCT/Cache/1fecac0f763c/6c666b/ C:/Apps/INSTINCT/Cache/1fecac0f763c/d3729c/ C:/Apps/INSTINCT/Cache/1fecac0f763c/d3729c/e08d0c  0.25 labels-w-iou-simple-v1-1"

args<-strsplit(args,split=" ")[[1]]

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

GTdata<-read.csv(paste(GTpath,"DETx.csv.gz",sep="/"))
FGdata<-read.csv(paste(FGpath,"FileGroupFormat.csv.gz",sep="/"))

#convert FG back to old format

#FGdata<-FGdata[which()]

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

FGdata$cumsum<- c(0,cumsum(FGdata$Duration)[1:(nrow(FGdata)-1)])


#convert each dataset into time from FG start instead of offset 

GTlong<-GTdata

#these steps are long... could make much faster with a merge. To do
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
  
  GTlongDur<-GTlong$EndTime[i]-GTlong$StartTime[i]
  if(any(outLong$EndTime<(GTlong$StartTime[i]-GTlongDur))){
    klow<-max(which((outLong$EndTime<(GTlong$StartTime[i]-GTlongDur)))) #give this a little buffer to avoid issues with small detections preventing longer det from 
  }else{
    klow=1
  }
  if(any((outLong$StartTime>(GTlong$EndTime[i]+GTlongDur)))){
    khigh<-min(which((outLong$StartTime>(GTlong$EndTime[i]+GTlongDur)))) #fitting the criteria. if wanted to get fancy, could base the buffer length on IOU
  }else{
    khigh=nrow(outLong)
  }
  
  k=klow
  for(k in klow:khigh){
  #while(GTlong$iou[i]<IoUThresh&k<=khigh){
    #test for intersection
    if((((GTlong$StartTime[i]<outLong$EndTime[k] & GTlong$StartTime[i]>=outLong$StartTime[k])| #GTstart is less than det end, and GT start is after or at start of det
       (GTlong$EndTime[i]>outLong$StartTime[k] & GTlong$StartTime[i]<outLong$StartTime[k]))| #GTend is after det end, and GT start is before det start
       (GTlong$EndTime[i]>=outLong$EndTime[k] & GTlong$StartTime[i]<=outLong$StartTime[k])) & #gt end is after or at det end, and gt start is at or before det start
       (((GTlong$LowFreq[i]<outLong$HighFreq[k] & GTlong$LowFreq[i]>=outLong$LowFreq[k])|
        (GTlong$HighFreq[i]>outLong$LowFreq[k] & GTlong$LowFreq[i]<outLong$LowFreq[k])) |
        (GTlong$HighFreq[i]>=outLong$HighFreq[k] & GTlong$LowFreq[i]<=outLong$LowFreq[k])))
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
      
      #give GT the best IOU
      if(GTlong$iou[i]<iou){
        GTlong$iou[i]<-iou
      }
      if(outLong$iou[k]<iou){
        outLong$iou[k]<-iou
      }
      
      #print(i)
      
      #plot(0,xlim=c(min(c(box1[1],box2[1]))-2,max(c(box1[3],box2[3]))+2),ylim=c(0,512),col="white")
      #rect(box1[1],box1[2],box1[3],box1[4],col="green")
      #Sys.sleep(0.25)
      #rect(box2[1],box2[2],box2[3],box2[4],col="gray")
      #Sys.sleep(0.25)
      #rect(intBox[1],intBox[2],intBox[3],intBox[4],col="red")
      #text(mean(c(min(c(box1[1],box2[1]))-2,max(c(box1[3],box2[3]))+2)),256,paste(iou,k))
      #Sys.sleep(0.5)
      
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

