#rebin to GT bins, and take GT labels 

#v1-1
#change how FG duration is calculated
#clean up condition to test for IOU (still could do a lot of work here to optimize)
#fix bug in determining if intersection is present.
#

args<-"C:/Apps/INSTINCT/Cache/f1f81de75ed5/ C:/Apps/INSTINCT/Cache/f1f81de75ed5/8237dc/ C:/Apps/INSTINCT/Cache/f1f81de75ed5/8a148e/ C:/Apps/INSTINCT/Cache/f1f81de75ed5/8a148e/8a1e79 n mean labels-w-GT-bins-v1-0"

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
mbehave<-args[5] #y,n, or i (yes, no or ignore)
routine = eval(parse(text=args[6]))#median, max... etc? Can also be user defined fxn

GTdata<-read.csv(paste(GTpath,"DETx.csv.gz",sep="/"))
FGdata<-read.csv(paste(FGpath,"FileGroupFormat.csv.gz",sep="/"))

#convert FG back to old format

#FGdata<-FGdata[which()]

outDataAll<-read.csv(paste(DETpath,"DETx.csv.gz",sep="/"))

GTdata$probs<-NA

GTdata$label[which(GTdata$label=='y')]<-"TP"
GTdata$label[which(GTdata$label=='n')]<-"FP"

if(mbehave=="y"){
  GTdata[which(GTdata$label=='m'),"label"]<-'TP'
}else if(mbehave=="n"){
  GTdata[which(GTdata$label=='m'),"label"]<-'FP'
}else if(mbehave=="i"){
  GTdata<-GTdata[-which(GTdata$label=='m'),]
}

GTdataCopy<-GTdata
GTdataCopy<-GTdataCopy[which(GTdata$label=="TP"),]

GTdata$SignalCode<-'out'

#could do this in parallel, but fast enough for now. Parallel would be a pain since the data is high memory. 

for(n in 1:nrow(GTdata)){
  GTrow=GTdata[n,]
  outDat<-outDataAll[which(outDataAll$StartFile==GTrow[,"StartFile"]),]
  GTdata[n,"probs"]<-routine(outDat[which(outDat$StartTime<GTrow[,"EndTime"]&outDat$StartTime>GTrow[,"StartTime"]),"probs"])
}

GTdata=rbind(GTdata,GTdataCopy)

outName<-"DETx.csv.gz"

GTdata$Type<-NULL

write.csv(GTdata,gzfile(paste(resultPath,outName,sep="/")),row.names = FALSE)


