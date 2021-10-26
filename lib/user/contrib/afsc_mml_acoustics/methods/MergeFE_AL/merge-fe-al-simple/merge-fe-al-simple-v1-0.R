#Load in Labels and features for all detections. These data are all considered ground truth, and will be used to create a model. 

MethodName<-"merge-fe-al-simple-v1-0"

#test
#DETwFEpath <- "C:/instinct_dt/Cache/2bf717aef81044ab2e655e4351d4342cc08de10a/8a757bbec32b5b6a56bc98ac2748d2df64deaa0f/0eb9f6fb7a54dd3ac00c2725cc728a1325b5facd/"
#DETwALpath <- "C:/instinct_dt/Cache/2bf717aef81044ab2e655e4351d4342cc08de10a/8a757bbec32b5b6a56bc98ac2748d2df64deaa0f/2cc09ef5daa22a410caf96a2b15d10537bd1a4b5/"
#resultPath<- "C:/instinct_dt/Cache/2bf717aef81044ab2e655e4351d4342cc08de10a/8a757bbec32b5b6a56bc98ac2748d2df64deaa0f/2cc09ef5daa22a410caf96a2b15d10537bd1a4b5/c50f93ed982bc9fc3da1074b5c4db3ffac3420a8"

args<-commandArgs(trailingOnly = TRUE)

DETwFEpath <- args[1]
DETwALpath <- args[2]
resultPath <- args[3]

DETwFE<- read.csv(paste(DETwFEpath,'DETx.csv.gz',sep=""))
DETwAL<- read.csv(paste(DETwALpath,'DETx.csv.gz',sep=""))
#merge the files. Only keep the labels. 

DETwFE$SignalCode<-'out'

samecols<-c("StartTime","EndTime","LowFreq","HighFreq","StartFile","EndFile","SignalCode")

CombData<-merge(DETwFE,DETwAL,by=samecols,all.x=TRUE)

CombData$SignalCode<-NULL
outName<-'DETx.csv.gz'

write.csv(CombData,gzfile(paste(resultPath,outName,sep="/")),row.names = FALSE)


