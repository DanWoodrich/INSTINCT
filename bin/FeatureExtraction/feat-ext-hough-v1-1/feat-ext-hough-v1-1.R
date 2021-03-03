#set these variables in all containers:
MethodName<-"feat-ext-hough-v1-1"

#attempt at recreating the feature extraction protocol of INSTINCT.
library(foreach) 
library(doParallel) #need
library(tuneR) #need
library(signal) #need
library(imager) #need
library(oce) #need

hough_line_test <- function(im,ntheta=100,data.frame=FALSE,shift=TRUE)
{
  theta.bins <- seq(0,2*pi,length=ntheta)
  hl <- hough_line_px(im,theta.bins)
  maxd <- sum(dim(im)[1:2]^2) %>% sqrt %>% ceiling %>% { .*2 }
  rho.bins <- seq(-maxd/2,maxd/2,l=maxd)
  theta <- theta.bins[Yc(hl)]
  rho <- rho.bins[Xc(hl)]
  rho <- rho+cos(theta)+sin(theta)
  data.frame(theta=theta,rho=rho,score=as.vector(hl))
}

std.error<- function(x){
 sd(x)/sqrt(length(x))
}

freqstat.normalize<- function(freqstat,lowFreq,highFreq){
  newstat<-((freqstat-lowFreq)/(highFreq-lowFreq))
  return(newstat)
}

lastFeature<-function(a,b,wl){
  
  ## get frequency windows length for smoothing
  step <- a/(wl/2)/1000
  
  fsmooth = 0.1
  fsmooth <- fsmooth/step
  
  ## number of samples
  n <- nrow(b)
  
  ## smoothing parameter
  FWL <- fsmooth - 1
  
  ## smooth 
  zx <- apply(as.matrix(1:(n - FWL)), 1, function(y) sum(b[y:(y + FWL), 2]))
  zf <- seq(min(b[,1]), max(b[,1]), length.out = length(zx))
  
  ## make minimum amplitude 0
  zx <- zx - min(zx)
  zx[zx < 0] <- 0
  
  ## normalize amplitude from 0 to 1
  zx <- zx/max(zx)
  
  # return low and high freq
  return(zf[which.max(zx)] + (step / 2))
}

read_ini = function(fn) {
  blank = "^\\s*$"
  header = "^\\[(.*)\\]$"
  key_value = "^.*=.*$"
  
  extract = function(regexp, x) regmatches(x, regexec(regexp, x))[[1]][2]
  lines = readLines(fn)
  ini = list()
  for (l in lines) {
    if (grepl(blank, l)) next
    if (grepl(header, l)) {
      section = extract(header, l)
      ini[[section]] = list()
    }
    if (grepl(key_value, l)) {
      kv = strsplit(l, "\\s*=\\s*")[[1]]
      ini[[section]][[kv[1]]] = kv[2]
    }
  }
  ini
}

startLocalPar<-function(num_cores,...){
  
  cluz <<- parallel::makeCluster(num_cores)
  registerDoParallel(cluz)
  
  clusterExport(cluz, c(...))
  
}

FeatureExtracteR<-function(z,featList,StartNameL,EndNameL,StartFileL,EndFileL,StartFileDur,coreF,TileAxisSize){
    

    #store reused calculations to avoid indexing 
    Start<-featList[1]
    End<-  featList[2]
    Low<-featList[3]
    High<-featList[4]
    
    if(StartNameL==EndNameL){
      Wav<-extractWave(StartFileL, from = Start, to = End,interact = FALSE, xunit = "time")
    }else{
      SoundList <- vector(mode = "list", length = 2)
      SoundList[[1]]<-extractWave(StartFileL, from = Start, to = StartFileDur,interact = FALSE, xunit = "time")
      SoundList[[2]]<-extractWave(EndFileL, from = 0, to = End,interact = FALSE, xunit = "time")
      Wav<-do.call(bind, SoundList)
    }
    
    fs<-Wav@samp.rate
    samples<-length(Wav@left)
    snd = Wav@left - mean(Wav@left)
    
    if(SpectrogramFunc=="specgram"){
      # create spectrogram
      spectrogram = specgram(x = snd,
                             Fs = fs,
                             window=WindowLength,
                             overlap=Overlap
      )
      
      
      P = abs(spectrogram$S)
      
      P<-P[(High*2):(Low*2),]
      
      P = P/max(P)
      
      
      if(ChannelNormalize=="y"&length(spectrogram$t)>1){
        #this normalizes by the row (which could be good for broadband signal like GS), could also try column for LM. Needs more testing
        for(k in 1:nrow(P)){
          ##P[k,]<-P[k,]-mean(P[k,])
          P[k,]<-P[k,]-median(P[k,])
        }
        
        
      }else{
        
        P = 15*log10(P)
      }
      
    }else if(SpectrogramFunc=="spectro"){
      spectrogram<-spectro(snd,f=fs,wl=WindowLength,zp=ZeroPadding,ovlp=Overlap,plot=F)
      
      
      P = abs(spectrogram$amp)
      
      P<-P[(High*2):(Low*2),]
      
      P = P/max(P)
      P = 15*log10(P)

    }

    #plot(spectrogram)
    #plot(spec2)
    
    #test
    image1<-as.cimg(as.numeric(t(P)),x=dim(P)[2],y=dim(P)[1])
    image1<-resize(image1,size_x=TileAxisSize,size_y=TileAxisSize)
    
    image1<-isoblur(image1,sigma=IsoblurSigma)
    
    image1<-threshold(image1,ImgThresh) 
    
    
    #image1<-clean(image1,ImgNoiseRedPower) %>% imager::fill(ImgFillPower) 
    
    #Black border so edge islands are respected 
    image1[1,1:TileAxisSize,1,1]<-FALSE
    image1[1:TileAxisSize,1,1,1]<-FALSE
    image1[TileAxisSize,1:TileAxisSize,1,1]<-FALSE #get rid of side border artifact 
    image1[1:TileAxisSize,TileAxisSize,1,1]<-FALSE 
    
    #plot(image1)
    
    
    #jpeg(paste(tempFolder,"/",zmod,"look.jpg",sep=""),quality=100,width=TileAxisSize,height=TileAxisSize)
    
    #par(#ann = FALSE,
    #  mai = c(0,0,0,0),
    #  mgp = c(0, 0, 0),
    #  oma = c(0,0,0,0),
    #  omd = c(0,1,0,1),
    #  omi = c(0,0,0,0),
    #  xaxs = 'i',
    #  xaxt = 'n',
    #  xpd = FALSE,
    #  yaxs = 'i',
    #  yaxt = 'n')
    #plot(image1)
    #dev.off()
    
    #if a complete black or white image, do not bother to extract features
    if(length(unique(image1))!=1){
      
      #hough lines
      test9<-hough_line(image1,data.frame = TRUE)
      
      test8<-test9
      #modify these values for bestline features
      test9<- cbind(test9,(-(cos(test9$theta)/sin(test9$theta))),test9$rho/sin(test9$theta))
      test8<- cbind(test8,(-(cos(test8$theta)/sin(test8$theta))),test8$rho/sin(test8$theta))
      test9[which(is.infinite(test9[,4])&test9[,4]<0),4]<-min(c(-2.76824e+18,min(test9[-which(is.infinite(test9[,4])),4])))
      test9[which(is.infinite(test9[,4])&test9[,4]>0),4]<-max(c(2.76824e+18,max(test9[-which(is.infinite(test9[,4])),4])))
      test9[which(is.infinite(test9[,5])&test9[,5]<0),5]<-min(c(-2.76824e+18,min(test9[-which(is.infinite(test9[,5])),5])))
      test9[which(is.infinite(test9[,5])&test9[,5]>0),5]<-max(c(2.76824e+18,max(test9[-which(is.infinite(test9[,5])),5])))
      
      Bestline<-test9[which.max(test9$score),]
      Bestlines<-test8[which(test8$score>=max(test8$score)*.7),]
      
      
      #for(n in 1:nrow(Bestlines)){
      #  nfline(Bestlines[n,1],Bestlines[n,2],col=rgb(0, 1, 0,0.1),lwd=3)
      #}
      
      # "BestRho Hough","BestTheta Hough","BestSlope Hough","BestB Hough","MedRho Hough","MedTheta Hough","MedSlope Hough","MedB Hough",
      featList[5]<-Bestline[1]#BestTheta Hough
      featList[6]<-Bestline[2]#BestRho Hough
      featList[7]<-Bestline[3]#bestScore
      featList[8]<-Bestline[4]#bestSlopeHough
      featList[9]<-Bestline[5]#bestBHough
      
      featListID<-10 #find median, mean, and variance of the following features: 
      for(c in 1:5){
        featList[featListID]<-median(Bestlines[,c],na.rm=TRUE)
        featList[featListID]<-if(is.na(as.numeric(featList[featListID]))){99999}else{featList[featListID]}
        featList[featListID]<-if(!is.finite(as.numeric(featList[featListID]))){99999}else{featList[featListID]}
        featList[featListID+1]<-mean(Bestlines[,c],na.rm=TRUE)
        featList[featListID+1]<-if(is.na(as.numeric(featList[featListID+1]))){99999}else{featList[featListID+1]}
        featList[featListID+1]<-if(!is.finite(as.numeric(featList[featListID+1]))){99999}else{featList[featListID+1]}
        featList[featListID+2]<-sd(Bestlines[,c],na.rm=TRUE)
        featList[featListID+2]<-if(is.na(as.numeric(featList[featListID+2]))){99999}else{featList[featListID+2]}
        featList[featListID+2]<-if(!is.finite(as.numeric(featList[featListID+2]))){99999}else{featList[featListID+2]}
        featListID<-featListID+3
      }
      
      featList[25]<-nrow(Bestlines)#numGoodlines
      
      
     
    }else{
        featList[5:25]<-99999
      }
  

    return(as.numeric(featList))

  }


#windows test values
#DataPath <-"C:/instinct_dt/Data/SoundFiles"        #docker volume
#FGpath <- "C:/instinct_dt/Cache/c3dfe4b64950e42d7f6f6c546446b5e8e4f73e42/" #docker volume
#DETpath <- "C:/instinct_dt/Cache/c3dfe4b64950e42d7f6f6c546446b5e8e4f73e42/8a757bbec32b5b6a56bc98ac2748d2df64deaa0f" #docker volume
#ParamPath <- "C:/INSTINCT_HPC/etc/FG_ED_FE"
#resultPath<-"C:/instinct_dt/Cache/c3dfe4b64950e42d7f6f6c546446b5e8e4f73e42/8a757bbec32b5b6a56bc98ac2748d2df64deaa0f/ab6779b5e7bbcb2d4e97f8addd1f34e5c8ef6463/test"
#tmpPath<-"C:/instinct_dt/tmp"
#ReadFile1<-'FileGroupFormat.csv.gz'
#ReadFile2<-'EventDetector.csv.gz'

args<-commandArgs(trailingOnly = TRUE)

FGpath <- args[1]
DETpath <- args[2]
DataPath <- args[3]
resultPath <- args[4] 

splitNum<-args[5] 
  
ReadFile2<-paste('DETsplitForFE',splitNum,'.csv.gz',sep="")

FG<-read.csv(paste(FGpath,'FileGroupFormat.csv.gz',sep="/"))
colnames(FG)[3]<-"RealTime"

tmpPath<-paste(resultPath,splitNum,sep="/")
dir.create(tmpPath)

data<-read.csv(paste(DETpath,ReadFile2,sep="/"))


#add buffer times: pseudocode:
#calcute duration of each call, for those that are too short, subtract duration from buffer size and add difference/2 to each call
#subset to find calls with negative start times and end times over the file duration
#for each reassign if there is an eligible previous/next file in file group
#otherwise, 

crs<- as.integer(args[6])

ChannelNormalize<-as.character(args[7])
ImgThresh<-paste(as.integer(args[8]),"%",sep="")
IsoblurSigma<-as.integer(args[9])
Overlap<-as.integer(args[10])
SpectrogramFunc<-args[11]
TileAxisSize<-as.integer(args[12])
TMB<- as.integer(args[13])
WindowLength<-as.integer(args[14])
ZeroPadding<-as.integer(args[15])



#Merge FG and data so data has full paths 
data<- merge(data, FG, by.x = "StartFile", by.y = "FileName")


data$calDur<-data$EndTime-data$StartTime
data$calDur[which(data$calDur<0)]<-data$calDur[which(data$calDur<0)]+data$Duration[which(data$calDur<0)]
data$StartTime[which(TMB-data$calDur>0)]<-data$StartTime[which(TMB-data$calDur>0)]-((TMB-data$calDur[which(TMB-data$calDur>0)])/2)
data$EndTime[which(TMB-data$calDur>0)]<-data$EndTime[which(TMB-data$calDur>0)]+((TMB-data$calDur[which(TMB-data$calDur>0)])/2)

if(any(data$StartTime<0)){
  for(i in which(data$StartTime<0)){
    difftime<-data[i,'DiffTime']
    earliestDifftime<-min(which(FG$DiffTime %in% difftime))
    fileName<-as.character(data[i,'StartFile'])
    fileNamePos<-which(FG$FileName==fileName)
    if(earliestDifftime<fileNamePos){
      data[i,'StartFile']<-FG$FileName[fileNamePos-1]
      data[i,'Duration']<-FG$Duration[fileNamePos-1]
      data[i,'StartTime']<-data[i,'Duration']+data[i,'StartTime']
    }else{
      #assume since its at the start of a file the end time can handle it if added on the other end. This behavior keeps 
      #TMB consistent and moves window to accomodate- does not attempt to find a valid file not specified in FG to 
      #keep effort assumptions consistent
      data[i,'EndTime']<-TMB
      data[i,'StartTime']<-0
    }
  }
}

if(any(data$EndTime>data$Duration)){
  for(i in which(data$EndTime>data$Duration)){
    difftime<-data[i,'DiffTime']
    latestDifftime<-max(which(FG$DiffTime %in% difftime))
    fileName<-as.character(data[i,'StartFile'])
    fileNamePos<-which(FG$FileName==fileName)
    if(latestDifftime>fileNamePos){
      data[i,'EndFile']<-FG$FileName[fileNamePos+1]
      data[i,'Duration']<-FG$Duration[fileNamePos+1]
      data[i,'EndTime']<-data[i,'EndTime']-data[i,'Duration']
    }else{
      #assume since its at the start of a file the end time can handle it if added on the other end. This behavior keeps 
      #TMB consistent and moves window to accomodate- does not attempt to find a valid file not specified in FG to 
      #keep effort assumptions consistent
      data[i,'EndTime']<-FG$Duration[i]
      data[i,'StartTime']<-FG$Duration[i]-TMB
    }
  }
}

#correct start and end times

#recalculate
data$calDur<-data$EndTime-data$StartTime
data$calDur[which(data$calDur<0)]<-data$calDur[which(data$calDur<0)]+data$Duration[which(data$calDur<0)]

#drop unneeded columns
data<-data[,c("StartTime","EndTime","LowFreq","HighFreq","StartFile","EndFile","FullPath","Duration")]

#sort by start time and file 
data<-data[order(data$StartFile,data$StartTime),]

if(crs>detectCores()){
  crs<-detectCores()
}

#go through data sequentially. 
itemz<-nrow(data)

#turn off parallel if low amounts of data 
if(itemz<=crs){
  crs<-1
}

chunkz_size<-ceiling(itemz/crs)
chunkzAssign<-rep(1:crs,each=chunkz_size)

realcrs<-unique(chunkzAssign)
#in case of rounding issues: 
crs<-length(unique(chunkzAssign))

chunkzAssign<-chunkzAssign[1:itemz]

#save chunks to temp files 
for(n in 1:crs){
  write.csv(data[chunkzAssign==n,],gzfile(paste(tmpPath,"/chunk",n,".csv.gz",sep="")),row.names=FALSE)
}
#divide up effort into consecutive chunks 

startLocalPar(crs,"crs","tmpPath","lastFeature","freqstat.normalize","SpectrogramFunc",'WindowLength',"Overlap","freqstat.normalize","lastFeature","std.error","imagep","specgram")

out2<-foreach(f=1:crs,.packages=c("tuneR","imager","doParallel")) %dopar% {
  dataIn<-read.csv(paste(tmpPath,"/chunk",f,".csv.gz",sep=""))

  #attempt to use IO/readwav more effeciently by reusing wav objects between iterations
  StartNameL<-"NULL1"
  StartFileL<-"NULL1"
  EndNameL<-"NULL2"
  EndFileL<-"NULL2"
  
  
  startTimes<-c()
  endTimes<-c()
  
  
  out1<-foreach(r=1:nrow(dataIn)) %do% {
    #check if start file is correct file, try to use loaded end file if it is the new start file
    if(StartNameL!=dataIn$StartFile[r]){
      if(StartNameL==EndNameL){
        StartFileL<-EndFileL
      }else{
        StartNameL<-as.character(dataIn$StartFile[r])
        StartFileL<-readWave(paste(DataPath,dataIn$StartFile[r],sep="/"))
      }
    }
    if(EndNameL!=dataIn$EndFile[r]){
      if(EndNameL==StartNameL){
        EndFileL<-StartFileL
      }else{
        EndNameL<-as.character(dataIn$EndFile[r])
        EndFileL<-readWave(paste(DataPath,dataIn$EndFile[r],sep="/"))
      }
    }
    
    #feature vector can be numeric, pass start/end file as seperate vars. After going through sequentially attach it to 
    #file name data again to export. 
    
    featList<-as.numeric(dataIn[r,1:4])
    StartFileDur<-dataIn$Duration[r]
    startTimes<-c(startTimes,Sys.time())
    featList<-FeatureExtracteR(r,featList,StartNameL,EndNameL,StartFileL,EndFileL,StartFileDur,f,TileAxisSize)
    endTimes<-c(endTimes,Sys.time())
    

    featList
  }
  
  out1<-do.call("rbind",out1)
  return(out1)
  
}

parallel::stopCluster(cluz)

outName<-paste("DETwFeaturesSplit",splitNum,".csv.gz",sep="")
out2<-do.call("rbind",out2)

out2<-data.frame(out2)

colnames(out2)<-c("StartTime","EndTime","LowFreq","HighFreq",
                  "BestTheta Hough","BestRho Hough","BestScore Hough","BestSlope Hough","BestB Hough","MedTheta Hough","MeanTheta Hough","sdTheta Hough",
                  "MedRho Hough","MeanRho Hough","sdRho Hough","MedScore Hough","MeanScore Hough","sdScore Hough","MedSlope Hough","MeanSlope Hough",
                  "sdSlope Hough","MedB Hough","MeanB Hough","sdB Hough","num Goodlines")

out2<-cbind(out2[,c("StartTime","EndTime","LowFreq","HighFreq")],data[,c("StartFile","EndFile")],out2[,5:ncol(out2)])

for(n in 1:crs){
  file.remove(paste(tmpPath,"/chunk",n,".csv.gz",sep=""))
}

unlink(tmpPath,recursive=TRUE, force = TRUE)

write.csv(out2,gzfile(paste(resultPath,outName,sep="/")),row.names = FALSE)


