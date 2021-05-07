library(doParallel)
library(tuneR)
library(signal)
library(foreach)
library(imager)
library(pracma)

#V1-0: this mainly uses the contour algorithm from pracma for detection. Right now, it takes parameters that attempt to 
#weed out FP using slope and island size. Slope is crude, if I like it probably should use hough lines instead. 

EventDetectoR<-function(soundFile=NULL,spectrogram=NULL,dataMini,ParamArgs){
  
  DesiredSlope<-ParamArgs[1]#"Upsweep"
  highFreq<-as.numeric(ParamArgs[2])
  ImgThresh=paste(as.integer(ParamArgs[3]),"%",sep="")#"85%"
  IsoblurSigma=as.numeric(ParamArgs[4])#1
  lowFreq<-as.numeric(ParamArgs[5])
  Overlap<-as.numeric(ParamArgs[6]) #can be handled in wrapper
  pixThresh<-as.numeric(ParamArgs[7])#25
  pixThreshDiv<-as.numeric(ParamArgs[8])#2
  windowLength<-as.numeric(ParamArgs[9]) #can be handled in wrapper
  
  #idea: find some noise value for segments. Normalize from high to low freq probably. (whiten)
  #then, just run the countour() algorithm to return detections. 

  if(!is.null(soundFile)){
    #optional spectrogram calculation. Only do if soundFile is passed
    spectrogram<- specgram(x = soundFile@left,
                           Fs = soundFile@samp.rate,
                           window=windowLength,
                           overlap=Overlap
    )
  }
  
  #use for testing upsweeps: inverts the matrix : 
  #spectrogram$S <- apply(spectrogram$S, 2, rev)
  
  #band limit spectrogram
  spectrogram$S<-spectrogram$S[which(spectrogram$f>=lowFreq&spectrogram$f<highFreq),]
  spectrogram$f<-spectrogram$f[which(spectrogram$f>=lowFreq&spectrogram$f<highFreq)]
  
  P = spectrogram$S
  P = abs(P)
  
  #could replace this with a moving window for more precise calculation
  #or, with a 'smart' window that doesn't average between big swings, and instead averages within these (such as in the case of mooring noise)
  for(k in 1:nrow(P)){
    ##P[k,]<-P[k,]-mean(P[k,])
    P[k,]<-P[k,]-median(P[k,])
  }
  
  #test Phor and Pvert
  #Pvert<-P
  #for(k in 1:ncol(P)){
  #  ##P[k,]<-P[k,]-mean(P[k,])
  #  Pvert[,k]<-P[,k]-median(P[,k])
  #}
  
  #P<-Phor+Pvert
  

  image1<-as.cimg(as.numeric(t(P)),x=dim(P)[2],y=dim(P)[1])
  #image1<-resize(image1,size_x=TileAxisSize,size_y=TileAxisSize) #this distorts the image, may or may not matter. Allows for standardized 
  #contour size thresholding
  
  image1<-as.cimg(image1[,dim(image1)[2]:1,,])

  image1<-isoblur(image1,sigma=IsoblurSigma)
  
  image1<-threshold(image1,ImgThresh) 
  
  #plot(image1)
  
  #image1<-clean(image1,ImgNoiseRedPower) %>% imager::fill(ImgFillPower) 
  
  #Black border so edge islands are respected 
  image1[1,1:dim(image1)[2],1,1]<-FALSE
  image1[1:dim(image1)[1],1,1,1]<-FALSE
  image1[dim(image1)[1],1:dim(image1)[2],1,1]<-FALSE #get rid of side border artifact 
  image1[1:dim(image1)[1],dim(image1)[2],1,1]<-FALSE 
  
  cont<-contours(image1)
  
  size<-vector(mode="numeric", length=length(cont))
  slope<-vector(mode="numeric", length=length(cont))

  for(i in 1:length(cont)){
    size[i]<-polyarea(cont[[i]]$x,cont[[i]]$y)
    islope<-cont[[i]]$y[which.min(cont[[i]]$x)]-cont[[i]]$y[which.max(cont[[i]]$x)]
    slope[i]<-ifelse(islope<0,1,0)
    #hough_line(image1,data.frame = TRUE) could put this in later to try, would be better after knowing who is considered in pixThreshDiv
  }
  
  size<-abs(size)

  if(DesiredSlope=="Upsweep"){
    slopeTest<-1
  }else{
    slopeTest<-0
  }
  
  if(DesiredSlope=="Stacked"){ #ignore slope for this option, in the future, can look at degrees of slope vs flat
    cont2<-cont[which(size>pixThresh)]
  }else{
    cont2<-cont[which(size>pixThresh|(slope==slopeTest & (size>pixThresh/pixThreshDiv)))]
  }

  plot(image1)
  purrr::walk(cont2,function(v) lines(v$x,v$y,col="red",lwd=4))
  
  tAdjust=length(soundFile@left)/soundFile@samp.rate/length(spectrogram$t)
  fAdjust=(highFreq-lowFreq)/length(spectrogram$f)
  
  
  Detections<-foreach(i=1:length(cont2)) %do% {
    x1=min(cont2[[i]]$x)*tAdjust
    x2=max(cont2[[i]]$x)*tAdjust
    y1=highFreq-(max(cont2[[i]]$y)*fAdjust)
    y2=highFreq-(min(cont2[[i]]$y)*fAdjust)
    return(c(x1,x2,y1,y2))
  }
  
  Detections<-do.call("rbind",Detections)
  
  return(Detections)

}
