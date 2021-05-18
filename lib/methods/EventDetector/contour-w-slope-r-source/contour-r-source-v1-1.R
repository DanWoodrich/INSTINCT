libraries<-c("imager","pracma")
librariesToLoad<-c("imager","pracma")
nameSpaceFxns<-c()

#V1-0: this mainly uses the contour algorithm from pracma for detection. Right now, it takes parameters that attempt to 
#weed out detections without 

EventDetectoR<-function(soundFile=NULL,spectrogram=NULL,dataMini,ParamArgs){
  
  dBadd<- as.numeric(ParamArgs[3])
  highFreq<-as.numeric(ParamArgs[4])
  lowFreq<-as.numeric(ParamArgs[5])
  maxDur = as.numeric(ParamArgs[6])
  minDur = as.numeric(ParamArgs[7])
  minFreq = as.numeric(ParamArgs[8])
  noiseThresh<-as.numeric(ParamArgs[10])
  Overlap<-as.numeric(ParamArgs[13]) #can be handled in wrapper 
  #target sample rate at 14, handled in wrapper 
  windowLength<-as.numeric(ParamArgs[15]) #can be handled in wrapper 
  
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
  
  #average psd first, then log transform to dB scale!
  
  
  #View(spectrogram$S) #psd unit? 
  
  #hist(as.numeric(spectrogram$S[,1]))
  #hist(10*log(as.numeric(spectrogram$S[,1])))
  
  #absolute value (throw out phase)
  spectrogram$S = abs(spectrogram$S) 
  
  #hist(as.numeric(spectrogram$S[,1]))
  #hist(10*log(as.numeric(spectrogram$S[,1])))
  
  #set power relative to max value for whole window
  #spectrogram$S = spectrogram$S/max(spectrogram$S)
  
  #hist(as.numeric(spectrogram$S[,1]))
  #hist(log(as.numeric(spectrogram$S[,1])))
  
  
  #log transform it 
  #View(spectrogram$S)
  #hist(spectrogram$S[,1])
  #hist(10*log(spectrogram$S[,1]))
  

  freqrange<-highFreq-lowFreq
  bandWidth<-floor(freqrange/numBands)
  bandOverlapNum<-round(1/bandOvlp) #this is pretty bad, need to change. Not really doing what it should be. 
  highStop=highFreq-bandWidth
  
  bands<-seq.int(lowFreq,highStop,by=(bandWidth/bandOverlapNum))
  bands<-floor(bands)
  
  tempogram<-spectrogram
  
  Dets<-foreach(r=1:length(bands)) %do% {

  
  #band limit spectrogram
    tempogram$S<-spectrogram$S[which(spectrogram$f>=bands[r]&spectrogram$f<(bands[r]+bandWidth)),]
    
    tempogram$f<-spectrogram$f[which(spectrogram$f>=bands[r]&spectrogram$f<(bands[r]+bandWidth))]
  
  #calculate time adjustment for spectrogram step
  tAdjust=max(roldur)/length(tempogram$t)

  #time2<-Sys.time()
  vec<-apply(tempogram$S,2,function(x) 10*log(mean(x)))
  
  #plot(vec,type="b")
  
  #declare matrix for calculating noise value: 
  rowz<-ceiling(noiseWinLength/noiseHopLength)
  
  
  #calculate moving function for each sound file (coarse, but allows for repeatability if spliced up differently)
  rowIt<-1
  steps<-seq(0,max(roldur),by=noiseHopLength)
  noise<-matrix(ncol=length(steps),nrow=rowz)
  for(f in 1:(length(steps)-rowz)){
    
    noiseVal<-quantile(vec[which(tempogram$t>=steps[f]&tempogram$t<steps[f+rowz])],noiseThresh, names = FALSE)
    noise[rowIt,f:(f+rowz-1)]<-rep(noiseVal,rowz)
    #abline(z=noiseThresh)
    if(rowIt==rowz){
      rowIt<-0
    }
    rowIt<-rowIt+1
  }
  noise<-noise[,1:(length(steps)-1)]
  noise<-apply(noise,2,function(x) mean(x,na.rm=TRUE))
  #plot(noise)
  noiseComp<-tempogram$t
  for(f in 1:(length(steps)-1)){
    noiseComp[tempogram$t>=steps[f]&tempogram$t<steps[f+1]]<-noise[f]
  }

  noiseComp<-noiseComp+dBadd
  
  vec[vec<noiseComp]<-0
  
  #plot(vec,type="b")
  #abline(z=noiseThresh)
  
  MaM<-MinimaAndMaxima(vec)
  
  if(any(MaM==2)){
    Detections<-AssignDetections(MaM,tempogram$t)
  }else{
    Detections<-NULL
  }
  
  #combine detections which end/start at same time slice
  Detections<-Detections[!(duplicated(Detections) | duplicated(Detections, fromLast=TRUE))]
  if(!is.null(Detections)){
  Detections<-matrix(Detections, byrow = TRUE,ncol = 2)
  
  Detections<-cbind(Detections,bands[r],bands[r]+bandWidth)
  }
  
  return(Detections)
  
  }
  
  Detections<-do.call('rbind',Dets)
  
  #time2<-Sys.time()
  
  #visualize detections
  step=30
  len = length(soundFile@left)/soundFile@samp.rate
  tAdjust=len/length(tempogram$t)
  
  
  #topcol<-max(Detections[,3])-lowFreq+1
  for(f in seq(to=len,from=0,by=step)){
  #just show certain time range
  tempogram<-spectrogram
  start=f
  if(start==len){
    break
  }
  end = f+step
  if(end>len){
    end=len
  }
  tempogram$S<-spectrogram$S[,(start/tAdjust):(end/tAdjust)]
  tempogram$t<-spectrogram$t[(start/tAdjust):(end/tAdjust)]
  plot(tempogram)
  
  P = tempogram$S
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
  
  TileAxisSize=1024
  IsoblurSigma=1
  ImgThresh="85%"
  image1<-as.cimg(as.numeric(t(P)),x=dim(P)[2],y=dim(P)[1])
  #image1<-resize(image1,size_x=TileAxisSize,size_y=TileAxisSize) #this distorts the image, may or may not matter. Allows for standardized 
  #contour size thresholding
  

  image1<-as.cimg(image1[,dim(image1)[2]:1,,])
  

  image1<-isoblur(image1,sigma=IsoblurSigma)
  
  image1<-threshold(image1,ImgThresh) 
  
  plot(image1)
  
  
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
  
  pixThresh<-25
  pixThreshDiv<-2
  DesiredSlope<-"Upsweep"
  
  if(DesiredSlope=="Upsweep"){
    slopeTest<-1
  }else{
    slopeTest<-0
  }
  
  if(DesiredSlope=="Stacked"){ #ignore slope for this option, in the future, can look at degrees of slope vs flat
    cont2<-cont[which(size>pixThresh)]
  }else{
    cont2<-cont[which(size>pixThresh|(slope==slopeTest&size>pixThresh/pixThreshDiv))]
  }

  #plot(image1)
  #purrr::walk(cont2,function(v) lines(v$x,v$y,col="red",lwd=4))
  
  Detections<-foreach(i=1:length(cont2)) %do% {
    x1=min(cont2[[i]]$x)
    x2=max(cont2[[i]]$x)
    y1=min(cont2[[i]]$y)
    y2=max(cont2[[i]]$y)
    return(c(x1,x2,y1,y2))
  }
  
  Detections<-do.call("rbind",Detections)
  
  return(Detections)

}
