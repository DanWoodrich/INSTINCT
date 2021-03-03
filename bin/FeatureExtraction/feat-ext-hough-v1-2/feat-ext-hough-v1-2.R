#set these variables in all containers:
MethodName<-"feat-ext-hough-v1-2"

#attempt at recreating the feature extraction protocol of INSTINCT.
library(foreach) 
library(doParallel) #need
library(tuneR) #need
library(signal) #need
library(imager) #need
library(oce) #need
library(seewave) #need
library(plotrix) #need
library(autoimage) #unsure if need
library(pracma) #need

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

#'rotating calipers algorithm' from http://dwoll.de/rexrepos/posts/diagBounding.html
getMinBBox <- function(xy,H) {
  stopifnot(is.matrix(xy), is.numeric(xy), nrow(xy) >= 2, ncol(xy) == 2)
  
  ## rotating calipers algorithm using the convex hull
  n    <- length(H)      ## number of hull vertices
  hull <- xy[H, ]        ## hull vertices
  
  ## unit basis vectors for all subspaces spanned by the hull edges
  hDir  <- diff(rbind(hull, hull[1, ])) ## hull vertices are circular
  hLens <- sqrt(rowSums(hDir^2))        ## length of basis vectors
  huDir <- diag(1/hLens) %*% hDir       ## scaled to unit length
  
  ## unit basis vectors for the orthogonal subspaces
  ## rotation by 90 deg -> y' = x, x' = -y
  ouDir <- cbind(-huDir[ , 2], huDir[ , 1])
  
  ## project hull vertices on the subspaces spanned by the hull edges, and on
  ## the subspaces spanned by their orthogonal complements - in subspace coords
  projMat <- rbind(huDir, ouDir) %*% t(hull)
  
  ## range of projections and corresponding width/height of bounding rectangle
  rangeH  <- matrix(numeric(n*2), ncol=2)  ## hull edge
  rangeO  <- matrix(numeric(n*2), ncol=2)  ## orthogonal subspace
  widths  <- numeric(n)
  heights <- numeric(n)
  
  for(i in seq(along=numeric(n))) {
    rangeH[i, ] <- range(projMat[  i, ])
    
    ## the orthogonal subspace is in the 2nd half of the matrix
    rangeO[i, ] <- range(projMat[n+i, ])
    widths[i]   <- abs(diff(rangeH[i, ]))
    heights[i]  <- abs(diff(rangeO[i, ]))
  }
  
  ## extreme projections for min-area rect in subspace coordinates
  ## hull edge leading to minimum-area
  eMin  <- which.min(widths*heights)
  hProj <- rbind(   rangeH[eMin, ], 0)
  oProj <- rbind(0, rangeO[eMin, ])
  
  ## move projections to rectangle corners
  hPts <- sweep(hProj, 1, oProj[ , 1], "+")
  oPts <- sweep(hProj, 1, oProj[ , 2], "+")
  
  ## corners in standard coordinates, rows = x,y, columns = corners
  ## in combined (4x2)-matrix: reverse point order to be usable in polygon()
  ## basis formed by hull edge and orthogonal subspace
  basis <- cbind(huDir[eMin, ], ouDir[eMin, ])
  hCorn <- basis %*% hPts
  oCorn <- basis %*% oPts
  pts   <- t(cbind(hCorn, oCorn[ , c(2, 1)]))
  
  ## angle of longer edge pointing up
  dPts <- diff(pts)
  e    <- dPts[which.max(rowSums(dPts^2)), ] ## one of the longer edges
  eUp  <- e * sign(e[2])       ## rotate upwards 180 deg if necessary
  deg  <- atan2(eUp[2], eUp[1])*180 / pi     ## angle in degrees
  
  return(list(pts=pts, width=widths[eMin], height=heights[eMin], angle=deg))
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
    
    Wav.spec <- spec(Wav,plot=F, PSD=T,wl=WindowLength)
    #Wav.spec <- Wav.spec[which(Wav.spec[,1]<(High/1000)&Wav.spec[,1]>(Low/1000)),]#,ylim=c(specVar$Low.Freq..Hz.[z],specVar$High.Freq..Hz.[z])
    Wav.specprop <- specprop(Wav.spec) #
    Wav.meanspec = meanspec(Wav, plot=F,ovlp=Overlap,wl=WindowLength)#not sure what ovlp parameter does but initially set to 90 #
    #Wav.meanspec.db = meanspec(Wav, plot=F,ovlp=50,dB="max0",wl=128)#not sure what ovlp parameter does but initially set to 90 #,flim=c(specVar$Low.Freq..Hz.[z]/1000,specVar$High.Freq..Hz.[z]/1000)
    if(samples>=64){
      Wav.autoc = autoc(Wav, plot=F,ovlp=Overlap,wl=WindowLength) #
    }else{
      Wav.autoc = 99999
    }
    Wav.dfreq = dfreq(Wav, plot=F, ovlp=Overlap,wl=WindowLength) #tried bandpass argument, limited dfreq to only 2 different values for some reason. Seemed wrong. 
    Startdom<-Wav.dfreq[,2][1]
    Enddom<-Wav.dfreq[,2][length(Wav.dfreq[,2])]
    Mindom <- min(Wav.dfreq, na.rm = TRUE)
    Maxdom <- max(Wav.dfreq, na.rm = TRUE)
    Dfrange <- Maxdom - Mindom
    featList[5] = rugo(Wav@left / max(Wav@left)) #rugosity
    featList[6] = crest(Wav,wl=WindowLength)$C #crest factor
    Wav.env = seewave:::env(Wav, plot=F) 
    featList[7] = th(Wav.env) #temporal entropy
    featList[8] = sh(Wav.spec) #shannon entropy
    featList[9] = roughness(Wav.meanspec[,2]) #spectrum roughness
    #not sure if these aren't crashing or just producing a lot of NAs- if crashing maybe make catch based on # samples. 
    if(samples>=64){ #little catch to hopefully avoid cases where window is too small 
      featList[10] = freqstat.normalize(mean(Wav.autoc[,2], na.rm=T),Low,High) #autoc mean 
      featList[11] = freqstat.normalize(median(Wav.autoc[,2], na.rm=T),Low,High) #autoc.median
      featList[12] = std.error(Wav.autoc[,2], na.rm=T) #autoc se
    }else{
      featList[10] = 99999
      featList[11] = 99999
      featList[12] = 99999
    }
    #
    featList[13] = freqstat.normalize(mean(Wav.dfreq[,2], na.rm=T),Low,High) #dfreq mean
    featList[14] = std.error(Wav.dfreq[,2], na.rm=T) #dfreq se
    featList[15] = freqstat.normalize(Wav.specprop$mean[1],Low,High) #specprop mean
    featList[16] = Wav.specprop$sd[1] #specprop sd
    featList[17] = Wav.specprop$sem[1] #specprop sem
    featList[18] = freqstat.normalize(Wav.specprop$median[1],Low,High) #specprop median
    featList[19] = freqstat.normalize(Wav.specprop$mode[1],Low,High) #specprop mode
    featList[20] = Wav.specprop$Q25[1] # specprop q25
    featList[21] = Wav.specprop$Q75[1] #specprop q75
    featList[22] = Wav.specprop$IQR[1] #specprop IQR
    featList[23] = Wav.specprop$cent[1] #specrop cent
    featList[24] = Wav.specprop$skewness[1] #specprop skewness
    featList[25] = Wav.specprop$kurtosis[1] #specprop kurtosis
    featList[26] = Wav.specprop$sfm[1] #specprop sfm
    featList[27] = Wav.specprop$sh[1] #specprop sh
    featList[28] = Wav.specprop$prec[1] #specprop prec
    featList[29] = M(Wav,wl=WindowLength) #amp env median
    featList[30] = H(Wav,wl=WindowLength) #total entropy
    #warbler params
    if(samples>=64){
      featList[31]<- (sum(sapply(2:length(Wav.dfreq[,2]), function(j) abs(Wav.dfreq[,2][j] - Wav.dfreq[,2][j - 1])))/(Dfrange)) #modinx
    }else{
      featList[31]<-99999
    }
    featList[32]<-freqstat.normalize(Startdom,Low,High) #startdom
    featList[33]<-freqstat.normalize(Enddom,Low,High) #enddom 
    featList[34]<-freqstat.normalize(Mindom,Low,High) #mindom
    featList[35]<-freqstat.normalize(Maxdom,Low,High) #maxdom
    featList[36]<-Dfrange #dfrange
    featList[37]<-((Enddom-Startdom)/(End-Start)) #dfslope
    if(length(lastFeature(fs,Wav.meanspec,WindowLength))!=0){ #address error related to meanspec not showing up- probably related to too few samples. This is patchwork fix for very rare error. 
      featList[38]  <- freqstat.normalize(lastFeature(fs,Wav.meanspec,WindowLength),Low,High)
    }else{
      featList[38]<-99999
    }
    
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
    
    if(length(unique(image1))!=1){
      
      #calculate area chunks x and y 
      chunks<-5
      areaX<- vector("list", length = chunks)
      num<-seq(1,5,1)
      areaX<-lapply(num,function(x) sum(as.matrix(image1)[((x*floor(TileAxisSize/chunks))-(floor(TileAxisSize/chunks)-1)):(x*floor(TileAxisSize/chunks)),1:TileAxisSize]))
      areaX<-unlist(areaX)
      
      areaY<- vector("list", length = chunks)
      areaY<-lapply(num,function(x) sum(as.matrix(image1)[1:TileAxisSize,((x*floor(TileAxisSize/chunks))-(floor(TileAxisSize/chunks)-1)):(x*floor(TileAxisSize/chunks))]))
      areaY<-unlist(areaY)
      
      
      
      #distinguish islands and calculate area
      labelsW<-label(image1)
      labelsW<-as.matrix(labelsW[1:TileAxisSize,1:TileAxisSize])
      threshBool<-as.matrix(image1[1:TileAxisSize,1:TileAxisSize])
      
      labelsW[threshBool]<-labelsW[threshBool]+1000000
      labelsW[which(labelsW<1000000)]<-0
      
      labelsInt<-as.integer(labelsW)
      areaW<-data.frame(table(labelsInt))
      
      areaW<-areaW[which(areaW$labelsInt!=0),]
      areaW<-areaW[order(-areaW[,2]),]
      
      p=1
      
      if(nrow(areaW)>1){
        for(h in 2:nrow(areaW)){
          worthyones<-p
          if((areaW$Freq[h])>(areaW$Freq[h-1]/3.5)&((areaW$Freq[h]/areaW$Freq[1])>0.1)&(h<10)){ #cap at 10 to limit shape calculations 
            p=p+1
          }else{
            break
          }
          
        }
      }else{
        worthyones<-1
      }
      
      #IslIndex<-as.numeric(as.character(areaW$labelsInt))[1:worthyones] slower computation than below
      IslIndex<-as.numeric(levels(areaW$labelsInt[1:worthyones]))[areaW$labelsInt[1:worthyones]]
      areaW<-as.vector(areaW[,2])
      
      ShapeStats<-foreach(i=1:worthyones) %do% {
        Island<-labelsW
        ind<-IslIndex[i]
        Island[which(Island!=ind)]<-0
        
        Island_cont<-contourLines(Island,nlevels=1)
        Island_cont[[1]]$x<-Island_cont[[1]]$x*TileAxisSize
        Island_cont[[1]]$y<-(Island_cont[[1]]$y*TileAxisSize)
        
        #extract shape features: 
        area<-abs(polyarea(Island_cont[[1]]$x,Island_cont[[1]]$y))
        centroid<-poly_center(Island_cont[[1]]$x, Island_cont[[1]]$y)
        xCent<-centroid[1]
        yCent<-centroid[2]
        perimeter<-poly_length(Island_cont[[1]]$x, Island_cont[[1]]$y)
        convexHullind<-chull(Island_cont[[1]]$x,Island_cont[[1]]$y) #index for convex hull, used for a few calculations 
        bb<-getMinBBox(cbind(Island_cont[[1]]$x,Island_cont[[1]]$y),convexHullind)
        Eccentricity<-bb$height/bb$width
        Elongation<-(1-(bb$width/bb$height))
        Circularity<-area/(perimeter^2)
        Rectangularity<-area/(bb$width*bb$height)
        CHper<-poly_length(Island_cont[[1]]$x[convexHullind], Island_cont[[1]]$y[convexHullind])
        CHarea<-polyarea(Island_cont[[1]]$x[convexHullind], Island_cont[[1]]$y[convexHullind])
        Convexity<-CHper/perimeter
        Solidarity<-area/CHarea
        RectAngle<-bb$angle
        
        minfreq<-min(Island_cont[[1]]$y)
        maxfreq<-max(Island_cont[[1]]$y)
        mintime<-min(Island_cont[[1]]$x)
        maxtime<-max(Island_cont[[1]]$x)
        freqrange<-maxfreq-minfreq
        duration<-maxtime-mintime
        
        #calculate slopes: 
        
        shapeSlopeH<-hough_line((as.cimg(Island)>0),data.frame = TRUE) 
        shapeSlopeH<-shapeSlopeH[which.max(shapeSlopeH$score),] 
        shapeSlopeH<- c(shapeSlopeH,(-(cos(shapeSlopeH$theta)/sin(shapeSlopeH$theta))),shapeSlopeH$rho/sin(shapeSlopeH$theta))
        
        #
        
        Theta<-shapeSlopeH[[1]]
        Rho<-shapeSlopeH[[2]]
        Score<-shapeSlopeH[[3]]
        Slope<-shapeSlopeH[[4]]
        b<-shapeSlopeH[[5]]
        
        linep1<-c(0,b)
        linep2<-c((-b/Slope),0)
        
        v1 <- linep1 - linep2
        v2 <- centroid - linep1
        m <- cbind(v1,v2)
        d <- abs(det(m))/sqrt(sum(v1*v1))
        
        if(is.finite(Slope)){
          if((Slope*(xCent)+b)>yCent){
            sign<-1
          }else if((Slope*(xCent)+b)==yCent){
            sign<-0
          }else{
            sign<-(-1)
          }
        }else{
          sign<-NA
        }
        
        Slope2<-atan(Slope)
        
        return(c(Theta,Rho,Score,Slope,Slope2,b,d,sign,area,perimeter,mintime,maxtime,minfreq,maxfreq,freqrange,duration,xCent,yCent,Eccentricity,Elongation,Circularity,Rectangularity,Convexity,Solidarity,RectAngle))
      }
      
      ShapeStats<-do.call('rbind', ShapeStats)
      
      perConcave<-length(ShapeStats[,8][which(ShapeStats[,8]>0)])/length(ShapeStats[,8])
      
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
      
      #calculate centroid of area (could also add centroid of largest area)
      positionsX <- apply(image1[1:TileAxisSize,1:TileAxisSize], 1, function(x) which(x==TRUE))
      positionsY <- apply(image1[1:TileAxisSize,1:TileAxisSize], 2, function(x) which(x==TRUE))
      
      #calculate stats on horizontal and vertical switches
      hpEven<-c()
      hpEven<-c(hpEven,sum(diff(image1[1:TileAxisSize,round(TileAxisSize*0.05)]) == 1) + sum(diff(image1[1:TileAxisSize,round(TileAxisSize*0.5)]) == -1))
      hpEven<-c(hpEven,sum(diff(image1[1:TileAxisSize,round(TileAxisSize*0.2)]) == 1) + sum(diff(image1[1:TileAxisSize,round(TileAxisSize*0.2)]) == -1))
      hpEven<-c(hpEven,sum(diff(image1[1:TileAxisSize,round(TileAxisSize*0.4)]) == 1) + sum(diff(image1[1:TileAxisSize,round(TileAxisSize*0.4)]) == -1))
      hpEven<-c(hpEven,sum(diff(image1[1:TileAxisSize,round(TileAxisSize*0.6)]) == 1) + sum(diff(image1[1:TileAxisSize,round(TileAxisSize*0.6)]) == -1))
      hpEven<-c(hpEven,sum(diff(image1[1:TileAxisSize,round(TileAxisSize*0.8)]) == 1) + sum(diff(image1[1:TileAxisSize,round(TileAxisSize*0.8)]) == -1))
      hpEven<-c(hpEven,sum(diff(image1[1:TileAxisSize,round(TileAxisSize*0.95)]) == 1) + sum(diff(image1[1:TileAxisSize,round(TileAxisSize*0.95)]) == -1))
      vpEven<-c()
      vpEven<-c(vpEven,sum(diff(image1[round(TileAxisSize*0.05),1:TileAxisSize]) == 1) + sum(diff(image1[round(TileAxisSize*0.05),1:TileAxisSize]) == -1))
      vpEven<-c(vpEven,sum(diff(image1[round(TileAxisSize*0.2),1:TileAxisSize]) == 1) + sum(diff(image1[round(TileAxisSize*0.2),1:TileAxisSize]) == -1))
      vpEven<-c(vpEven,sum(diff(image1[round(TileAxisSize*0.4),1:TileAxisSize]) == 1) + sum(diff(image1[round(TileAxisSize*0.4),1:TileAxisSize]) == -1))
      vpEven<-c(vpEven,sum(diff(image1[round(TileAxisSize*0.6),1:TileAxisSize]) == 1) + sum(diff(image1[round(TileAxisSize*0.6),1:TileAxisSize]) == -1))
      vpEven<-c(vpEven,sum(diff(image1[round(TileAxisSize*0.8),1:TileAxisSize]) == 1) + sum(diff(image1[round(TileAxisSize*0.8),1:TileAxisSize]) == -1))
      vpEven<-c(vpEven,sum(diff(image1[round(TileAxisSize*0.95),1:TileAxisSize]) == 1) + sum(diff(image1[round(TileAxisSize*0.95),1:TileAxisSize]) == -1))
      
      #add new variables
      featList[39]<-which.max(areaX) #areaXmaxP
      featList[40]<-max(areaX) #areaXmax
      featList[41]<-max(areaX)/sum(areaX) #areaXdom
      featList[42]<-if(!is.na(sd(areaX))){sd(areaX)}else{99999} #areaXstd
      
      featList[43]<-which.max(areaY) #areaYmaxP
      featList[44]<-max(areaY) #areaYmax
      featList[45]<-max(areaY)/sum(areaY)#areaYdom
      featList[46]<-if(!is.na(sd(areaY))){sd(areaY)}else{99999}#areaYstd
      
      featList[47]<-if(!is.na(sd(areaW))){sd(areaW)}else{99999} #Areaspread
      featList[48]<-max(areaW)#AreaTop
      featList[49]<-max(areaW)/(sum(areaW))#AreaTopDom
      featList[50]<-if(length(areaW)>=3){sum(-sort(-areaW)[1:3])/sum(areaW)}else{1}#AreaTop3Dom
      featList[51]<-length(areaW)#NumShapes
      
      # "BestRho Hough","BestTheta Hough","BestSlope Hough","BestB Hough","MedRho Hough","MedTheta Hough","MedSlope Hough","MedB Hough",
      featList[52]<-Bestline[1]#BestTheta Hough
      featList[53]<-Bestline[2]#BestRho Hough
      featList[54]<-Bestline[3]#bestScore
      featList[55]<-Bestline[4]#bestSlopeHough
      featList[56]<-Bestline[5]#bestBHough
      
      featListID<-57 #find median, mean, and variance of the following features: 
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
      
      featList[72]<-nrow(Bestlines)#numGoodlines
      
      
      featList[73]<-mean(unlist(positionsX,recursive = TRUE),na.rm=TRUE)#xavg
      featList[74]<-mean(unlist(positionsY,recursive = TRUE),na.rm=TRUE)#yavg
      
      featList[75]<-mean(hpEven)#switchesXmean
      featList[76]<-if(!is.na(sd(hpEven))){sd(hpEven)}else{99999}#switchesXse
      featList[77]<-max(hpEven)#switchesXmax
      featList[78]<-min(hpEven)#switchesXmin
      
      featList[79]<-mean(vpEven)#switchesYmean
      featList[80]<-if(!is.na(sd(vpEven))){sd(vpEven)}else{99999}#switchesYse
      featList[81]<-max(vpEven)#switchesYmax
      featList[82]<-min(vpEven)#switchesYmin
      
      
      #all stats: c("sTheta","sThetaSD","sRho","sRhoSD","sScore","sScoreSD","sSlope","sSlopeSD",
      #"atansSlope","atansSlopeSD","sB","sBSD","sD","sDSD","sSign","sSignSD","sArea","sAreaSD","sPerimeter",
      #"sPerimeterSD","sMintime","sMintimeSD","sMaxtime","sMaxtimeSD","sMinfreq","sMinfreqSD","sMaxfreq","sMaxfreqSD",
      #"sFreqrange","sFreqrangeSD","sDuration","sDurationSD","sxCent","sxCentSD","syCent","syCentSD","sEccentricity",
      #"sEccentricitySD","sElongation","sElongationSD","sCircularity","sCircularitySD","sRectangularity","sRectangularitySD",
      #"sConvexity","sConvexitySD","sSolidarity","sSolidaritySD","sRectAngle","sRectAngleSD")
      #plug these in in bulk
      featListID<-83
      for(c in 1:ncol(ShapeStats)){
        featList[featListID]<-mean(ShapeStats[,c],na.rm=TRUE)
        featList[featListID]<-if(is.na(as.numeric(featList[featListID]))){99999}else{featList[featListID]}
        featList[featListID]<-if(!is.finite(as.numeric(featList[featListID]))){99999}else{featList[featListID]}
        featList[featListID+1]<-sd(ShapeStats[,c],na.rm=TRUE)
        featList[featListID+1]<-if(is.na(as.numeric(featList[featListID+1]))){99999}else{featList[featListID+1]}
        featList[featListID+1]<-if(!is.finite(as.numeric(featList[featListID+1]))){99999}else{featList[featListID+1]}
        featListID<-featListID+2
      }
      
      #individual shape features:
      
      featList[133]<-sum(ShapeStats[,7],na.rm=TRUE) #sum of all centroid distances (hopefully weighted towards larger shapes that have better potential for concavity)
      featList[134]<-sum(ShapeStats[,7]*ShapeStats[,8],na.rm=TRUE)
      featList[135]<-mean(ShapeStats[,7],na.rm=TRUE) #other way of comparing centroid distances
      featList[136]<-mean((ShapeStats[,7]*ShapeStats[,8]),na.rm=TRUE) #other way of comparing centroid distances
      featList[137]<-perConcave #% positive distances.  
      featList[138]<-worthyones
      
      for(g in 5:138){
        if(is.na(as.numeric(featList[g]))){
          featList[g]<-99999
        }else if(!is.finite(as.numeric(featList[g]))){
          featList[g]<-99999
        }
      }
      
    }
    
    return(as.numeric(featList))

  }


#windows test values
#DataPath <-"C:/instinct_dt/Data/SoundFiles"        #docker volume
#FGpath <- "C:/instinct_dt/Cache/491ed9da5575918b03e04d73402145b99dd3fec0/" #docker volume
#DETpath <- "C:/instinct_dt/Cache/491ed9da5575918b03e04d73402145b99dd3fec0/8a757bbec32b5b6a56bc98ac2748d2df64deaa0f" #docker volume
#resultPath<-"C:/instinct_dt/Cache/491ed9da5575918b03e04d73402145b99dd3fec0/8a757bbec32b5b6a56bc98ac2748d2df64deaa0f/0eb9f6fb7a54dd3ac00c2725cc728a1325b5facd"
#tmpPath<-"C:/instinct_dt/tmp"
#ReadFile2<-'Detections.csv.gz'

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

#make a column to identify FG ID 
FG$ID<-1:nrow(FG)

#save chunks to temp files 
for(n in 1:crs){
  write.csv(data[chunkzAssign==n,],gzfile(paste(tmpPath,"/chunk",n,".csv.gz",sep="")),row.names=FALSE)
}
#divide up effort into consecutive chunks 

startLocalPar(crs,"crs","tmpPath","lastFeature","freqstat.normalize","SpectrogramFunc",'WindowLength',"Overlap","freqstat.normalize","lastFeature","imagep","specgram","getMinBBox","TileAxisSize","FG")

out2<-foreach(f=1:crs,.packages=c("tuneR","imager","doParallel","seewave","pracma","plotrix")) %dopar% {
  dataIn<-read.csv(paste(tmpPath,"/chunk",f,".csv.gz",sep=""))

  #attempt to use IO/readwav more effeciently by reusing wav objects between iterations
  StartNameL<-"NULL1"
  StartFileL<-"NULL1"
  EndNameL<-"NULL2"
  EndFileL<-"NULL2"
  
  
  #startTimes<-c()
  #endTimes<-c()
  
  
  out1<-foreach(r=1:nrow(dataIn)) %do% {
    #check if start file is correct file, try to use loaded end file if it is the new start file
    if(StartNameL!=dataIn$StartFile[r]){
      if(dataIn$StartFile[r]==EndNameL){
        StartFileL<-EndFileL
        StartNameL<-EndNameL
      }else{
        StartNameL<-as.character(dataIn$StartFile[r])
        StartFileL<-readWave(paste(DataPath,FG$FullPath[which(FG$FileName==dataIn$StartFile[r])],dataIn$StartFile[r],sep=""))
      }
    }
    if(EndNameL!=dataIn$EndFile[r]){
      if(dataIn$EndFile[r]==StartNameL){
        EndFileL<-StartFileL
        EndNameL<-StartNameL
      }else{
        EndNameL<-as.character(dataIn$EndFile[r])
        EndFileL<-readWave(paste(DataPath,FG$FullPath[which(FG$FileName==dataIn$EndFile[r])],dataIn$EndFile[r],sep=""))
      }
    }

    #feature vector can be numeric, pass start/end file as seperate vars. After going through sequentially attach it to 
    #file name data again to export. 
    
    featList<-as.numeric(dataIn[r,1:4])
    StartFileDur<-dataIn$Duration[r]
    #startTimes<-c(startTimes,Sys.time())
    featList<-FeatureExtracteR(r,featList,StartNameL,EndNameL,StartFileL,EndFileL,StartFileDur,f,TileAxisSize)
    #endTimes<-c(endTimes,Sys.time())
    
    featList<-c(featList[1:4],FG[which(FG$FileName==StartNameL),"ID"],FG[which(FG$FileName==EndNameL),"ID"],featList[5:length(featList)]) #this is a test line to see if it fixes bug
    

    featList
  }
  
  out1<-do.call("rbind",out1)
  return(out1)
  
}

parallel::stopCluster(cluz)

outName<-paste("DETwFeaturesSplit",splitNum,".csv.gz",sep="")
out2<-do.call("rbind",out2)

out2<-data.frame(out2)

colnames(out2)<-c("StartTime","EndTime","LowFreq","HighFreq","StartFile","EndFile", #test: startfile and endfile in here 
                  "Rugosity","Crest","Temporal Entropy","Shannon Entropy","Roughness", "autoc mean", "autoc median","autoc se",
                  "dfreq mean","dfreq se","specprop mean","specprop sd","specprop se","specprop median","specprop mode","specprop q25",
                  "specprop q75","specprop IQR","specprop centroid","specprop skewness","specprop kurtosis","specprop sfm","specprop sh","specprop precision",
                  "Amp env med","total entropy","Modinx","Startdom","Enddom","Mindom",
                  "Maxdom","Dfrange","Dfslope","Meanpeakf","AreaX maxP","AreaX Max", "AreaX dom","AreaX std","AreaY maxP",
                  "AreaY max","AreaY dom","AreaY std","Area spread","AreaMax","AreaMax Dom","AreaTop3 Dom","Num Shapes",
                  "BestTheta Hough","BestRho Hough","BestScore Hough","BestSlope Hough","BestB Hough","MedTheta Hough","MeanTheta Hough","sdTheta Hough",
                  "MedRho Hough","MeanRho Hough","sdRho Hough","MedScore Hough","MeanScore Hough","sdScore Hough","MedSlope Hough","MeanSlope Hough",
                  "sdSlope Hough","MedB Hough","MeanB Hough","sdB Hough","num Goodlines","xavg","yavg","SwitchesX mean","SwitchesX se",
                  "SwitchesX max","SwitchesX min","SwitchesY mean","SwitchesY se","SwitchesY max","SwitchesY min","sTheta","sThetaSD","sRho",
                  "sRhoSD","sScore","sScoreSD","sSlope","sSlopeSD","atansSlope","atansSlopeSD","sB","sBSD","sD","sDSD","sSign","sSignSD",
                  "sArea","sAreaSD","sPerimeter","sPerimeterSD","sMintime","sMintimeSD","sMaxtime","sMaxtimeSD","sMinfreq","sMinfreqSD","sMaxfreq","sMaxfreqSD",
                  "sFreqrange","sFreqrangeSD","sDuration","sDurationSD","sxCent","sxCentSD","syCent","syCentSD","sEccentricity",
                  "sEccentricitySD","sElongation","sElongationSD","sCircularity","sCircularitySD","sRectangularity","sRectangularitySD",
                  "sConvexity","sConvexitySD","sSolidarity","sSolidaritySD","sRectAngle","sRectAngleSD","SumCent","SumCent Abs","meanCent",
                  "meanCent Abs","perconcave","sCompared"
)

#out2<-cbind(out2[,c("StartTime","EndTime","LowFreq","HighFreq")],data[,c("StartFile","EndFile")],out2[,5:ncol(out2)])

#make FG back into character then factor type: 

StartID<-FG$FileName[out2$StartFile] #only assumption is that you didn't reorder FG after assigning ID 
EndID<-FG$FileName[out2$EndFile]

out2$StartFile<-StartID
out2$EndFile<-EndID

for(n in 1:crs){
  file.remove(paste(tmpPath,"/chunk",n,".csv.gz",sep=""))
}

unlink(tmpPath,recursive=TRUE, force = TRUE)

write.csv(out2,gzfile(paste(resultPath,outName,sep="/")),row.names = FALSE)


