#set these variables in all containers:
MethodName<-"feat-ext-hough-light-source-v1-2"
#v1-1
#change catch in spectrogram rendering for something more general to other window lengths
#change catch for using autoc feature

#v1-2
#fix bug where P high and P low were same value 

#attempt at recreating the feature extraction protocol of INSTINCT.
libraries<-c("imager","pracma","oce","seewave","plotrix")
librariesToLoad<-c("imager","pracma")
nameSpaceFxns<-c("lastFeature","getMinBBox","freqstat.normalize","std.error")

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

FeatureExtracteR<-function(wav=NULL,spectrogram=NULL,featList,args,verbose=NULL,metadata=NULL){
  
    ChannelNormalize<-as.character(args[1])
    ImgThresh<-paste(as.integer(args[2]),"%",sep="")
    IsoblurSigma<-as.integer(args[3])
    Overlap<-as.integer(args[4])
    SpectrogramFunc<-args[5]
    targetSampRate<-args[6]
    TileAxisSize<-as.integer(args[7])
    #TMB<- as.integer(args[8]) #handled in wrapper 
    WindowLength<-as.integer(args[9])
    ZeroPadding<-as.integer(args[10])
    
    
    fs<-wav@samp.rate
    samples<-length(wav@left)
    snd = wav@left - mean(wav@left)
    
    wav.spec <- spec(wav,plot=F, PSD=T,wl=WindowLength)
    #wav.spec <- wav.spec[which(wav.spec[,1]<(High/1000)&wav.spec[,1]>(Low/1000)),]#,ylim=c(specVar$Low.Freq..Hz.[z],specVar$High.Freq..Hz.[z])
    wav.specprop <- specprop(wav.spec) #
    wav.meanspec = meanspec(wav, plot=F,ovlp=Overlap,wl=WindowLength)#not sure what ovlp parameter does but initially set to 90 #
    #wav.meanspec.db = meanspec(wav, plot=F,ovlp=50,dB="max0",wl=128)#not sure what ovlp parameter does but initially set to 90 #,flim=c(specVar$Low.Freq..Hz.[z]/1000,specVar$High.Freq..Hz.[z]/1000)
    if(samples>=(WindowLength*2)){
      wav.autoc = autoc(wav, plot=F,ovlp=Overlap,wl=WindowLength) #
    }else{
      wav.autoc = 99999
    }
    wav.dfreq = dfreq(wav, plot=F, ovlp=Overlap,wl=WindowLength) #tried bandpass argument, limited dfreq to only 2 different values for some reason. Seemed wrong. 
    Startdom<-wav.dfreq[,2][1]
    Enddom<-wav.dfreq[,2][length(wav.dfreq[,2])]
    Mindom <- min(wav.dfreq, na.rm = TRUE)
    Maxdom <- max(wav.dfreq, na.rm = TRUE)
    Dfrange <- Maxdom - Mindom
    featList[5] = rugo(wav@left / max(wav@left)) #rugosity
    featList[6] = crest(wav,wl=WindowLength)$C #crest factor
    wav.env = seewave:::env(wav, plot=F) 
    featList[7] = th(wav.env) #temporal entropy
    featList[8] = sh(wav.spec) #shannon entropy
    featList[9] = roughness(wav.meanspec[,2]) #spectrum roughness
    #not sure if these aren't crashing or just producing a lot of NAs- if crashing maybe make catch based on # samples. 
    if(samples>=(WindowLength*2)){ #little catch to hopefully avoid cases where window is too small 
      featList[10] = freqstat.normalize(mean(wav.autoc[,2], na.rm=T),featList[3],featList[4]) #autoc mean 
      featList[11] = freqstat.normalize(median(wav.autoc[,2], na.rm=T),featList[3],featList[4]) #autoc.median
      featList[12] = std.error(wav.autoc[,2], na.rm=T) #autoc se
    }else{
      featList[10] = 99999
      featList[11] = 99999
      featList[12] = 99999
    }
    #
    featList[13] = freqstat.normalize(mean(wav.dfreq[,2], na.rm=T),featList[3],featList[4]) #dfreq mean
    featList[14] = std.error(wav.dfreq[,2], na.rm=T) #dfreq se
    featList[15] = freqstat.normalize(wav.specprop$mean[1],featList[3],featList[4]) #specprop mean
    featList[16] = wav.specprop$sd[1] #specprop sd
    featList[17] = wav.specprop$sem[1] #specprop sem
    featList[18] = freqstat.normalize(wav.specprop$median[1],featList[3],featList[4]) #specprop median
    featList[19] = freqstat.normalize(wav.specprop$mode[1],featList[3],featList[4]) #specprop mode
    featList[20] = wav.specprop$Q25[1] # specprop q25
    featList[21] = wav.specprop$Q75[1] #specprop q75
    featList[22] = wav.specprop$IQR[1] #specprop IQR
    featList[23] = wav.specprop$cent[1] #specrop cent
    featList[24] = wav.specprop$skewness[1] #specprop skewness
    featList[25] = wav.specprop$kurtosis[1] #specprop kurtosis
    featList[26] = wav.specprop$sfm[1] #specprop sfm
    featList[27] = wav.specprop$sh[1] #specprop sh
    featList[28] = wav.specprop$prec[1] #specprop prec
    featList[29] = M(wav,wl=WindowLength) #amp env median
    featList[30] = H(wav,wl=WindowLength) #total entropy
    #warbler params
    if(samples>=(WindowLength*2)){
      featList[31]<- (sum(sapply(2:length(wav.dfreq[,2]), function(j) abs(wav.dfreq[,2][j] - wav.dfreq[,2][j - 1])))/(Dfrange)) #modinx
    }else{
      featList[31]<-99999
    }
    featList[32]<-freqstat.normalize(Startdom,featList[3],featList[4]) #startdom
    featList[33]<-freqstat.normalize(Enddom,featList[3],featList[4]) #enddom 
    featList[34]<-freqstat.normalize(Mindom,featList[3],featList[4]) #mindom
    featList[35]<-freqstat.normalize(Maxdom,featList[3],featList[4]) #maxdom
    featList[36]<-Dfrange #dfrange
    featList[37]<-((Enddom-Startdom)/(End-Start)) #dfslope
    if(length(lastFeature(fs,wav.meanspec,WindowLength))!=0){ #address error related to meanspec not showing up- probably related to too few samples. This is patchwork fix for very rare error. 
      featList[38]  <- freqstat.normalize(lastFeature(fs,wav.meanspec,WindowLength),featList[3],featList[4])
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
      
      
      if(length(spectrogram)==3){
        P = abs(spectrogram$S)
      }else{
        P = abs(spectrogram)
      }

      #
      
      #calculate based on nearest value
      P_high=which.min(abs(spectrogram$f-featList[4]))
      P_low=which.min(abs(spectrogram$f-featList[3]))
      
      #don't let P_high and P_low be same value. might break on low values. 
      if(P_high==P_low){
        P_low=P_high-1
      }
      
      P<-P[(P_high):(P_low),]
      
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
      
      #calculate based on nearest value
      P_high=which.min(abs(spectrogram$f-featList[4]))
      P_low=which.min(abs(spectrogram$f-featList[3]))
      
      P<-P[(P_high):(P_low),]
      
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
      labelsW<-as.matrix(labelsW[1:TileAxisSize,1:TileAxisSize,1,1])

      threshBool<-as.matrix(image1[1:TileAxisSize,1:TileAxisSize,1,1])

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
      
      #make this into a matrix
      image1<-as.matrix(image1[1:TileAxisSize,1:TileAxisSize,1,1])
      
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
      featList[83]<-worthyones
      
      for(g in 5:83){
        if(is.na(as.numeric(featList[g]))){
          featList[g]<-99999
        }else if(!is.finite(as.numeric(featList[g]))){
          featList[g]<-99999
        }
      }
      
    }
    

    
    return(as.numeric(featList))

  }
