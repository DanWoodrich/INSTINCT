#set these variables in all containers:
MethodID<-"bled-and-combine-v1-0"

library(doParallel)
library(tuneR)
library(signal)
library(foreach)

readWave2 <- 
  function(filename, from = 1, to = Inf, 
           units = c("samples", "seconds", "minutes", "hours"), header = FALSE, toWaveMC = NULL){
    
    read4ByteUnsignedInt <- function(){
      as.vector(readBin(con, int, n = 4, size = 1, endian = "little", signed = FALSE) %*% 2^c(0, 8, 16, 24))
    }
    
    if(!is.character(filename))
      stop("'filename' must be of type character.")
    if(length(filename) != 1)
      stop("Please specify exactly one 'filename'.")
    if(!file.exists(filename))
      stop("File '", filename, "' does not exist.")
    
    ## Open connection
    con <- file(filename, "rb")
    on.exit(close(con)) # be careful ...
    int <- integer()
    
    ## Reading in the header:
    RIFF <- readChar(con, 4)
    file.length <- read4ByteUnsignedInt()
    WAVE <- readChar(con, 4)
    
    ## waiting for the WAVE part
    i <- 0
    while(!(RIFF == "RIFF" && WAVE == "WAVE")){
      i <- i+1
      seek(con, where = file.length - 4, origin = "current")
      RIFF <- readChar(con, 4)
      file.length <- read4ByteUnsignedInt()
      WAVE <- readChar(con, 4)
      if(i > 5) stop("This seems not to be a valid RIFF file of type WAVE.")
    }
    
    
    FMT <- readChar(con, 4)    
    bext <- NULL
    ## extract possible bext information, if header = TRUE
    if (header && (tolower(FMT) == "bext")){
      bext.length <- read4ByteUnsignedInt()
      bext <- sapply(seq(bext.length), function(x) readChar(con, 1, useBytes=TRUE))
      bext[bext==""] <- " "
      bext <- paste(bext, collapse="")
      FMT <- readChar(con, 4)
    }
    
    ## waiting for the fmt chunk
    i <- 0
    while(FMT != "fmt "){
      i <- i+1
      belength <- read4ByteUnsignedInt()
      seek(con, where = belength, origin = "current")
      FMT <- readChar(con, 4)
      if(i > 5) stop("There seems to be no 'fmt ' chunk in this Wave (?) file.")
    }
    fmt.length <- read4ByteUnsignedInt()
    pcm <- readBin(con, int, n = 1, size = 2, endian = "little", signed = FALSE)
    ## FormatTag: only WAVE_FORMAT_PCM (0,1), WAVE_FORMAT_IEEE_FLOAT (3), WAVE_FORMAT_EXTENSIBLE (65534, determined by SubFormat)
    if(!(pcm %in% c(0, 1, 3, 65534)))
      stop("Only uncompressed PCM and IEEE_FLOAT Wave formats supported")
    channels <- readBin(con, int, n = 1, size = 2, endian = "little")
    sample.rate <- readBin(con, int, n = 1, size = 4, endian = "little")
    bytes.second <- readBin(con, int, n = 1, size = 4, endian = "little")
    block.align <- readBin(con, int, n = 1, size = 2, endian = "little")
    bits <- readBin(con, int, n = 1, size = 2, endian = "little")
    if(!(bits %in% c(8, 16, 24, 32, 64)))
      stop("Only 8-, 16-, 24-, 32- or 64-bit Wave formats supported")
    ## non-PCM (chunk size 18 or 40)
    
    if(fmt.length >= 18){    
      cbSize <- readBin(con, int, n = 1, size = 2, endian = "little")
      ## chunk size 40 (extension 22)
      if(cbSize == 22 && fmt.length == 40){
        validBits <- readBin(con, int, n = 1, size = 2, endian = "little")
        dwChannelMask <- readBin(con, int, n = 1, size = 4, endian = "little")    
        channelNames <- MCnames[as.logical(intToBits(dwChannelMask)),"name"]
        SubFormat <- readBin(con, int, n = 1, size = 2, endian = "little", signed = FALSE)
        x <- readBin(con, "raw", n=14)
      } else {
        if(cbSize > 0) 
          seek(con, where = fmt.length-18, origin = "current")
      }   
    }    
    if(exists("SubFormat") && !(SubFormat %in% c(0, 1, 3)))
      stop("Only uncompressed PCM and IEEE_FLOAT Wave formats supported")
    
    ## fact chunk
    #    if((pcm %in% c(0, 3)) || (pcm = 65534 && SubFormat %in% c(0, 3))) {
    #      fact <- readChar(con, 4)
    #      fact.length <- readBin(con, int, n = 1, size = 4, endian = "little")
    #      dwSampleLength <- readBin(con, int, n = 1, size = 4, endian = "little")
    #    }
    
    DATA <- readChar(con, 4)
    ## waiting for the data chunk    
    i <- 0    
    while(length(DATA) && DATA != "data"){
      i <- i+1
      belength <- read4ByteUnsignedInt()
      seek(con, where = belength, origin = "current")
      DATA <- readChar(con, 4)
      if(i > 5) stop("There seems to be no 'data' chunk in this Wave (?) file.")
    }
    if(!length(DATA)) 
      stop("No data chunk found")
    data.length <- read4ByteUnsignedInt()
    bytes <- bits/8
    if(((sample.rate * block.align) != bytes.second) || 
       ((channels * bytes) != block.align))
      warning("Wave file '", filename, "' seems to be corrupted.")
    
    ## If only header required: construct and return it
    if(header){
      return(c(list(sample.rate = sample.rate, channels = channels, 
                    bits = bits, samples = data.length / (channels * bytes)),
               if(!is.null(bext)) list(bext = bext)))
    }
    
    ## convert times to sample numbers
    fctr <- switch(match.arg(units),
                   samples = 1,
                   seconds = sample.rate,
                   minutes = sample.rate * 60,
                   hours = sample.rate * 3600)
    if(fctr > 1) {
      from <- round(from * fctr + 1)
      to <- round(to * fctr)
    } 
    
    ## calculating from/to for reading in sample data    
    N <- data.length / bytes
    N <- min(N, to*channels) - (from*channels+1-channels) + 1
    seek(con, where = (from - 1) * bytes * channels, origin = "current")
    
    ## reading in sample data
    ## IEEE FLOAT 
    if(pcm == 3 || (exists("SubFormat") && SubFormat==3)){
      sample.data <- readBin(con, "numeric", n = N, size = bytes, 
                             endian = "little")        
    } else {
      ## special case of 24 bits
      if(bits == 24){
        sample.data <- readBin(con, int, n = N * bytes, size = 1, 
                               signed = FALSE, endian = "little")
        sample.data <- as.vector(t(matrix(sample.data, nrow = 3)) %*% 256^(0:2))
        sample.data <- sample.data - 2^24 * (sample.data >= 2^23)
      } else {
        sample.data <- readBin(con, int, n = N, size = bytes, 
                               signed = (bytes != 1), endian = "little")
      }
    }
    
    ## output to WaveMC if selected by the user or if dwChannelMask suggests this is a multichannel Wave
    toWaveMC <- if(pcm != 65534 || (exists("dwChannelMask") && dwChannelMask %in% c(1,3))) isTRUE(toWaveMC) else TRUE  
    
    if(toWaveMC){
      ## Constructing the WaveMC object: 
      object <- new("WaveMC", samp.rate = sample.rate, bit = bits, 
                    pcm = !(pcm == 3 || (exists("SubFormat") && SubFormat==3)))
      object@.Data <- matrix(sample.data, ncol = channels, byrow=TRUE)
      if(exists("channelNames")) {
        if((lcN <- length(channelNames)) < channels)
          channelNames <- c(channelNames, paste("C", (lcN+1):channels, sep=""))
        colnames(object@.Data) <- channelNames
      }
    } else {
      ## Constructing the Wave object: 
      object <- new("Wave", stereo = (channels == 2), samp.rate = sample.rate, bit = bits, 
                    pcm = !(pcm == 3 || (exists("SubFormat") && SubFormat==3)))
      if(channels == 2) {
        sample.data <- matrix(sample.data, nrow = 2)
        object@left <- sample.data[1, ]
        object@right <- sample.data[2, ]
      } else {
        object@left <- sample.data
      }
    }
    
    ## Return the Wave object
    return(object)
  }


doIDvec<-function(x){
  #define peak as from the first minima, through the maxima, and up until the next minima. If peak is not true detection, 
  #turn off this region. 
  
  #ID each peak
  IDvec<-rep(0,length(x))
  ID<-1
  for(z in 1:length(x)){
    if(x[z]==2){
      IDvec[z]<-ID
      rightID<-FALSE
      p=z
      while(rightID==FALSE){
        if(z>1){
          p=p-1
          if(x[p]==1){
            IDvec[z]<-ID
            rightID<-TRUE
          }            
        }else{
          IDvec[z]<-ID
          rightID<-TRUE
        }
        
      }
      IDvec[p:z]<-ID
      
      ID<-ID+1
    }
  }
  
  IDvec<-cummax(IDvec) #this had date boundaries for the peaks 
  IDvec[which(IDvec==0)]<-1
  
  return(IDvec)
}


localMaxima <- function(x) {
  # Use -Inf instead if x is numeric (non-integer)
  y <- diff(c(-.Machine$integer.max, x)) > 0L
  rle(y)$lengths
  y <- cumsum(rle(y)$lengths)
  y <- y[seq.int(1L, length(y), 2L)]
  if (x[[1]] == x[[2]]) {
    y <- y[-1]
  }
  y
}

localMinima <- function(x) {
  # Use -Inf instead if x is numeric (non-integer)
  y <- diff(c(.Machine$integer.max, x)) < 0L
  rle(y)$lengths
  y <- cumsum(rle(y)$lengths)
  y <- y[seq.int(1L, length(y), 2L)]
  if (x[[1]] == x[[2]]) {
    y <- y[-1]
  }
  y
}

localMinima2 <- function(x) {
  # Use -Inf instead if x is numeric (non-integer)
  y <- diff(c(.Machine$integer.max, rev(x))) < 0L
  rle(y)$lengths
  y <- cumsum(rle(y)$lengths)
  y <- y[seq.int(1L, length(y), 2L)]
  if (rev(x)[[1]] == rev(x)[[2]]) {
    y <- y[-1]
  }
  y
}

MinimaAndMaxima<-function(x){
  x=as.numeric(x)
  #CALCULATE MAXIMA AND MINIMA
  out1<-localMaxima(x)
  vec1<-rep(FALSE,length(x))
  vec1[out1]<-TRUE
  
  out2<-localMinima(x)
  vec2<-rep(FALSE,length(x))
  vec2[out2]<-TRUE
  
  out3<-localMinima2(x)
  vec3<-rep(FALSE,length(x))
  vec3[out3]<-TRUE
  vec3<-rev(vec3)
  
  #turn on to visualize
  #plot(x,type="l")
  #points(vec1,add=TRUE,col="red")
  #points(vec2,add=TRUE,col="green")
  #points(vec3,add=TRUE,col="blue")
  
  #combine minima
  vec2<-(vec2|vec3)
  
  vec<-rep(0,length(x))
  vec[vec1]<-2
  vec[vec2]<-1
  
  return(vec)
}

AssignDetections<-function(MaM,Times){
  peaks<-which(MaM==2)
  dets<-foreach(f=1:sum(MaM==2)) %do% {
    mid<-peaks[f]
    val<-mid
    while(MaM[val]!=1&val!=1){
      val<-val-1
    }
    start<-Times[val]
    
    mid<-peaks[f]
    val<-mid
    while(MaM[val]!=1&val!=length(MaM)){
      val<-val+1
    }
    end<-Times[val]
    return(c(start,end))
    
  }
  
  return(do.call('c',dets))
}


startLocalPar<-function(num_cores,...){
  
  cluz <<- parallel::makeCluster(num_cores)
  registerDoParallel(cluz)
  
  clusterExport(cluz, c(...))
  
}

#the actual energy detector (Make new image when changing this)

EnergyDetectoR<-function(Wav,metaData,windowLength,Overlap,noiseThresh,noiseWinLength,noiseHopLength,dBadd,lowFreq,highFreq,numBands,bandOvlp,minDur,maxDur,minFreq,combineMethod){
  
  
  
  #temporary
  #Wav=readWave(filePaths,from=0,to=600,units='seconds')
  #metaData=dataMini

    
    #next time: 
    #adapt fin algo to work for these methods
    
    
  #to convert to db: average psd values, then 10*log(values) to get dB? 
  #should work basically the same as raven to normalize energy values and then log transform? 
  
  roldur<-c(dataMini$cumsum,(dataMini$cumsum[nrow(dataMini)]+dataMini$Duration[nrow(dataMini)]))
  
  #goal is to find 'in band power' in each time slice in spectrogram
  #will allow us to reverse engineer BLED
  #time1<-Sys.time()
  
  #this is a dummy energy detector just designed for low. Other designs should be created for full sound files
  start<-Sys.time() 
  
  spectrogram<- specgram(x = Wav@left,
                         Fs = Wav@samp.rate,
                         window=windowLength,
                         overlap=Overlap
  )
  end<-Sys.time()
  
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
  bandOverlapNum<-round(1/bandOvlp)
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
  rowz<-noiseWinLength/noiseHopLength
  
  
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
  step=60
  
  topcol<-max(Detections[,3])-lowFreq+1
  for(f in seq(to=max(roldur),from=0,by=step)){
  #just show certain time range
  tempogram<-spectrogram
  start=f
  if(start==max(roldur)){
    break
  }
  end = f+step
  if(end>max(roldur)){
    end=max(roldur)
  }
  tempogram$S<-spectrogram$S[,(start/tAdjust):(end/tAdjust)]
  tempogram$t<-spectrogram$t[(start/tAdjust):(end/tAdjust)]
  plot(tempogram)
  
  rect(Detections[,1],Detections[,3],Detections[,2],Detections[,4],col=rainbow(topcol,alpha=0.25)[Detections[,3]-lowFreq+1])
  
  abline(v=Detections[,1],col="red")
  abline(v=Detections[,2],col="red")
  }
  
  #
  #algorithm: upsweep downsweep: every spectrogram time step search for if new (not in run) box exists.give unique ID and search for box
  #existence every new step. If not there, search for existence of above/below 
  
  #only perform if there are multiple bands 
  if(numBands>1&combineMethod!="None"){
  
  if(combineMethod=="Upsweep"|combineMethod=="Downsweep"){
    
    
    #if upsweep just invert freq position of downsweep bins
    if(combineMethod=="Upsweep"){
      #sort from low to high
      bandsMod<-rev(bands)
      Detections<-Detections[order(Detections[,1],Detections[,3]),]#reverse the order it counts stacks detection
      extentCorrect<-bandWidth
      lowIndex<-3
      highIndex<-4
    }else{
      bandsMod<-bands
      Detections<-Detections[order(Detections[,1],-Detections[,3]),]#reverse the order it counts stacks detection
      extentCorrect<-0
      lowIndex<-4
      highIndex<-3
    }
    
    #once box is detected, will give it unique ID and set box and etection status 'live'. while box is live, if another box 
    #appears in next position, will give it same unique ID, set prev box status dead and set box live. If box ends
    #before finding next eligible box will set ID status dead
    
    IDsvec<-rep(0,length(bandsMod))
    IDsvecPrev<-rep(0,length(bandsMod))

    combDets<-vector("list", nrow(Detections))
    IDcounter<-1
    
   
    for(z in 1:length(spectrogram$t)){
      slice=Detections[which(Detections[,1]==spectrogram$t[z]),,drop=FALSE]
      if(length(slice)>0){
        #for each of these, check if band above has an ID. If not, 
        for(g in 1:nrow(slice)){
          whichBand<-which(bandsMod==slice[g,3])
          if(whichBand==length(bandsMod)){
            IDsvec[whichBand]<-IDcounter
            combDets[[IDcounter]][1]<-spectrogram$t[z]
            combDets[[IDcounter]][2]<-NA
            combDets[[IDcounter]][lowIndex]<-bandsMod[whichBand]+bandWidth-extentCorrect
            combDets[[IDcounter]][highIndex]<-NA
            IDcounter<-IDcounter+1
          }else{
            bandStatus<-IDsvec[whichBand+1]
            if(bandStatus!=0){
              IDsvec[whichBand]<-bandStatus
              #makes run take most 'direct' route'. If issue of call time data getting cut off, can adjust spectrogram
              #window to be less precise on time or add buffer to end of calls
              IDsvec[whichBand+1]<-0

              
            }else{
              IDsvec[whichBand]<-IDcounter
              combDets[[IDcounter]][1]<-spectrogram$t[z]
              combDets[[IDcounter]][2]<-NA
              combDets[[IDcounter]][lowIndex]<-bandsMod[whichBand]+bandWidth-extentCorrect
              combDets[[IDcounter]][highIndex]<-NA
              
              IDcounter<-IDcounter+1
            }
          }
        }
      }
      
      slice=Detections[which(Detections[,2]==spectrogram$t[z]),,drop=FALSE]
      if(length(slice)>0){
        for(g in 1:nrow(slice)){
          whichBand<-which(bandsMod==slice[g,3])
          IDsvec[whichBand]<-0
        }
      }
      
      #save detections whose streak has ended
      activeIDs<-unique(IDsvec)[which(unique(IDsvec)!=0)]
      activeIDsPrev<-unique(IDsvecPrev)[which(unique(IDsvecPrev)!=0)]
        
      ended=activeIDsPrev[(!activeIDsPrev %in% activeIDs)]
      if(length(ended)>0){
        for(p in ended){
          lowBound<-bandsMod[min(which(IDsvecPrev==p))]
          combDets[[p]][2]<-spectrogram$t[z]
          combDets[[p]][highIndex]<-lowBound+extentCorrect
        }
      }

      #set next active Prev
      IDsvecPrev<-IDsvec
      
    } 
  }else if(combineMethod=="Stacked"){
    
    Detections<-Detections[order(Detections[,1],Detections[,3]),]#reverse the order it counts stacks detection
    
    IDsvec<-rep(0,length(bands))
    IDsvecPrev<-rep(0,length(bands))
    
    trackIDs<-vector("list",length(spectrogram$t))
    
    combDets<-vector("list", nrow(Detections))
    IDcounter<-1
    
    
    for(z in 1:length(spectrogram$t)){
      slice=Detections[which(Detections[,1]==spectrogram$t[z]),,drop=FALSE]
      if(length(slice)>0){
        #for each of these, check if band above has an ID. If not, 
        for(g in nrow(slice):1){
          whichBand<-which(bands==slice[g,3])
          if(whichBand==length(bands)){
            bandStatus<-IDsvec[whichBand-1]
          }else if(whichBand==1){
            bandStatus<-IDsvec[whichBand+1]

          }else{
            bandStatus<-IDsvec[whichBand+1]

            if(bandStatus==0){
              bandStatus<-IDsvec[whichBand-1]
            }
          }
          if(bandStatus!=0){
            IDsvec[whichBand]<-bandStatus
            combDets[[IDsvec[whichBand]]][3]<-bands[whichBand]
            

          }else{
            IDsvec[whichBand]<-IDcounter
            combDets[[IDcounter]][1]<-spectrogram$t[z]
            combDets[[IDcounter]][2]<-NA
            combDets[[IDcounter]][3]<-bands[whichBand]
            combDets[[IDcounter]][4]<-bands[whichBand]+bandWidth
            
            IDcounter<-IDcounter+1
            }
          }
        }
      
        slice=Detections[which(Detections[,2]==spectrogram$t[z]),,drop=FALSE]
        if(length(slice)>0){
          for(g in 1:nrow(slice)){
            whichBand<-which(bands==slice[g,3])
            IDsvec[whichBand]<-0
          }
        }
      
        #save detections whose streak has ended
        activeIDs<-unique(IDsvec)[which(unique(IDsvec)!=0)]
        activeIDsPrev<-unique(IDsvecPrev)[which(unique(IDsvecPrev)!=0)]
      
        ended=activeIDsPrev[(!activeIDsPrev %in% activeIDs)]
        if(length(ended)>0){
          for(p in ended){
            combDets[[p]][2]<-spectrogram$t[z]
            }
          }
      
      #set next active Prev
      IDsvecPrev<-IDsvec
      
      #trackIDs[[z]]<-IDsvec #take out except for testing
      
      }
    } 
  
  Detections<-do.call('rbind',combDets)
  #test2<-do.call('cbind',trackIDs)
  #rect(test[,1],test[,3],test[,2],test[,4],col=rainbow(1,alpha=.5))
  #text(test[,1],test[,3])
  
  }
  
  #subset by min/max choices 
  Detections<-Detections[which(Detections[,2]-Detections[,1]>=minDur),,drop=FALSE]
  Detections<-Detections[which(Detections[,2]-Detections[,1]<=maxDur),,drop=FALSE]
  
  #subset by min freq
  Detections<-Detections[which(Detections[,4]-Detections[,3]>=minFreq),,drop=FALSE]
  
  #combine boxes which are internal to other bounding boxes: someday
  
  return(Detections)

}


#get params from the following line: 
#Worker(salt=643267509, workers=1, host=AKCSL2051-LN18, username=daniel.woodrich, pid=3728) failed    UnifyED(ProjectRoot=C:/Apps/INSTINCT/, system=win, r_version=C:/Users/daniel.woodrich/Work/R/R-4.0.3, upstream_task=FormatFG(ProjectRoot=C:/Apps/INSTINCT/, FGhash=aa3f4ad63e6ad534dc7ce33e260f9aebb3f10485, FGfile=C:/Apps/INSTINCT/Data/FileGroups/AL16_AU_BS03_files_152-354.csv), splits=3, CPU=99, Chunk=20, FGhash=aa3f4ad63e6ad534dc7ce33e260f9aebb3f10485, SoundFileRootDir_Host=//161.55.120.117/NMML_AcousticsData/Audio_Data/Waves, EDparamsHash=d939e0121a0c175b0d83dbb02b1c7f91c8b8d3b9, Params=0.5 Stacked 2 50 25 100 1 0 2 0.25 40 1 0 128 bled-and-combine-v1-0, MethodID=bled-and-combine-v1-0, ProcessID=EventDetector)

#Params=0.5 Stacked 2 50 25 100 1 0 2 0.25 40 1 0 128 bled-and-combine-v1-0, MethodID=bled-and-combine-v1-0, ProcessID=EventDetector)

#windows test values
DataPath <-"//161.55.120.117/NMML_AcousticsData/Audio_Data/Waves/"        #docker volume
FGpath <- "C:/Apps/INSTINCT/Cache/7f1040f41deafd01007a7cd0ad636c71fc686212"  #docker volume
ParamPath <- "C:/Apps/INSTINCT/etc"
resultPath<-"C:/Apps/INSTINCT/Out"
ReadFile<-'FileGroupFormatSplit3.csv.gz'
EDstage<-"1"

crs<- as.numeric(99)
chunkSize<- as.numeric(20)

#make sure these are alphabetical (alphabeterical to python variables that is)
bandOvlp = as.numeric(0.5)
combineMethod = "Stacked"
dBadd<- as.numeric(2)
highFreq<-as.numeric(50)
lowFreq<-as.numeric(25)
maxDur = as.numeric(100)
minDur = as.numeric(1)
minFreq = as.numeric(0)
noiseHopLength<-as.numeric(2)
noiseThresh<-as.numeric(0.25)
noiseWinLength<-as.numeric(40)
numBands <- as.numeric(1)
Overlap<-as.numeric(0)
windowLength<-as.numeric(128)

args<-commandArgs(trailingOnly = TRUE)

#docker values
DataPath <- args[1]
FGpath <- args[2]
resultPath <- args[3]
ReadFile<-args[4]
EDstage<-args[5]

crs<- as.numeric(args[6])
chunkSize<- as.numeric(args[7])

#make sure these are alphabetical (alphabeterical to python variables that is)
bandOvlp = as.numeric(args[8])
combineMethod = args[9]
dBadd<- as.numeric(args[10])
highFreq<-as.numeric(args[11])
lowFreq<-as.numeric(args[12])
maxDur = as.numeric(args[13])
minDur = as.numeric(args[14])
minFreq = as.numeric(args[15])
noiseHopLength<-as.numeric(args[16])
noiseThresh<-as.numeric(args[17])
noiseWinLength<-as.numeric(args[18])
numBands <- as.numeric(args[19])
Overlap<-as.numeric(args[20])
windowLength<-as.numeric(args[21])

data<-read.csv(paste(FGpath,ReadFile,sep="/"))

#split dataset into difftime group to parallelize 
filez<-nrow(data)


#big process: do split chunks evenly to ensure close to equal processing times
if(EDstage=="1"){
  
  BigChunks<-ceiling(filez/(crs*chunkSize))
  #splitID<-2
  splitID<-as.integer(substr(ReadFile,21,nchar(ReadFile)-7)) #assumes this stays as FileGroupFormatSplitx.csv.gz (tolerant of more digits)
  
  crsRead<-crs
  
detOut<-foreach(i=1:BigChunks) %do% {
  #reload crs at start of every loop 

  crs<- crsRead
  if(crs>detectCores()){
    crs<-detectCores()
  }
  
  StartFile<-(1+i*(crs*chunkSize)-(crs*chunkSize))
  if(i!=BigChunks){
    EndFile<-i*(crs*chunkSize)
  }else{
    EndFile<-filez
  }
  
  FilezPerCr<-ceiling(length(StartFile:EndFile)/crs)
  
  FilezAssign<-rep(1:crs,each=FilezPerCr)
  FilezAssign<-FilezAssign[1:length(StartFile:EndFile)]
  
  #reassign crs based on crs which actually made it into file split (will be crs on each except possibly not on last BigChunk)
  crs<-length(unique(FilezAssign))
  
  #eventually go to effort to make a step here to presave data and grab it with each core. 
  
  
  #foreach into chunks 
  startLocalPar(crs,"FilezAssign","data","EnergyDetectoR","specgram","splitID","StartFile","EndFile","windowLength","Overlap","noiseThresh","noiseWinLength","noiseHopLength","dBadd","lowFreq","highFreq","numBands","bandOvlp","minDur","maxDur","minFreq","combineMethod")
  
  Detections<-foreach(n=1:crs,.packages=c("tuneR","doParallel")) %dopar% {
    dataIn<-data[StartFile:EndFile,][which(FilezAssign==n),]
    #process per diffTime chunk
    outList <- vector(mode = "list")
    for(h in unique(dataIn$DiffTime)){
      #identifier for how file was processed
      processTag<-paste(h,splitID,i,n,sep="_")
      
      #load the sound file(s) into memory
      dataMini<-dataIn[which(dataIn$DiffTime==h),]
      dataMini$cumsum<-cumsum(dataMini$Duration)-dataMini$Duration[1]
      filePaths<-paste(DataPath,paste(dataMini$FullPath,dataMini$FileName,sep=""),sep="")
      if(nrow(dataMini)==1){
        soundFile=readWave2(filePaths)
      }else{
        SoundList <- vector(mode = "list", length = nrow(dataMini))
        for(g in 1:nrow(dataMini)){
          SoundList[[g]]<-readWave2(filePaths[g])
        }
        soundFile<-do.call(bind, SoundList)
      }
      
      #run detector
      
      outputs<-EnergyDetectoR(soundFile,dataMini,windowLength,Overlap,noiseThresh,noiseWinLength,noiseHopLength,dBadd,lowFreq,highFreq,numBands,bandOvlp,minDur,maxDur,minFreq,combineMethod)
      if(length(outputs)>0){

      Cums<-data.frame(cut(outputs[,1],breaks=c(0,dataMini$cumsum+dataMini$Duration[1]),labels=dataMini$cumsum),
                          cut(outputs[,2],breaks=c(0,dataMini$cumsum+dataMini$Duration[1]),labels=dataMini$cumsum),
                       cut(outputs[,1],breaks=c(0,dataMini$cumsum+dataMini$Duration[1]),labels=dataMini$FileName),
                       cut(outputs[,2],breaks=c(0,dataMini$cumsum+dataMini$Duration[1]),labels=dataMini$FileName))
      Cums[,1]<-as.numeric(levels(Cums[,1])[Cums[,1]])
      Cums[,2]<-as.numeric(levels(Cums[,2])[Cums[,2]])
      
      StartMod<-outputs[,1]-Cums[,1]
      EndMod<-outputs[,2]-Cums[,2]

      #convert outputs to have startfile, starttime, endfile, endtime. 
      outputs<-data.frame(StartMod,EndMod,outputs[,3],outputs[,4],Cums[,3],Cums[,4],processTag)
      colnames(outputs)<-c('StartTime','EndTime','LowFreq','HighFreq','StartFile',"EndFile","ProcessTag")
      }else{
        outputs<-NULL
      }
      
    outList[[h]]<-outputs
    }
    outList<-do.call('rbind',outList)
    return(outList)
  }
  stopCluster(cluz)
  
  Detections<-do.call('rbind',Detections)
  return(Detections)
}

#write to result
outName<-paste("EDSplit",splitID,".csv.gz",sep="")  


}else if(EDstage=="2"){
  #small process: use to unify breaks in larger process
  #keep difftimes together, but can still break into 
  #crs/chunk size batches to process 
  
  if(length(unique(data$DiffTime))<crs){
    crs<-length(unique(data$DiffTime))
  }
  
  startLocalPar(crs,"data","EnergyDetectoR","specgram")
  
  detOut<-foreach(n=unique(data$DiffTime),.packages=c("tuneR","doParallel")) %dopar% {
    dataMini<-data[which(data$DiffTime==n),]
    dataMini$cumsum<-cumsum(dataMini$Duration)-dataMini$Duration[1]
    filePaths<-paste(DataPath,paste(dataMini$FullPath,dataMini$FileName,sep=""),sep="")
    
    SoundList <- vector(mode = "list", length = nrow(dataMini))
    for(g in 1:nrow(dataMini)){
      SoundList[[g]]<-readWave(filePaths[g])
    }
    soundFile<-do.call(bind, SoundList)
    
    outputs<-EnergyDetectoR(soundFile,dataMini,windowLength,Overlap,noiseThresh,noiseWinLength,noiseHopLength,dBadd,lowFreq,highFreq,numBands,bandOvlp,minDur,maxDur,minFreq,combineMethod)
    
    if(length(outputs)>0){
    Cums<-data.frame(cut(outputs[,1],breaks=c(0,dataMini$cumsum+dataMini$Duration[1]),labels=dataMini$cumsum),
                     cut(outputs[,2],breaks=c(0,dataMini$cumsum+dataMini$Duration[1]),labels=dataMini$cumsum),
                     cut(outputs[,1],breaks=c(0,dataMini$cumsum+dataMini$Duration[1]),labels=dataMini$FileName),
                     cut(outputs[,2],breaks=c(0,dataMini$cumsum+dataMini$Duration[1]),labels=dataMini$FileName))
    Cums[,1]<-as.numeric(levels(Cums[,1])[Cums[,1]])
    Cums[,2]<-as.numeric(levels(Cums[,2])[Cums[,2]])
    
    StartMod<-outputs[,1]-Cums[,1]
    EndMod<-outputs[,2]-Cums[,2]
    
    #convert outputs to have startfile, starttime, endfile, endtime. 
    outputs<-data.frame(StartMod,EndMod,outputs[,3],outputs[,4],Cums[,3],Cums[,4],n)
    colnames(outputs)<-c('StartTime','EndTime','LowFreq','HighFreq','StartFile',"EndFile","DiffTime")
    }else{
      outputs<-NULL
    }
    
    return(outputs)
  #break data into 
  }

  outName<-paste("EDunify.csv.gz",sep="")  
  
}

detOut<-do.call('rbind',detOut)


write.csv(detOut,gzfile(paste(resultPath,outName,sep="/")),row.names = FALSE)







