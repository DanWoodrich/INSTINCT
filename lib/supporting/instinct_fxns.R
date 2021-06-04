#These are miscellaneous INSTINCT functions that are reused often.

#don't think I need to call libraries in this script... just in the script I am sourcing from 

#however, script that calls this needs: 
#signal
#tuneR

#start parallel processing

getFileName<-function(x){
  allVals<-vector("list",length=length(x))
  for(n in 1:length(x)){
    lastSlash<-gregexpr("\\\\",x[n])[[1]][length(gregexpr("\\\\",x[n])[[1]])]
    xmod<-substr(x[n],lastSlash+1,nchar(x[n]))
    allVals[[n]]<-xmod
  }

  allVals<-do.call("c",allVals)
  return(allVals)
}


startLocalPar<-function(num_cores,...){
  
  cluz <<- parallel::makeCluster(num_cores)
  registerDoParallel(cluz)
  
  clusterExport(cluz, c(...))
  
}

#writeWave that suppresses warning
#ripped from the internet somewhere 
writeWave.nowarn <- 
  function(object, filename, extensible = TRUE) {
    if(!is(object, "WaveGeneral")) 
      stop("'object' needs to be of class 'Wave' or 'WaveMC'")
    validObject(object)
    if(is(object, "Wave")){
      object <- as(object, "WaveMC")
      colnames(object) <- c("FL", if(ncol(object) > 1) "FR")
    }
    if(ncol(object) > 2 && !extensible)
      stop("Objects with more than two columns (multi channel) can only be written to a Wave extensible format file.")
    
    cn <- colnames(object)
    if((length(cn) != ncol(object) || !all(cn %in% MCnames[["name"]])) || any(duplicated(cn)))
      stop("colnames(object) must be specified and must uniquely identify the channel ordering for WaveMC objects, see ?MCnames for possible channels")
    cnamesnum <- as.numeric(factor(colnames(object), levels=MCnames[["name"]]))
    if(is.unsorted(cnamesnum))
      object <- object[,order(cnamesnum)]
    dwChannelMask <- sum(2^(cnamesnum - 1))  ##
    
    l <- as.numeric(length(object)) # can be an int > 2^31
    sample.data <- t(object@.Data)
    dim(sample.data) <- NULL
    
    ## PCM or IEEE FLOAT
    pcm <- object@pcm                                 
    
    # Open connection
    con <- file(filename, "wb")
    on.exit(close(con)) # be careful ...
    
    # Some calculations:
    byte <- as.integer(object@bit / 8)
    channels <- ncol(object)
    block.align <- channels * byte
    bytes <- l * byte * channels
    
    if((!is.numeric(bytes) || is.na(bytes)) || (bytes < 0 || (round(bytes + if(extensible) 72 else 36) + 4) > (2^32-1)))
      stop(paste("File size in bytes is", round(bytes + if(extensible) 72 else 36) + 4, "but must be a 4 byte unsigned integer in [0, 2^32-1], i.e. file size < 4 GB"))
    
    ## Writing the header:
    # RIFF
    writeChar("RIFF", con, 4, eos = NULL) 
    write_4byte_unsigned_int(round(bytes + if(extensible) 72 else 36), con) # cksize RIFF
    
    
    # WAVE
    writeChar("WAVE", con, 4, eos = NULL)
    # fmt chunk
    writeChar("fmt ", con, 4, eos = NULL)
    if(extensible) { # cksize format chunk
      writeBin(as.integer(40), con, size = 4, endian = "little") 
    } else {
      writeBin(as.integer(16), con, size = 4, endian = "little")
    }    
    if(!extensible) { # wFormatTag
      writeBin(as.integer(if(pcm) 1 else 3), con, size = 2, endian = "little")
    } else {
      writeBin(as.integer(65534), con, size = 2, endian = "little") # wFormatTag: extensible   
    }
    writeBin(as.integer(channels), con, size = 2, endian = "little") # nChannels
    writeBin(as.integer(object@samp.rate), con, size = 4, endian = "little") # nSamplesPerSec
    writeBin(as.integer(object@samp.rate * block.align), con, size = 4, endian = "little") # nAvgBytesPerSec
    writeBin(as.integer(block.align), con, size = 2, endian = "little") # nBlockAlign
    writeBin(as.integer(object@bit), con, size = 2, endian = "little") # wBitsPerSample
    # extensible
    if(extensible) {
      writeBin(as.integer(22), con, size = 2, endian = "little") # cbsize extensible
      writeBin(as.integer(object@bit), con, size = 2, endian = "little") # ValidBitsPerSample
      writeBin(as.integer(dwChannelMask), con, size = 4, endian = "little") #  dbChannelMask
      writeBin(as.integer(if(pcm) 1 else 3), con, size = 2, endian = "little") # SubFormat 1-2
      writeBin(as.raw(c(0,   0,   0,  0,  16,   0, 128,   0 ,  0, 170,   0,  56, 155, 113)), con) # SubFormat 3-16
      # fact
      writeChar("fact", con, 4, eos = NULL)
      writeBin(as.integer(4), con, size = 4, endian = "little") # cksize fact chunk
      writeBin(as.integer(l), con, size = 4, endian = "little") # dwSampleLength
    }
    # data
    writeChar("data", con, 4, eos = NULL)
    write_4byte_unsigned_int(round(bytes), con)
    
    # Write data:
    # PCM format
    if(pcm) { 
      if(byte == 3){
        sample.data <- sample.data + 2^24 * (sample.data < 0)
        temp <- sample.data %% (256^2)
        sample.data <- sample.data %/% 256^2
        a2 <- temp %/% 256
        temp <- temp %%  256
        write_longvector(as.integer(rbind(temp, a2, sample.data)), con, size = 1, endian = "little", bytes=bytes)
      } else {
        write_longvector(as.integer(sample.data), con, size = byte, endian = "little", bytes=bytes)
      }
    } else {
      write_longvector(as.numeric(sample.data), con, size = byte, endian = "little", bytes=bytes)
    }
    
    invisible(NULL)
  }

#need this for writeWav.nowarn, unknown library or source
write_4byte_unsigned_int <- function(x, con){
  if((!is.numeric(x) || is.na(x)) || (x < 0 || x > (2^32-1))) 
    stop("the field length must be a 4 byte unsigned integer in [0, 2^32-1], i.e. file size < 4 GB")
  big <- x %/% 256^2
  small <- x %% 256^2
  writeBin(as.integer(c(small, big)), con, size = 2, endian = "little")
}

#need this for writeWav.nowarn, unknown library or source
write_longvector <- function(x, ..., bytes){
  if(bytes > (2^31-1)){
    index <- 1:floor(length(x)/2)
    writeBin(x[index], ...)
    writeBin(x[-index], ...)        
  }
  else writeBin(x, ...)
}

#readwav2
#description: just the tuneR readwav function, but with the file.access step removed to improve performance
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

#prime factor: 
#return a vector of prime factors for an integer. Used in decimateData. assume this is ripped from stackoverflow 
prime.factor <- function(x){
  n=c()
  i=2
  r=x
  while(prod(n)!=x){
    if(!r%%i) {n=c(n,i);r=r/i;i=1}
    i=i+1
  }
  return(n)
}

decDo<-function(wav,dfact,target_samp_rate){
  #if it is, test if factor is > 64. If it is, split rounds into 2 and start with small factor. 
  if(dfact<=64){ #64 is highest decimation where it appeared to be successful (128 resulted in complete signal loss)
    #perform a single decimation
    wavOut<-signal::decimate(wav@left,dfact)
    wavOut <- Wave(wavOut, right = numeric(0), samp.rate = target_samp_rate)
    return(wavOut)
  }else{
    #Otherwise, 
    #perform a decimation in rounds
    primes<-prime.factor(dfact)
    if(length(primes)>2){
      dfactRounds<-c(primes[length(primes)],prod(primes[1:length(primes)-1]))
    }else{
      dfactRounds<-primes
    }
    wavOut<-wav@left
    for(n in 1:length(dfactRounds)){ #if there is a prime over 64, can't reduce so will process all in one. results uncertain in these cases. 
      wavOut<-signal::decimate(wavOut,dfactRounds[n])
    }
    wavOut <- Wave(wavOut, right = numeric(0), samp.rate = target_samp_rate)
    return(wavOut)
  }
}

#this function resamples. If target is very close to wav rate, decimate without filtering. Will induce some aliasing but can be tolerable
#depending on application
resampINST<-function(wav,wav.samp.rate,target_samp_rate,resampThresh){
  
  percDiff<-1-(target_samp_rate/wav.samp.rate)
  
  if(percDiff<resampThresh){
    #just ignore aliasing and downsample
    wavOut<-Wave(wav@left[round(seq(1,length(wav@left),by=wav.samp.rate/target_samp_rate))], right = numeric(0), samp.rate = target_samp_rate)
  }else{
    #if too dissimilar, do the full decimation route
    wavOut<-signal::resample(wav@left,p=target_samp_rate,q=wav.samp.rate,d=5)
    
    wavOut <- Wave(wavOut, right = numeric(0), samp.rate = target_samp_rate)
  }
  
  return(wavOut)
}

#decimate sound file
#decimate a sound file in memory, so it can be used in various routines. Optimized for speed but 
#after acheiving a minimum suitiable quality (low signal loss and aliasing)

#uses resample and 

#some testing performed to assess if aliasing appears in intermediate steps. 
decimateData<-function(wav,target_samp_rate){
  
    #if target is within 2.5%, skip filter and accept some aliasing. 
    #specific to application, may want to change this value. Hardcoded for now 
  
    resampThresh<-0.025
  
    wav.samp.rate<-wav@samp.rate
    
    #wav.samp.rate<-16384
    #target_samp_rate<-250
    #default values if 
    #dfact<-wav.samp.rate/target_samp_rate
    
    #first, test for 3 conditions: target = wav samp rate
    if(wav.samp.rate==target_samp_rate){
      return(wav)
    }
    #leave wav file unchanged
    
    dfact<-wav.samp.rate/target_samp_rate
    
    #then, test if wav file factor is a clean integer multiple  
    if(dfact%%1==0){
      return(decDo(wav,dfact,target_samp_rate))
    }else{
      
    #if not a clean integer decimation, use resample to get there (much longer)
    #resample to the next clean multiple, using downsampling shortcut if eligible (introduces some amount of aliasing, careful with this)
    targetMaxSamp<-(wav.samp.rate-(wav.samp.rate%%target_samp_rate))
    
    targetDfact<-targetMaxSamp/target_samp_rate
    
    #prevents this from being prime number 
    while(length(prime.factor(targetDfact))==1){
      targetDfact<-targetDfact-1
    }
    
    targetMaxSamp<-target_samp_rate*targetDfact

    #make sure target MaxSamp is not prime. 
      
    wavOut<-resampINST(wav,wav.samp.rate,targetMaxSamp,resampThresh)
    
    if(wavOut@samp.rate!=target_samp_rate){
      dfact<-targetMaxSamp/target_samp_rate
      return(decDo(wavOut,dfact,target_samp_rate))
    }else{
      return(wavOut)
    }
    

    }
    
}