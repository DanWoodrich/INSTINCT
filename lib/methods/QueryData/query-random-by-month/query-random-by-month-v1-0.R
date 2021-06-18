MethodID<-"query-random-by-month"

library(foreach)

dataFormat<- function(x){
  x$StartFieldTimeUTC<-as.POSIXct(x$StartFieldTimeUTC,format="%d-%b-%Y %H:%M:%S",tz="UTC")
  x$StartDateTime<-as.character(x$StartFieldTimeUTC)
  
  #give this thing more queryable columns: month and year
  
  x$year<-as.character(format(x$StartFieldTimeUTC,"%Y"))
  x$month<-as.character(format(x$StartFieldTimeUTC,"%m"))
  
  x$EndSecInWav<-as.numeric(x$EndSecInWav)
  x$StartSecInWav<-as.numeric(x$StartSecInWav)
  
  #how many unique
  x$sym<-paste(x$MooringSite,x$year,x$month)
  
  x$dur<-as.numeric(x$EndSecInWav)-as.numeric(x$StartSecInWav)
  
  x<-x[sample(1:nrow(x),nrow(x)),]
  
  x$sampleID<-1:nrow(x)
  
  return (x)
}

pullFxn<-function(dataIn,pullDur,pullType,percTarget){
  
  metaData<-data.frame(aggregate(dataIn$dur,list(dataIn$sym),sum))
  colnames(metaData)<-c("sym","mins")
  metaData$total<-0
  metaData$perc<-0
  
  pullID=1
  
  out<-list()
  dataTot<-0
  dataTotPerc=0
  i=1
  
  while(dataTotPerc<percTarget){
    
    RemoveSym<-c()
    
    #do random pulls of 5 hr chunks from sym (522 maximum). 
    out2<-foreach(p=1:length(unique(dataIn$sym))) %do% {
      dataMini<-dataIn[which(dataIn$sym==unique(dataIn$sym)[p]),]
      dataMini<-dataMini[order(dataMini$StartFieldTimeUTC),]
      
      pullInd<-sample(1:nrow(dataMini),1)
      
      h = round(dataMini[pullInd,"dur"])
      #walk forward and backwards to put together pull and satisfy effort condition. This can result in an odd behavior where 
      #one chunks 'jumps' ahead of another if it was previously sampled. 
      
      #maybe I could design it to that it walks towards most consecutive data when possible to minimize this, and preserve chunk continuity
      #say whichever direction has lowest distfrom start, go that way. 
      pull<-list(dataMini[pullInd,])
      listInd<-2
      backstep=1
      forwardstep=1
      step=1
      
      while(h<pullDur&step<nrow(dataMini)){
        
        if((pullInd-backstep)>0){
          distback<-(dataMini[pullInd,"StartFieldTimeUTC"]-dataMini[pullInd-backstep,"StartFieldTimeUTC"])
        }else{
          distback<-99999999999999
        }
        
        if((pullInd+forwardstep)<=nrow(dataMini)){
          distforward<-(dataMini[pullInd+forwardstep,"StartFieldTimeUTC"]-dataMini[pullInd,"StartFieldTimeUTC"])
        }else{
          distforward<-99999999999999
        }
        
        #choose the smallest step. If a tie, go with a random one
        if(distback==distforward){
          mod<-sample(c(-1,1),1)
          distback<-distback + mod #this will break the tie. 50/50 to go with forward or back
        }
        if(distback<distforward){
          pull[[listInd]]<-dataMini[pullInd-backstep,]
          h=h+round(dataMini[pullInd-backstep,"dur"])
          backstep=backstep+1
          
        }else if(distback>distforward){
          pull[[listInd]]<-dataMini[pullInd+forwardstep,]
          h=h+round(dataMini[pullInd+forwardstep,"dur"])
          forwardstep=forwardstep+1
          
        }
        
        listInd<-listInd+1
        step=step+1
        
        #expand outwards, stop when hit 
      }
      
      pull<-do.call('rbind',pull)
      pull$pullID<-pullID
      pullID=pullID+1
      
      #find the pull duration
      pullDurReal<-sum(pull$EndSecInWav-pull$StartSecInWav)
      
      #compare with metadata
      metaData[which(metaData$sym==dataMini$sym[1]),"total"]<-metaData[which(metaData$sym==dataMini$sym[1]),"total"]+pullDurReal
      metaData[which(metaData$sym==dataMini$sym[1]),"perc"]<-metaData[which(metaData$sym==dataMini$sym[1]),"total"]/metaData[which(metaData$sym==dataMini$sym[1]),"mins"]*100
      
      #if percentage sampled from the month exceeds the target, remove the sym from further consideration
      if(metaData[which(metaData$sym==dataMini$sym[1]),"perc"]>percTarget){
        RemoveSym<-c(RemoveSym,dataMini$sym[1])
      }
      
      return(pull)
      
    }
    out2<-do.call("rbind",out2)
    
    #remove the sampelIDs from dataIn. Add in a round and type identifier.
    out2$round<-i
    out2$type<-pullType
    
    dataIn<-dataIn[-which(dataIn$sampleID %in% out2$sampleID),]
    
    #remove any sym where percTarget was exceeded
    if(length(RemoveSym)>0){
      dataIn<-dataIn[-which(dataIn$sym %in% RemoveSym),]
      RemoveSym<-c()
    }
    
    out[[i]]<-out2
    
    print(i)
    
    i=i+1
    
    dataTot=sum(metaData$total)
    dataTotPerc<-dataTot/sum(metaData$mins)*100
    
    print(dataTotPerc)
    
  }
  
  out<-do.call("rbind",out)
  return(list(out,metaData))
  
}

saveSample<-function(x,xname,path){
  write.csv(x[[1]],paste(path,"/",xname,'_Samples.csv',sep=""))
  #write.csv(x[[2]],paste(path,"/",xname,'_Effort.csv',sep=""))
}


#cant do it this way unfortunately, with the internal quotes. maybe can find a better way eventually..
args="C:/Apps/INSTINCT/Data/FileGroups //161.55.120.117/NMML_AcousticsData/Audio_Data testFG.csv //nmfs/akc-nmml/CAEP/Acoustics/ANALYSIS/RWupyeses_1.csv AL16_AU_BS01_files_All.csv,AL16_AU_BS03_files_59-114.csv 15 7200 allRand 1"

args<-strsplit(args,split=" ")[[1]]

datapath <- "//nmfs/akc-nmml/CAEP/Acoustics/ANALYSIS/LMyeses_2.csv"
resultPath<- "C:/Apps/INSTINCT/Data/FileGroups"

SfRoot<- "//161.55.120.117/NMML_AcousticsData/Audio_Data"
fileName <- "LMyes.csv" 
statement <- "StartDateTime > '2016-10-04 21:13:45' AND StartDateTime < '2017-10-10 21:13:45' AND MooringSite = 'BS01' LIMIT 150"

args<-commandArgs(trailingOnly = TRUE)

resultPath<- args[1]

SfRoot<- args[2]
fileName <- args[3]
datapath <- args[4]
Exclude <-args[5] #vector of FG to exclude from sample 
PercTarget<- as.numeric(args[6])
PullDur<- as.numeric(args[7])
PullType<- args[8]
RandSeed<- as.numeric(args[9])

#make random pulls consistent depending on seed set
set.seed(RandSeed)

source(paste("C:/Apps/INSTINCT/lib/supporting/instinct_fxns.R",sep="")) 

#datapath<-"//nmfs/akc-nmml/CAEP/Acoustics/ANALYSIS/RWupcallYeses4Dan.csv" #change to local folder
#datapath<-"//nmfs/akc-nmml/CAEP/Acoustics/ANALYSIS/RWgunshots4Dan.csv"   #change to local folder 

data<-read.csv(datapath)

#mandate column names to fit standard (column names changed on Cath end)
colnames(data)[1:7]<-c("Wavefile","StartSecInWav","EndSecInWav","MooringSite","MooringDeployID","StartFieldTimeUTC","EndFieldTimeUTC")

data<-dataFormat(data)

#remove any NA rows
data<-data[-which(is.na(data$dur)),]

#load in other FG, and remove them from data. 
if(Exclude!="None"){
  Exclude<-strsplit(Exclude,split=",")[[1]]
  for(n in 1:length(Exclude)){
    
    #load in FG
    FG<-read.csv(paste(resultPath,"/",Exclude[n],sep=""))
    
    matches <-paste(getFileName(data$Wavefile),data$StartSecInWav,data$EndSecInWav) %in% paste(FG$FileName,FG$SegStart,FG$SegStart+FG$SegDur)
    if(any(matches)){
      data<-data[-which(matches),]
    }
    
  }
}

data<-pullFxn(dataIn=data,pullDur=PullDur,pullType=PullType,percTarget = PercTarget)

datasub<-data[[1]]

#now convert it to FG format
sf<-getFileName(datasub$Wavefile)
sfdt<-vector(length=length(sf))
#insert a slash
for(n in 1:length(sf)){
  lenN<-nchar(sf[n])
  sfdt[n]<-substr(sf[n],lenN-16,lenN-4)
  #sf[n]<-paste(substr(sf[n],1,lenN-17),"-",sfdt[n],sep="")
}



year<-paste("20",substr(sfdt,1,2),sep="")
month<-substr(sfdt,3,4)

#
#SfRoot<-"//161.55.120.117/NMML_AcousticsData/Audio_Data"


#assemble fg: 
out<-data.frame(sf,paste("/",datasub$MooringDeployID,"/",month,"_",year,"/",sep=""),sfdt,0,datasub$MooringDeployID,datasub$StartSecInWav,datasub$EndSecInWav-datasub$StartSecInWav,datasub$MooringSite)

colnames(out)<-c("FileName","FullPath","StartTime","Duration","Deployment","SegStart","SegDur","SiteID")

pathsave<-""
#find the file durations
for(i in 1:nrow(out)){
  path = paste(SfRoot,"/Waves",out[i,"FullPath"],out[i,"FileName"],sep="")
  if(path!=pathsave){
    info<-readWave2(path,header = TRUE)
    out[i,"Duration"]<-round(info$samples/info$sample.rate)
  }else{
    out[i,"Duration"]<-round(info$samples/info$sample.rate)
  }
}

#Do not allow overrides! Test to see if file is present, and only print if not. If it is, spit an error. 
filePath<-paste(resultPath,"/",fileName,sep="")
if(file.exists(filePath)){
  stop("Cannot overwrite existing FG of same name! Stopping...")
}else{
  write.csv(out,filePath,row.names = FALSE)
}
#print out the csv in outputs. 
