MethodID<-"query-random-v1-0"

library(foreach)

dataFormat<- function(x){
  x$StartFieldTimeUTC<-as.POSIXct(x$StartFieldTimeUTC,format="%d-%b-%Y %H:%M:%S",tz="UTC")
  x$EndFieldTimeUTC<-as.POSIXct(x$EndFieldTimeUTC,format="%d-%b-%Y %H:%M:%S",tz="UTC")
  
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

#cant do it this way unfortunately, with the internal quotes. maybe can find a better way eventually..
args="C:/Apps/INSTINCT/Data/FileGroups //161.55.120.117/NMML_AcousticsData/Audio_Data NewFGtest.csv //nmfs/akc-nmml/CAEP/Acoustics/ANALYSIS/RWupyeses_1.csv AL16_AU_BS01_files_All.csv,AL16_AU_BS03_files_59-114.csv 6 1 query-random-v1-0"

args<-strsplit(args,split=" ")[[1]]

args<-commandArgs(trailingOnly = TRUE)

resultPath<- args[1]

SfRoot<- args[2]
fileName <- args[3]
datapath <- args[4]
Exclude <-args[5] #vector of FG to exclude from sample 
Pulls<- as.numeric(args[6])
RandSeed<- as.numeric(args[7])

#make random pulls consistent depending on seed set
set.seed(RandSeed)

source(paste("C:/Apps/INSTINCT/lib/supporting/instinct_fxns.R",sep="")) 

#datapath<-"//nmfs/akc-nmml/CAEP/Acoustics/ANALYSIS/RWupcallYeses4Dan.csv" #change to local folder
#datapath<-"//nmfs/akc-nmml/CAEP/Acoustics/ANALYSIS/RWgunshots4Dan.csv"   #change to local folder 

data<-read.csv(datapath,stringsAsFactors = FALSE)

#mandate column names to fit standard (column names changed on Cath end)
colnames(data)[1:7]<-c("Wavefile","StartSecInWav","EndSecInWav","MooringSite","MooringDeployID","StartFieldTimeUTC","EndFieldTimeUTC")

data<-dataFormat(data)

#remove any NA rows
if(any(is.na(data$dur))){
  data<-data[-which(is.na(data$dur)),]
}

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

#find out the consecutive sections present in the data: 

cycleVar<-1

data<-foreach(i=1:length(unique(data$MooringDeployID))) %do% {
  DatMoor<-data[which(data$MooringDeployID==unique(data$MooringDeployID)[i]),]
  DatMoor<-DatMoor[order(DatMoor$StartDateTime),]
  
  DatMoor$Cycle<-0
  
  for(n in 1:nrow(DatMoor)){
    DatMoor$Cycle[n]<-cycleVar
    
    if(DatMoor$EndFieldTimeUTC[n]!=DatMoor$StartFieldTimeUTC[n+1]&n!=nrow(DatMoor)){
      cycleVar<-cycleVar+1
    }
  }
  
  return(DatMoor)
}

data<-do.call("rbind",data)

#randomly sample n seeds: 

cyclesPull<-sample(data$Cycle,Pulls)

index<-which(data$Cycle %in% cyclesPull)

datasub<-data[sample(index,length(index)),] #randomize index, so when you sort by cycle you don't pure random order

orders<-cbind(unique(datasub$Cycle),1:length(unique(datasub$Cycle)))

datasub$Cycle<-match(datasub$Cycle, orders[,1])

datasub<-datasub[order(datasub$Cycle,datasub$StartDateTime),]


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
