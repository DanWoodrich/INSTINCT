MethodID<-"query-data-csv-v1-0"

#this is only ever to read from master .mat file 
#library(R.matlab)
library(sqldf)

args="C:/Apps/INSTINCT/Data/SoundChecker"

args<-strsplit(args,split=" ")[[1]]

args<-commandArgs(trailingOnly = TRUE)

datapath <- args[1]
resultPath<- args[2]

SfRoot<- args[3]
statement <- args[4]

source(paste("C:/Apps/INSTINCT/lib/supporting/instinct_fxns.R",sep="")) 

#datapath<-"//nmfs/akc-nmml/CAEP/Acoustics/ANALYSIS/RWupcallYeses4Dan.csv" #change to local folder
#datapath<-"//nmfs/akc-nmml/CAEP/Acoustics/ANALYSIS/RWgunshots4Dan.csv"   #change to local folder 

data<-read.csv(datapath)

data$StartFieldTimeUTC<-as.POSIXct(data$StartFieldTimeUTC,format="%d-%b-%Y %H:%M:%S",tz="UTC")
data$StartDateTime<-as.character(data$StartFieldTimeUTC)

#give this thing more queryable columns: month and year

data$year<-as.character(format(data$StartFieldTimeUTC,"%Y"))
data$month<-as.character(format(data$StartFieldTimeUTC,"%m"))


statement<-"StartDateTime > '2016-10-04 21:13:45' AND StartDateTime < '2017-10-10 21:13:45' AND MoorSite = 'BS01' LIMIT 150"
statement<-"month = 10"

#enforce keeping all of the columns, and starting with Where 
statement<-paste("SELECT * FROM data Where ",statement)

datasub<-sqldf(statement) 

#check to make sure there is data
if(nrow(datasub)==0){
  stop("The subset based on your statement has length 0! Stopping...")
}

#now convert it to FG format
sf<-getFileName(datasub$WaveFile)
sfdt<-vector(length=length(sf))
#insert a slash
for(n in 1:length(sf)){
  lenN<-nchar(sf[n])
  sfdt[n]<-substr(sf[n],lenN-16,lenN-4)
  sf[n]<-paste(substr(sf[n],1,lenN-17),"-",sfdt[n],sep="")
}

year<-paste("20",substr(sfdt,1,2),sep="")
month<-substr(sfdt,3,4)

#
SfRoot<-"//161.55.120.117/NMML_AcousticsData/Audio_Data"
  
#assemble fg: 
out<-data.frame(sf,paste("/",datasub$MoorDeploy,"/",month,"_",year,"/",sep=""),sfdt,0,datasub$MoorDeploy,datasub$StartSecInWav,datasub$EndSecInWav,datasub$MoorSite)

colnames(out)<-c("FileName","FullPath","StartTime","Duration","Deployment","SegStart","SegDur","SiteID")

pathsave<-""
#find the file durations
for(i in 1:nrow(out)){
  path = paste(SfRoot,"/Waves",out[i,"FullPath"],out[i,"FileName"],".wav",sep="")
  if(path!=pathsave){
    info<-readWave2(path,header = TRUE)
    out[i,"Duration"]<-round(info$samples/info$sample.rate)
  }else{
    out[i,"Duration"]<-round(info$samples/info$sample.rate)
  }
}

#print out the csv in outputs. 
write.csv(out,paste(resultPath,"/FGfile.csv",sep=""),row.names = FALSE)
