MethodID<-"query-data-csv-SQL-v1-0"

library(sqldf)

#cant do it this way unfortunately, with the internal quotes. maybe can find a better way eventually..
args="C:/Apps/INSTINCT/Data/FileGroups //161.55.120.117/NMML_AcousticsData/Audio_Data TestQueryWSQL.csv //nmfs/akc-nmml/CAEP/Acoustics/ANALYSIS/HumpbackYeses_2.csv None StartDateTime_>_'2014-01-01_00:00:00'_AND_StartDateTime_<_'2018-01-01_00:00:00'_AND_MooringSite_=_'PM02'_LIMIT_100 query-data-csv-SQL-v1-0"
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
statement <- as.character(args[6])

statement<-gsub("_", " ", statement)

source(paste("C:/Apps/INSTINCT/lib/supporting/instinct_fxns.R",sep="")) 

#datapath<-"//nmfs/akc-nmml/CAEP/Acoustics/ANALYSIS/RWupcallYeses4Dan.csv" #change to local folder
#datapath<-"//nmfs/akc-nmml/CAEP/Acoustics/ANALYSIS/RWgunshots4Dan.csv"   #change to local folder 

data<-read.csv(datapath)

#mandate column names to fit standard (column names changed on Cath end)
colnames(data)[1:7]<-c("Wavefile","StartSecInWav","EndSecInWav","MooringSite","MooringDeployID","StartFieldTimeUTC","EndFieldTimeUTC")

data$StartFieldTimeUTC<-as.POSIXct(data$StartFieldTimeUTC,format="%d-%b-%Y %H:%M:%S",tz="UTC")
data$StartDateTime<-as.character(data$StartFieldTimeUTC)

#give this thing more queryable columns: month and year

data$year<-as.character(format(data$StartFieldTimeUTC,"%Y"))
data$month<-as.character(format(data$StartFieldTimeUTC,"%m"))

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

#statement<-"StartDateTime > '2016-10-04 21:13:45' AND StartDateTime < '2017-10-10 21:13:45' AND MoorSite = 'BS01' LIMIT 150"
#statement<-"month = 10"

#enforce keeping all of the columns, and starting with Where 
statement<-paste("SELECT * FROM data WHERE",statement)

datasub<-sqldf(statement) 


#check to make sure there is data
if(nrow(datasub)==0){
  stop("The subset based on your statement has length 0! Stopping...")
}

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
