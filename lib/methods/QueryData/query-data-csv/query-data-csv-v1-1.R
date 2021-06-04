MethodID<-"query-data-csv-v1-0"

library(sqldf)

#cant do it this way unfortunately, with the internal quotes. maybe can find a better way eventually..
args="//nmfs/akc-nmml/CAEP/Acoustics/ANALYSIS/RWupcallYeses4Dan.csv C:/Apps/INSTINCT/Data/FileGroups //161.55.120.117/NMML_AcousticsData/Audio_Data testFG.csv \"StartDateTime > '2016-10-04 21:13:45' AND StartDateTime < '2017-10-10 21:13:45' AND MoorSite = 'BS01' LIMIT 150\" query-data-csv-v1-0"

args<-strsplit(args,split=" ")[[1]]

datapath <- "//nmfs/akc-nmml/CAEP/Acoustics/ANALYSIS/LMyeses_2.csv"
resultPath<- "C:/Apps/INSTINCT/Data/FileGroups"

SfRoot<- "//161.55.120.117/NMML_AcousticsData/Audio_Data"
fileName <- "LMyes.csv" 
statement <- "StartDateTime > '2016-10-04 21:13:45' AND StartDateTime < '2017-10-10 21:13:45' AND MoorSite = 'BS01' LIMIT 150"

args<-commandArgs(trailingOnly = TRUE)

datapath <- args[1]
resultPath<- args[2]

SfRoot<- args[3]
fileName <- args[4]
statement <- args[5]


source(paste("C:/Apps/INSTINCT/lib/supporting/instinct_fxns.R",sep="")) 

#datapath<-"//nmfs/akc-nmml/CAEP/Acoustics/ANALYSIS/RWupcallYeses4Dan.csv" #change to local folder
#datapath<-"//nmfs/akc-nmml/CAEP/Acoustics/ANALYSIS/RWgunshots4Dan.csv"   #change to local folder 

data<-read.csv(datapath)

data$StartFieldTimeUTC<-as.POSIXct(data$StartFieldTimeUTC,format="%d-%b-%Y %H:%M:%S",tz="UTC")
data$StartDateTime<-as.character(data$StartFieldTimeUTC)

#give this thing more queryable columns: month and year

data$year<-as.character(format(data$StartFieldTimeUTC,"%Y"))
data$month<-as.character(format(data$StartFieldTimeUTC,"%m"))


#statement<-"StartDateTime > '2016-10-04 21:13:45' AND StartDateTime < '2017-10-10 21:13:45' AND MoorSite = 'BS01' LIMIT 150"
#statement<-"month = 10"

#enforce keeping all of the columns, and starting with Where 
statement<-paste("SELECT * FROM data Where ",statement)

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
