#do the pull for Cole
#LM negatives: one pull from each month, each mooring, and each year. 3 hours each. 

#no's
data<-read.csv("//nmfs/akc-nmml/CAEP/Acoustics/ANALYSIS/LowMoanNos_1.csv")

#steph wants a round of random sampling (522)
#and, a pull from the yeses

#5 hour chunks for nos. 10 hour chunks for yeses (break into 5 hours to disguise it in with the no's for Cole)

#Do the same for pulling positives. 1 every month/year/site

#there is a wonky row. Get rid of it. 
data<-data[-119400,]

data$StartFieldTimeUTC<-as.POSIXct(data$StartFieldTimeUTC,format="%d-%b-%Y %H:%M:%S",tz="UTC")
data$StartDateTime<-as.character(data$StartFieldTimeUTC)

#give this thing more queryable columns: month and year

data$year<-as.character(format(data$StartFieldTimeUTC,"%Y"))
data$month<-as.character(format(data$StartFieldTimeUTC,"%m"))

#how many unique
data$sym<-paste(data$MooringSite,data$year,data$month)

data$dur<-as.numeric(data$EndSecInWav)-as.numeric(data$StartSecInWav)

data<-data[sample(1:nrow(data),nrow(data)),]

data$sampleID<-1:nrow(data)

#there are 522 unique sym. so, to get to 15% of data, probably best to do different rounds of pulls
#each round will sample 3hrs from each sym. Then at the end, shuffle them. 
#based on the sampleID

dataIn<-data

#doing 6 pulls from nos, in 5 hours. 
out<-foreach(i=1:6) %do% {
  #do 522 random pulls of 5 hr chunks. 
  out2<-foreach(p=1:length(unique(dataIn$sym))) %do% {
    dataMini<-dataIn[which(dataIn$sym==unique(dataIn$sym)[p]),]
    pullInd<-sample(1:nrow(dataMini),1)
    #expand outwards, stop when hit 
  }
  
}
