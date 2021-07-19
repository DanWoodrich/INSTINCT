#load in FG, subset ANALYSIS data file to that FG, and export as format GT with i_neg_SC labels. 

library(R.matlab)


source("C:/Apps/INSTINCT/lib/supporting/instinct_fxns.R") 

#define mooring

Mooring<-"BS17_AU_PM02-a"

#load in FG

FG<-read.csv(paste("C:/Apps/INSTINCT/Data/FileGroups/",Mooring,"_files_All.csv",sep=""))

##########################
#load in ANALYSIS tab

ANALYSIStab<-read.csv("//nmfs/akc-nmml/CAEP/Acoustics/ANALYSIS/HumpbackYeses_2.csv")

#mandate column names to fit standard (column names changed on Cath end)
colnames(ANALYSIStab)[1:7]<-c("Wavefile","StartSecInWav","EndSecInWav","MooringSite","MooringDeployID","StartFieldTimeUTC","EndFieldTimeUTC")

###########################

#above is normally how I would do it- however, just loading straight from pngresults for now since the labels in the prior 
#output were combined. 

library(R.matlab)
library(foreach)

Mooringpath="//nmfs/akc-nmml/CAEP/Acoustics/ANALYSIS/BS17_AU_02a/"
prefix="PNGrslts_BS17_AU_02a_check0"

MooringMid=paste(Mooringpath,prefix,"2.mat",sep="")

Mids<-readMat(MooringMid)

daysInMoor<-length(Mids$fileMTX)

Pngs<-foreach(i=1:daysInMoor) %do% {
  return(Mids$fileMTX[[i]][[1]][,1])
}

PngsLen<-foreach(i=1:daysInMoor) %do% {
  lenPngs<-length(Mids$fileMTX[[i]][[1]][,1])
  return(lenPngs)
}

Pngs<-do.call("c",Pngs)
PngsLen<-do.call("c",PngsLen)

numspec<-length(Mids$PNGrslts.MetaData[,,2]$CheckSpp[,1])

specs<-foreach(j=1:numspec) %do% {
  spec<-foreach(i=1:daysInMoor) %do% {
    return(Mids$resltMTX[1:PngsLen[i],i,j])
  }
  
  spec<-do.call("c",spec)
  return(spec)
}

specs<-do.call("cbind",specs)

#do this later
#Data$FileName<-fileNameFromPng(Data$Pngs)

dash2<-gregexpr("-",Pngs)[[1]][2]
endDT<-dash2+13

datetime<-substr(Pngs,dash2+1,endDT)
datetime<-as.POSIXct(datetime,format="%y%m%d-%H%M%S",tz='UTC')

Data<-data.frame(Pngs,datetime,specs) 
#need to populate a 

colnames(Data)[3:ncol(Data)]<-trimws(Mids$PNGrslts.MetaData[,,2]$CheckSpp[,1])

#png names are old names. Need to convert to new names, get wav file, GT form (Sf names, seg start/dur)
#to find end time, just grab start time of next. For the files between duty cycle, catch high values and 
#read header of wav file to find true end time. Ughhhhh

#subset ANALYSIS tab to mooring ID, 

GT<-ANALYSIStab[which(ANALYSIStab$MooringDeployID==Mooring),]

GTnew<-data.frame(GT$StartSecInWav,GT$EndSecInWav,0,800,getFileName(GT$Wavefile),getFileName(GT$Wavefile),"y","SC","HB")
colnames(GTnew)<-c("StartTime","EndTime","LowFreq","HighFreq","StartFile",	"EndFile",	"label",	"Type",	"SignalCode")

write.csv(GTnew,"C:/Apps/INSTINCT/Data/GroundTruth/HB/HB_BS17_AU_PM02-a_files_All.csv",row.names = FALSE)




#format GT to standard

#now seeing that current exports don't have pngs retained. So don't even load from this, just load from the analysis .mat 
#with readMat. 