#load in FG, subset ANALYSIS data file to that FG, and export as format GT with i_neg_SC labels. 

source("C:/Apps/INSTINCT/lib/supporting/instinct_fxns.R") 

#define mooring

Mooring<-"BS17_AU_PM02-a"

#load in FG

FG<-read.csv(paste("C:/Apps/INSTINCT/Data/FileGroups/",Mooring,"_files_All.csv",sep=""))

#load in ANALYSIS tab

ANALYSIStab<-read.csv("//nmfs/akc-nmml/CAEP/Acoustics/ANALYSIS/HumpbackYeses_2.csv")

#mandate column names to fit standard (column names changed on Cath end)
colnames(ANALYSIStab)[1:7]<-c("Wavefile","StartSecInWav","EndSecInWav","MooringSite","MooringDeployID","StartFieldTimeUTC","EndFieldTimeUTC")


#subset ANALYSIS tab to mooring ID, 

GT<-ANALYSIStab[which(ANALYSIStab$MooringDeployID==Mooring),]

GTnew<-data.frame(GT$StartSecInWav,GT$EndSecInWav,0,800,getFileName(GT$Wavefile),getFileName(GT$Wavefile),"y","SC","HB")
colnames(GTnew)<-c("StartTime","EndTime","LowFreq","HighFreq","StartFile",	"EndFile",	"label",	"Type",	"SignalCode")

write.csv(GTnew,"C:/Apps/INSTINCT/Data/GroundTruth/HB/HB_BS17_AU_PM02-a_files_All.csv",row.names = FALSE)


#format GT to standard