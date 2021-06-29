#load in FG, subset ANALYSIS data file to that FG, and export as format GT with i_neg_SC labels. 

#define mooring

Mooring<-"BS17_AU_PM02-b"

#load in FG

FG<-read.csv(paste("C:/Apps/INSTINCT/Data/FileGroups/",Mooring,"_files_All.csv",sep=""))

#load in ANALYSIS tab

ANALYSIStab<-read.csv("//nmfs/akc-nmml/CAEP/Acoustics/ANALYSIS/RWupcallYeses4Dan.csv")

#mandate column names to fit standard (column names changed on Cath end)
colnames(ANALYSIStab)[1:7]<-c("Wavefile","StartSecInWav","EndSecInWav","MooringSite","MooringDeployID","StartFieldTimeUTC","EndFieldTimeUTC")


#subset ANALYSIS tab to mooring ID, 

GT<-ANALYSIStab[which(ANALYSIStab$MooringDeployID==Mooring),]


#format GT to standard