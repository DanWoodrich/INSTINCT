#convert Sfiles_and_durations to FG format: 
#this is a simple convenience script that could be recreated to work with any lab metadata formats. Not meant to be bugproof or 
#work for a variety of formats 

SpeciesDo<-"n"
Species<-"LM"
Decimate<-"No_whiten_decimate_by_128"
DecimateShort<-"decimate_by_128"

if(SpeciesDo=='y'){
  folderName<-paste("//akc0ss-n086/NMML_CAEP_Acoustics/Detector/Combined_sound_files",Species,Decimate,sep="/")
}else{
  folderName<-paste("//akc0ss-n086/NMML_CAEP_Acoustics/Detector/Combined_sound_files",Decimate,sep="/")
}

#find .csv files in folder
files<-dir(folderName,pattern=".csv")
transferSF<-"y"

files<-c("AW14_AU_PH1_files_All_SFiles_and_durations.csv","AL18_AU_BS4_files_All_SFiles_and_durations.csv","AL16_AU_BS1_files_All_SFiles_and_durations.csv",
         "BS17_AU_05a_files_All_SFiles_and_durations.csv","BS17_AU_08a_files_All_SFiles_and_durations.csv")

for(n in files){

SfilesName<-n

data<-read.csv(paste(folderName,SfilesName,sep="/"))

dash2<-gregexpr("-",data$SFsh[1])[[1]][2]
dot1<-gregexpr("\\.",data$SFsh[1])[[1]][1]
dateTimeFormat<-substr(data$SFsh,dash2+1,dot1-1)

und3<-gregexpr("_",data$MooringName[1])[[1]][2]
siteID<-substr(data$MooringName,und3+1,nchar(as.character(data$MooringName[1])))

outData<-cbind(as.character(data$SFsh),paste("/",as.character(data$MooringName),"/",sep=""),dateTimeFormat,data$Duration,as.character(data$MooringName),siteID)
colnames(outData)<-c("FileName","FullPath","StartTime","Duration","Deployment","SiteID")

und5<-gregexpr("_",SfilesName)[[1]][5]
saveName<-substr(SfilesName,1,und5-1)

write.csv(outData,paste("C:/Apps/instinct_dt/Data/FileGroups/",saveName,".csv",sep=""),row.names = FALSE)

#not going to be relevant later, but transfer sound files along with renaming. 

if(transferSF=="y"){
  
  mooringFolder<-as.character(data$MooringName[1])
  
  if(SpeciesDo=="y"){
    mooringFolderFull<-paste("//akc0ss-n086/NMML_CAEP_Acoustics/Detector/HG_datasets/",mooringFolder,"/",Species,"_yesUnion/",saveName,"_",DecimateShort,"/",sep="")
  }else{
    mooringFolderFull<-paste("//akc0ss-n086/NMML_CAEP_Acoustics/Detector/Full_datasets/",mooringFolder,"/",saveName,"_",DecimateShort,"/",sep="")
  }
  
  #Filestart<-as.numeric(substr(saveName,gregexpr("_",saveName)[[1]][4]+1,gregexpr("-",saveName)[[1]][1]-1))
  #Fileend<-as.numeric(substr(saveName,gregexpr("-",saveName)[[1]][1]+1,nchar(saveName)))
  
  filesFullp<-paste(mooringFolderFull,dir(mooringFolderFull,pattern=".wav"),sep="") #[Filestart:Fileend]
  
  path<- paste("C:/Apps/instinct_dt/Data/SoundFiles/",mooringFolder,sep="")
  
  dir.create(path)
  file.copy(filesFullp,path)
  
}



}
