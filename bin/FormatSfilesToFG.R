#convert Sfiles_and_durations to FG format: 
#this is a simple convenience script that could be recreated to work with any lab metadata formats. Not meant to be bugproof or 
#work for a variety of formats 

#3/3/20:
#modify so it fits the NAS naming requirements (month subfolders)
source(paste("C:/Apps/INSTINCT/bin/instinct_fxns.R",sep="")) 

SpeciesDo<-"y"
Species<-"RW"
Decimate<-"No_whiten_decimate_by_16"
DecimateShort<-"decimate_by_16"

if(SpeciesDo=='y'){
  folderName<-paste("//akc0ss-n086/NMML_CAEP_Acoustics/Detector/Combined_sound_files",Species,Decimate,sep="/")
}else{
  folderName<-paste("//akc0ss-n086/NMML_CAEP_Acoustics/Detector/Combined_sound_files",Decimate,sep="/")
}

#find .csv files in folder
files<-dir(folderName,pattern=".csv")
transferSF<-"n"

#files<-c("AW14_AU_PH1_files_All_SFiles_and_durations.csv","AL18_AU_BS4_files_All_SFiles_and_durations.csv","AL16_AU_BS1_files_All_SFiles_and_durations.csv",
#         "BS17_AU_05a_files_All_SFiles_and_durations.csv","BS17_AU_08a_files_All_SFiles_and_durations.csv")

files<-dir(folderName)[grep(".csv",dir(folderName))]

#just for RW, and temporary
files<-files[1:12]

for(n in files){

SfilesName<-n

data<-read.csv(paste(folderName,SfilesName,sep="/"))

data$SFsh<-gsub("_", "-", data$SFsh)

dash2<-gregexpr("-",data$SFsh[1])[[1]][2]
dot1<-gregexpr("\\.",data$SFsh[1])[[1]][1]
dateTimeFormat<-substr(data$SFsh,dash2+1,dot1-1)

#add in a segstart column to save the start time 

if(!any(!nchar(dateTimeFormat)==15)){ #split this conditional into 2 parts, one that strips the yy and one that tests for non-000 files
  dateTimeFormat<-substr(dateTimeFormat,3,15)
  #  dateTimeFormat<-paste(substr(dateTimeFormat,3,12),"000",sep="")

}

#find the time difference from file name to 000. Save this as a vector in SegStart. 

mss<-paste("0",substr(dateTimeFormat,11,15),sep="")
vals<-as.POSIXlt(mss,format="%M%S")
vals<- vals$sec+vals$min*60

data$SegStart<-vals
data$SegDur<-data$Duration

#Then, convert all dateTimeFormat to 000

dateTimeFormat<-paste(substr(dateTimeFormat,1,10),"000",sep="")

data$SFsh<-paste(substr(data$SFsh,1,dash2),dateTimeFormat,".wav",sep="")

und3<-gregexpr("_",data$MooringName[1])[[1]][2]
siteID<-substr(data$MooringName,und3+1,nchar(as.character(data$MooringName[1])))

#calculate month for NAS pathing
#format mm_yyyy

year<-paste("20",substr(dateTimeFormat,1,2),sep="")
month<-substr(dateTimeFormat,3,4)

#for now, quick and dirty mooring rename. Replace when lookup table is available

#also, rename wavs if applicable:
if(substr(data$SFsh[1],1,5)=="AU-BS"){
  
  MooringSite<-substr(data$SFsh[1],6,7)

  #if a BS mooring, find the #, then 
  newWavName<-paste("AU-BSPM",MooringSite,sep="")
  
  MooringName<-paste(substr(data$MooringName[1],1,8),"PM",MooringSite,sep="")
  
  if((substr(data$SFsh[1],8,8)=="a"|substr(data$SFsh[1],8,8)=="b")&MooringSite=="02"){
    newWavName<-paste(newWavName,substr(data$SFsh[1],8,8),sep="_")
    
    MooringName<-paste(MooringName,substr(data$SFsh[1],8,8),sep="-")
  }
  
  data$SFsh<-paste(newWavName,"-",dateTimeFormat,".wav",sep="")
  
  data$MooringName<-MooringName
}

if(substr(data$SFsh[1],1,5)=="AU-AL"|substr(data$SFsh[1],1,5)=="AU-AW"){
  
  #do not need to change .wav names
  
  #only works with single digit, but that's ok I think just for backwards conversion of this limited set
  MooringSite<-substr(data$MooringName[1],11,12)
  
  #if a BS mooring, find the #, then 
  MooringName<-paste(substr(data$MooringName[1],1,10),"0",MooringSite,sep="")
  
  data$MooringName<-MooringName
}

NASpath<-paste("/",data$MooringName,"/",month,"_",year,"/",sep="")

outData<-data.frame(cbind(as.character(data$SFsh),NASpath,dateTimeFormat,data$Duration,as.character(data$MooringName),data$SegStart,data$SegDur,siteID))
colnames(outData)<-c("FileName","FullPath","StartTime","Duration","Deployment","SegStart","SegDur","SiteID")

#modify saved name as well 
und5<-gregexpr("_",SfilesName)[[1]][5]
saveName<-substr(SfilesName,gregexpr("_",SfilesName)[[1]][3],und5-1)

saveName<-paste(data$MooringName[1],saveName,sep="")

#before saving, need to loop through each file, call readwave2 to get duration. Then, remove redunant rows. 

#need to edit this: could combine into fewer segments, but should work fine without this step 
#outData<-outData[which(!duplicated(outData$FileName)),]

NASpath = "//161.55.120.117/NMML_AcousticsData/Audio_Data/Waves"

#this will be file duration, but save original duration as 'segDur'. 
pathsave=""
for(i in 1:nrow(outData)){
  path = paste(NASpath,outData[i,"FullPath"],outData[i,"FileName"],sep="")
  if(path!=pathsave){
    info<-readWave2(path,header = TRUE)
    outData[i,"Duration"]<-round(info$samples/info$sample.rate)
  }else{
    outData[i,"Duration"]<-round(info$samples/info$sample.rate)
  }
  
  pathsave=path
  
}

write.csv(outData,paste("C:/Apps/INSTINCT/Data/FileGroups/",saveName,".csv",sep=""),row.names = FALSE)










#depreciated

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
  
  path<- paste("C:/Apps/INSTINCT/Data/SoundFiles/",mooringFolder,sep="")
  
  dir.create(path)
  file.copy(filesFullp,path)
  
}



}
