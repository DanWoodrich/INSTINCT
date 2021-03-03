#eventually should make a lot better by merging FD and Dets to find paths. 
#can steal logic from feature-extract-heavy

#load in FG if want to make this generalizable to different source data locations. 

labels= 'n' 

probs='y'

Detspath="//akc0ss-n086/NMML_CAEP_Acoustics/Detector/INSTINCT/"
DetsName="DETwProbs"

#load in FG.csv
Outpath<-"C:/Apps/instinct_dt/Out/"
Dets<-read.csv(paste(Detspath,"/",DetsName,".csv.gz",sep=""))

Dets<-Dets[order(Dets$StartFile,Dets$StartTime),]

Mooring<-"BS13_AU_02a"

Dets$StartFile<-paste("C:/Apps/instinct_dt/Data/SoundFiles",Mooring,Dets$StartFile,sep="/")
Dets$EndFile<-paste("C:/Apps/instinct_dt/Data/SoundFiles",Mooring,Dets$EndFile,sep="/")


if(labels=='n'){
#raven format: 
Detsraven<-data.frame(1:nrow(Dets),"Spectrogram 1",1,Dets$StartTime,Dets$EndTime,Dets$LowFreq,Dets$HighFreq,Dets$StartFile,Dets$EndFile,Dets$StartTime,Dets$EndTime-Dets$StartTime)
colnames(Detsraven)<- c("Selection","View","Channel","Begin Time (s)","End Time (s)","Low Freq (Hz)","High Freq (Hz)","Begin Path","End Path","File Offset","Delta Time")
}else{
  
  if("Label" %in% colnames(Dets)){
    colnames(Dets)[colnames(Dets)=="Label"]<-'label'
  }
  Detsraven<-data.frame(1:nrow(Dets),"Spectrogram 1",1,Dets$StartTime,Dets$EndTime,Dets$LowFreq,Dets$HighFreq,Dets$StartFile,Dets$EndFile,Dets$StartTime,Dets$EndTime-Dets$StartTime,Dets$label)
  colnames(Detsraven)<- c("Selection","View","Channel","Begin Time (s)","End Time (s)","Low Freq (Hz)","High Freq (Hz)","Begin Path","End Path","File Offset","Delta Time","label")
  
}

if(probs=="y"){
  Detsraven$probs<-Dets$probs
}


write.table(Detsraven,paste(Outpath,"RavenOutput2.txt",sep="/"),quote=FALSE,sep = "\t",row.names=FALSE)

