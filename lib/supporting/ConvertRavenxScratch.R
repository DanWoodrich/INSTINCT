#convert Raven.txt scratch

dat<-read.delim("//akc0ss-n086/NMML_CAEP_Acoustics/Detector/Datasets_transfer/RAVENxCole.txt")

dat$Begin.Path<-paste("\\\\161.55.120.117\\NMML_AcousticsData\\Audio_Data\\DecimatedWaves\\512",substr(dat$Begin.Path,28,nchar(dat$Begin.Path)),sep="")
dat$End.Path<-paste("\\\\161.55.120.117\\NMML_AcousticsData\\Audio_Data\\DecimatedWaves\\512",substr(dat$End.Path,28,nchar(dat$End.Path)),sep="")

for(n in 1:nrow(dat)){
  dat$Begin.Path[n]<-gsub("\\\\", "/",dat$Begin.Path[n])
  dat$End.Path[n]<-gsub("\\\\", "/",dat$End.Path[n])
}


colnames(dat)[1:11]<-c("Selection","View","Channel","Begin Time (s)","End Time (s)","Low Freq (Hz)","High Freq (Hz)",
                       "Begin Path","End Path","File Offset (s)","Delta Time (s)")

write.table(dat,"//akc0ss-n086/NMML_CAEP_Acoustics/Detector/LF moan project/RAVENxColeJune30.txt",quote=FALSE,sep = "\t",row.names=FALSE)
