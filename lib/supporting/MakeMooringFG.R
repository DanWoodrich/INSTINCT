#pseudocode
#given a mooring name
#find the month folder
#loop through each month folder and save file names, path, 
#read header of each file and get duration, 

source(paste("C:/Apps/INSTINCT/lib/supporting/instinct_fxns.R",sep="")) 

MooringName<-"XB12_AU_BW01"

siteInd<-gregexpr("_",MooringName)[[1]][length(gregexpr("_",MooringName)[[1]])]
site<-substr(MooringName,siteInd+1,nchar(MooringName))

NASpath = "//161.55.120.117/NMML_AcousticsData/Audio_Data/Waves"

path=paste(NASpath,MooringName,sep="/")
monthFoldersP<-paste(path,dir(path),sep="/")
monthFolders<-dir(path)

rows<-foreach(l=1:length(monthFolders)) %do% {
  FilesP<-paste(monthFoldersP[l],dir(monthFoldersP[l]),sep="/")
  Files<-dir(monthFoldersP[l])
  
  out1<-foreach(p=1:length(FilesP)) %do% {
    #make the row
    filedeets<-readWave2(FilesP[p],header = TRUE)
    dur<-round(filedeets$samples/filedeets$sample.rate)
    row<-c(Files[p],paste("/",MooringName,"/",monthFolders[l],"/",sep=""),substr(Files[p],nchar(Files[p])-16,nchar(Files[p])-4),dur,MooringName,0,dur,site)
    return(row)
  }
  
  out1<-do.call("rbind",out1)
  
  return(out1)
}

out2<-do.call("rbind",rows)

out2<-data.frame(out2)

colnames(out2)<-c("FileName","FullPath","StartTime","Duration","Deployment","SegStart","SegDur","SiteID")

out2<-out2[sort(order(out2$FileName)),]

out2$Duration<-as.numeric(out2$Duration)
out2$SegStart<-as.numeric(out2$SegStart)
out2$SegDur<-as.numeric(out2$SegDur)

write.csv(out2,paste("C:/Apps/INSTINCT/Data/FileGroups/",MooringName,"_files_All.csv",sep=""),row.names = FALSE)
