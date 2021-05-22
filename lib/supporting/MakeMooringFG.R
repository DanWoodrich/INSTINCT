#pseudocode
#given a mooring name
#find the month folder
#loop through each month folder and save file names, path, 
#read header of each file and get duration, 

source(paste("C:/Apps/INSTINCT/lib/supporting/instinct_fxns.R",sep="")) 

#all DFO moorings:
MooringNames<-"XB06_PA_US01,XB17_AM_AP01,XB15_AM_BL01,XB11_AU_BW01-a,XB11_AU_BW01-b,XB12_AU_BW01,XB18_AU_BB01,XB11_AU_BP01,XB13_AU_BP01,XB18_AM_CS01,XB19_AM_CS01,XB14_AM_DK01,XB18_AM_DS01,XB17_AM_ES01,XB19_AM_ES01,XB15_AU_LB01,XB17_AU_LB01,XB10_AU_LI01,XB13_AU_LI01,XB17_AM_OG01,XB18_AM_OG01,XB19_AM_OG01,XB17_AM_PR01,XB18_AM_PL01,XB12_AU_SS01,XB11_AU_TI01,XB12_AU_TI01"
MooringNames<-strsplit(MooringNames,",")[[1]]

#don't redo existing ones: -6, -25, 

#MooringNames<-MooringNames[c(-6,-25)]

MooringNames<-MooringNames[10:length(MooringNames)]

for(n in 1:length(MooringNames)){

MooringName<-MooringNames[n]

siteInd<-gregexpr("_",MooringName)[[1]][length(gregexpr("_",MooringName)[[1]])]
site<-substr(MooringName,siteInd+1,nchar(MooringName))

NASpath = "//161.55.120.117/NMML_AcousticsData/Audio_Data/Waves"

path=paste(NASpath,MooringName,sep="/")
monthFoldersP<-paste(path,dir(path),sep="/")
monthFolders<-dir(path)

rows<-foreach(l=1:length(monthFolders)) %do% {
  FilesP<-paste(monthFoldersP[l],dir(monthFoldersP[l]),sep="/")
  Files<-dir(monthFoldersP[l])
  
  ind<-which(Files=="AM-XBCS01-200709-090001.wav")
  
  if(length(ind)>0){
    Files<-Files[-ind]
    FilesP<-FilesP[-ind]
  }

  out1<-foreach(p=1:length(FilesP)) %do% {
    #make the row
    filedeets<-readWave2(FilesP[p],header = TRUE)
    dur<-round(filedeets$samples/filedeets$sample.rate)
    
    #have a catch in here: if file is <20s, reject it (and spit out warning)
    if(dur<20){
      print("SF rejected: too short (<20s)")
      return(NULL)
    }
    
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

}
