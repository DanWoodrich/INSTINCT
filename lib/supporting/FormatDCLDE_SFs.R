#one off, reformat sound files names from DCLDE2013: 

path<-choose.dir()

subfolders<-dir(path)

for(i in 1:length(subfolders)){
  path2<-paste(path,subfolders[i],sep="\\")
  files<-dir(path2)
  for(n in 1:length(files)){
    newname<-gsub("_","-",files[n])
    newname<-paste(substr(newname,1,10),substr(newname,13,nchar(newname)),sep="") #this is not general, only to DCDLE RW
    file.rename(paste(path2,files[n],sep="\\"),paste(path2,newname,sep="\\"))
  }
}
