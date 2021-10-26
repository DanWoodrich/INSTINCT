MethodID<-"perf-report-simple-v1-0"

args<-"C:/Apps/INSTINCT/Cache/4ed47b/6f7ebc C:/Apps/INSTINCT/Outputs/ModelPerfEval/66a753 C:/Apps/INSTINCT/Cache/1af01a/5499ab/998c0e C:/Apps/INSTINCT/Outputs/ModelPerfEval/66a753 C:/Apps/INSTINCT/Cache//0d0dc380b1ee/5499ab/998c0e,C:/Apps/INSTINCT/Cache//fdb2261ed7fd/5499ab/998c0e,C:/Apps/INSTINCT/Cache//eedec80a4499/5499ab/998c0e,C:/Apps/INSTINCT/Cache//daf9e0046b12/5499ab/998c0e,C:/Apps/INSTINCT/Cache//ff536027d17e/5499ab/998c0e,C:/Apps/INSTINCT/Cache//830000c84c1e/5499ab/998c0e,C:/Apps/INSTINCT/Cache//4b78a778998c/5499ab/998c0e,C:/Apps/INSTINCT/Cache//181ce5ead182/5499ab/998c0e,C:/Apps/INSTINCT/Cache//7c7adf8c8761/5499ab/998c0e,C:/Apps/INSTINCT/Cache//2f6b36609bc0/5499ab/998c0e AL16_AU_BS01_files_All.csv,AW12_AU_BS03_files_All.csv,AW14_AU_BS02_files_All.csv,AW14_AU_BS03_files_1-160.csv,BS13_AU_PM02-a_files_All.csv,BS13_AU_PM04_files_All.csv,BS14_AU_PM04_files_All.csv,BS15_AU_PM02-a_files_1-104.csv,BS15_AU_PM02-b_files_All.csv,BS15_AU_PM04_files_301-417.csv"

args<-strsplit(args,split=" ")[[1]]

args<-commandArgs(trailingOnly = TRUE)

EDstatsPath<-args[1]
ModelstatsPath<-args[2]
ModelvisPath<-args[3]
resultPath<-args[4]
FGVisPaths<-args[5]
FGIDs<-args[6]

#this is going to be the simplest possible version of the script: combine the datasets into a csv, and just copy the images
#into a single folder (variables in file name)


FGVisPaths<-strsplit(FGVisPaths,",")[[1]]
FGIDs<-strsplit(FGIDs,",")[[1]]

FGIDs<-substr(FGIDs,1,nchar(FGIDs)-4)

#Make quick FG data table: 

FGdataQuick<-data.frame(FGIDs)

FGdataQuick$AveragePrecision<-NA

for(i in 1:length(FGVisPaths)){
  FGdataQuick$AveragePrecision[i]<-read.table(paste(FGVisPaths[i],'PRcurve_auc.txt',sep="/"))[1,]
}


EDstats<-read.csv(paste(EDstatsPath,"/Stats.csv.gz",sep=""))
Modelstats<-read.csv(paste(ModelstatsPath,"/Stats.csv.gz",sep=""))

EDstats$Stage<-"ED"
EDstats$AveragePrecision<-NA
Modelstats$Stage<-"AM"
Modelstats$AveragePrecision<-c(FGdataQuick$AveragePrecision,read.table(paste(ModelvisPath,'PRcurve_auc.txt',sep="/"))[1,])

Stats<-rbind(EDstats,Modelstats)

#move visualizations
for(i in 1:length(FGVisPaths)){
  file.copy(from=paste(FGVisPaths[i],'PRcurve.png',sep="/"),to=paste(resultPath,'/',FGIDs[i],'.png',sep=""))
}

file.copy(from=paste(ModelvisPath,'PRcurve.png',sep="/"),to=paste(resultPath,'/','All.png',sep=""))


write.csv(Stats,paste(resultPath,"FullStats.csv",sep="/"),row.names = FALSE)
