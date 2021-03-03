MethodID<-"perf-report-simple-v1-0"

args<-commandArgs(trailingOnly = TRUE)

#EDstatsPath<-"C:/instinct_dt/Cache/07592b6298d81eb10f5044f0bb9a8d8a58fc9d31"
#ModelstatsPath<-"C:/instinct_dt/Cache/de2974684d7d4bdc86c66533bcdf4c3e6635321f"
#ModelvisPath<- "C:/instinct_dt/Cache/9b83e604c10167e92547431790a3008f3db64157/a321a8f4c3abd3c2781be01ca92aff16fcd6f07f"
#FGVisPaths<-"C:/instinct_dt/Cache/2bf717aef81044ab2e655e4351d4342cc08de10a/8a757bbec32b5b6a56bc98ac2748d2df64deaa0f/2cc09ef5daa22a410caf96a2b15d10537bd1a4b5/9b83e604c10167e92547431790a3008f3db64157/a321a8f4c3abd3c2781be01ca92aff16fcd6f07f,C:/instinct_dt/Cache/4447ee5580b7e09998ced8194e868db6be2e21dc/8a757bbec32b5b6a56bc98ac2748d2df64deaa0f/2cc09ef5daa22a410caf96a2b15d10537bd1a4b5/9b83e604c10167e92547431790a3008f3db64157/a321a8f4c3abd3c2781be01ca92aff16fcd6f07f,C:/instinct_dt/Cache/290c766b4f8e1dbb3e5a962dee042855be3df37a/8a757bbec32b5b6a56bc98ac2748d2df64deaa0f/2cc09ef5daa22a410caf96a2b15d10537bd1a4b5/9b83e604c10167e92547431790a3008f3db64157/a321a8f4c3abd3c2781be01ca92aff16fcd6f07f,C:/instinct_dt/Cache/0858b6a64633adcb3b6170fdb419a5a30e116804/8a757bbec32b5b6a56bc98ac2748d2df64deaa0f/2cc09ef5daa22a410caf96a2b15d10537bd1a4b5/9b83e604c10167e92547431790a3008f3db64157/a321a8f4c3abd3c2781be01ca92aff16fcd6f07f,C:/instinct_dt/Cache/ca28e08bde8474d6a004b5acbd2644b187061f03/8a757bbec32b5b6a56bc98ac2748d2df64deaa0f/2cc09ef5daa22a410caf96a2b15d10537bd1a4b5/9b83e604c10167e92547431790a3008f3db64157/a321a8f4c3abd3c2781be01ca92aff16fcd6f07f,C:/instinct_dt/Cache/c3dfe4b64950e42d7f6f6c546446b5e8e4f73e42/8a757bbec32b5b6a56bc98ac2748d2df64deaa0f/2cc09ef5daa22a410caf96a2b15d10537bd1a4b5/9b83e604c10167e92547431790a3008f3db64157/a321a8f4c3abd3c2781be01ca92aff16fcd6f07f,C:/instinct_dt/Cache/491ed9da5575918b03e04d73402145b99dd3fec0/8a757bbec32b5b6a56bc98ac2748d2df64deaa0f/2cc09ef5daa22a410caf96a2b15d10537bd1a4b5/9b83e604c10167e92547431790a3008f3db64157/a321a8f4c3abd3c2781be01ca92aff16fcd6f07f,C:/instinct_dt/Cache/51ee31b15b31aa23204ffb3b200d4c2d7f2f44c7/8a757bbec32b5b6a56bc98ac2748d2df64deaa0f/2cc09ef5daa22a410caf96a2b15d10537bd1a4b5/9b83e604c10167e92547431790a3008f3db64157/a321a8f4c3abd3c2781be01ca92aff16fcd6f07f"
#FGIDs<- "AL16_AU_BS3_files_152-354.csv,AL16_AU_BS3_files_59-114.csv,BS13_AU_02a_files_343-408.csv,BS13_AU_02a_files_38-122.csv,BS13_AU_02a_files_510-628.csv,BS14_AU_04_files_189-285.csv,BS14_AU_04_files_304-430.csv,BS14_AU_04_files_45-188.csv"
#resultPath<-"C:/instinct_dt/Outputs//ModelPerfEval/"

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
