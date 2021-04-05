library(PRROC)
library(flux)

args<-commandArgs(trailingOnly = TRUE)

#dataPath<-"C:/instinct_dt/Cache/2bf717aef81044ab2e655e4351d4342cc08de10a/8a757bbec32b5b6a56bc98ac2748d2df64deaa0f/2cc09ef5daa22a410caf96a2b15d10537bd1a4b5/9b83e604c10167e92547431790a3008f3db64157"
#resultPath<-"C:/instinct_dt/Cache/9b83e604c10167e92547431790a3008f3db64157/testOut"
#PE1s2path<-"C:/instinct_dt/Cache/07592b6298d81eb10f5044f0bb9a8d8a58fc9d31"

dataPath<-args[1]
resultPath<-args[2]
PE1s2path<-args[3]
DataType<-args[4]


data<-read.csv(paste(dataPath,"DETwProbs.csv.gz",sep="/"))
PE1data<-read.csv(paste(PE1s2path,"Stats.csv.gz",sep="/"))

if(rootPath=="All"){
  EDrecDiff<-1-PE1data$Recall[nrow(PE1data)]
}else if(rootPath=="FG"){
  EDrecDiff<-1-PE1data$Recall[which(as.character(PE1data$FGID)==as.character(unique(data$FGID)))]
}

labelVec<-data$label=="TP"
labelVec[labelVec]<-1

data$label<-labelVec

png(paste(resultPath,"/PRcurve.png",sep=""),width=500,height = 500)
par(mgp=c(2.4,1,0),mar=c(4,4,0,0.5))
par(cex.main=2,cex.lab=2,cex.axis=1.5)

curve<-pr.curve(scores.class0=data$probs,weights.class0 = data$label,curve=TRUE)
curve$curve[,1]<-curve$curve[,1]-EDrecDiff

curve$curve<-curve$curve[which(curve$curve[,1]>=0),]

PRauc<-auc(curve$curve[,1],curve$curve[,2])

if(PRauc<0.80){

curveFortext_x<-curve$curve[,1]+max(curve$curve[,1]*0.075)
curveFortext_y<-curve$curve[,2]+max(curve$curve[,2]*0.03)

}else{
  curveFortext_x<-curve$curve[,1]-max(curve$curve[,1]*0.075)
  curveFortext_y<-curve$curve[,2]-max(curve$curve[,2]*0.03)
  
}


plot(curve, auc.main=FALSE, main ="",legend=FALSE,col="black",cex.axis=1.8)
#pointsOfInterest<-c(0.2,0.3,0.4,0.5,0.60,0.75,0.83,0.92,0.95,0.97,0.99)

pointsOfInterest<-c(0.2,0.3,0.4,0.5,0.6,0.75,0.85,0.95,0.99)

curve$curve[,3]<-round(curve$curve[,3],digits=2)

for(n in 1:length(pointsOfInterest)){
  x=curve$curve[median(which(curve$curve[,3]==pointsOfInterest[n])),1]
  y=curve$curve[median(which(curve$curve[,3]==pointsOfInterest[n])),2]
    
  xtxt=curveFortext_x[median(which(curve$curve[,3]==pointsOfInterest[n]))]
  ytxt=curveFortext_y[median(which(curve$curve[,3]==pointsOfInterest[n]))]
  
  graphics::text(xtxt,ytxt,labels=pointsOfInterest[n],cex=1.75)
  points(x,y,cex=1.75,lwd=2,pch=16)
}

legend(x=-0.025,y=0.19, 
       legend = c("Cutoff values"), #,round(minDist_CUT_Meanspec,digits=2)),round(CUTmeanspec,digits=2)
       col = c("black"), 
       pch = c(16), 
       bty = "n", 
       pt.cex = 3, 
       cex = 2, 
       text.col = "black", 
       horiz = F , 
       inset = c(0.1))

text(0.33,0.05,paste("Average precision =",round(PRauc,digits=2)),cex=2)

dev.off()



#total artifacts: 

#1. PR curve vector (allows for plotting different species against each other)
PRcurve<-data.frame(curve$curve)
colnames(PRcurve)<-c("Recall","Precision","Cutoff")
outName<-"PRcurve.csv.gz"
write.csv(PRcurve,gzfile(paste(resultPath,outName,sep="/")),row.names = FALSE)

#2. pr curve
#did above 
#3. perf stats (for now, only PR auc)
outName<-"PRcurve_auc.txt"
write.table(PRauc,paste(resultPath,outName,sep="/"),quote=FALSE,sep = "\t",row.names=FALSE,col.names=FALSE)

#retrieve later with: 
#text = readLines(paste(resultPath,outName,sep="/"))
