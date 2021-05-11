library(PRROC)
library(flux)


args<-"C:/Apps/INSTINCT/Cache//0d0dc380b1ee/5499ab C:/Apps/INSTINCT/Cache//0d0dc380b1ee/5499ab/998c0e C:/Apps/INSTINCT/Cache/4ed47b/6f7ebc FG"

args<-strsplit(args,split=" ")[[1]]

args<-commandArgs(trailingOnly = TRUE)

#dataPath<-"C:/Apps/INSTINCT/Cache/2f38f7440b5b/a04a78/04f178/813e20"
#resultPath<-"C:/Apps/INSTINCT/Cache/2f38f7440b5b/a04a78/04f178/813e20/53d3bb"
#PE1s2path<-"C:/Apps/INSTINCT/Cache/2f38f7440b5b/a04a78/04f178/813e20/03bc3c/c393b4/6c4546"

dataPath<-args[1]
resultPath<-args[2]
PE1s2path<-args[3]
DataType<-args[4]


data<-read.csv(paste(dataPath,"DETx.csv.gz",sep="/"))
PE1data<-read.csv(paste(PE1s2path,"Stats.csv.gz",sep="/"))

if(DataType=="All"){
  EDrecDiff<-1-PE1data$Recall[nrow(PE1data)]
}else if(DataType=="FG"){
  #see if FGID even being tracked yet
  if("FGID" %in% colnames(data)){
    EDrecDiff<-1-PE1data$Recall[which(as.character(PE1data$FGID)==as.character(unique(data$FGID)))]
  }else{ #if not, assume it is the correct one
    EDrecDiff<-1-PE1data$Recall
  }
}

#drop FN labels. 
data<-data[which(data$label %in% c("TP","FP")),]

#if still has species code labels, drop them

if("SignalCode" %in% colnames(data)){
  data<-data[which(data$SignalCode=="out"),]
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


plot(curve, auc.main=FALSE, main ="",legend=FALSE,col="black",cex.axis=1.8)
#pointsOfInterest<-c(0.2,0.3,0.4,0.5,0.60,0.75,0.83,0.92,0.95,0.97,0.99)



if(!is.na(curve$auc.integral)){
  
PRauc<-auc(curve$curve[,1],curve$curve[,2])

if(PRauc<0.80){

curveFortext_x<-curve$curve[,1]+max(curve$curve[,1]*0.075)
curveFortext_y<-curve$curve[,2]+max(curve$curve[,2]*0.03)

}else{
  curveFortext_x<-curve$curve[,1]-max(curve$curve[,1]*0.075)
  curveFortext_y<-curve$curve[,2]-max(curve$curve[,2]*0.03)
  
}



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

}else{
  PRauc<-NA
  text(0.65,0.6,paste("No True Positives"),cex=2)
  
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
