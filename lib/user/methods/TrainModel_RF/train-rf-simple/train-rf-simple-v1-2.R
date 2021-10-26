MethodID<-"train-rf-simple-v1-2"

#change: smaller model object from v1-0
#1-2: make a list of reserved colnames to remove from model training. 

#Use for model generation, CV, and model application. Bundles processes to ensure consistent behavior across applications. 
library(randomForest)
library(doParallel)


startLocalPar<-function(num_cores,...){
  
  cluz <<- parallel::makeCluster(num_cores)
  registerDoParallel(cluz)
  
  clusterExport(cluz, c(...))
  
}

splitdf <- function(
  #from flightcallr, now unavailable
  ### This will randomly split a data frame into training and test sets. I borrowed it from
  ### http://gettinggeneticsdone.blogspot.com/2011/02/split-data-frame-into-testing-and.html
  ### The caret packages has a similar facility using its createDataPartition() function.
  ### Perhaps even more appealing, there is sample.split() in caTools.
  dataframe, 
  weight = 2/3, 
  seed=NULL
) {
  if (!is.null(seed)) set.seed(seed)
  index <- 1:nrow(dataframe)
  trainindex <- sample(index, trunc(length(index) * weight))
  trainset <- dataframe[trainindex, ]
  testset <- dataframe[-trainindex, ]
  list(trainset=trainset,testset=testset)
  ### A list with two members; the training set and the test set
}

set.seed(5) 

#C:/Apps/instinct_dt/Cache/959a2bba685da47ccefa41672e393628115d14f8/TM_Intermediate1.csv.gz 
#C:/Apps/instinct_dt/Cache/959a2bba685da47ccefa41672e393628115d14f8/FG_Intermediate2.csv.gz 
#C:/Apps/instinct_dt/Cache/959a2bba685da47ccefa41672e393628115d14f8 
#NULL train 99 y 1 1 1 11 500 train-rf-simple-v1-0

#load in data
#dataPath<-"C:/instinct_dt/Cache/0522f89eae1c5b56b3f7154987991372bc63ad14/TM_Intermediate1.csv.gz"
#load in FG data 
#FGpath<-"C:/instinct_dt/Cache/da605819b1df78ca52d6309cb2a4fa7e0b772791/FG_Intermediate2.csv.gz"
#resultPath<-"C:/instinct_dt/Cache/d2bd878335a19577121f1e978da3ab00bbc4e558"

#load in params #don't care about CV and datasplit params for making single model. 

args<-"C:/Apps/INSTINCT_2/Cache/15825/46174/678589/828293//DETx.csv.gz C:/Apps/INSTINCT_2/Cache/15825/240386//FileGroupFormat.csv.gz C:/Apps/INSTINCT_2/Cache/15825/46174/678589/828293/960603 NULL CV 99 y 60 0.75 1 11 500 train-rf-simple-v1-2"

args<-strsplit(args,split=" ")[[1]]

args<-commandArgs(trailingOnly = TRUE)

dataPath<-args[1]
FGpath<-args[2]
resultPath<-args[3]
model_path<-args[4]

stage<-args[5]

#read in different params depending on the stage: Note this behavior would not work for docker- convert to just having a longer param 
#list with some 'dead' params. Easy enough to change over, this is simpler for now.

#one annoying this is that I have to reference different parameter locations if the param files are different. Probably better to 
#convert this to the format with 'dead' features. 

#Could make this process have multiple sections, and a 'general' section, which will help streamline param string. 

if(stage!="apply"){

crs<-as.integer(args[6])

balance_tpfp<-args[7]
cv_it<-as.integer(args[8])
cv_split<-as.numeric(args[9])
fp_multiple<-as.numeric(args[10])
mtry<-as.integer(args[11])
ntree<-as.integer(args[12])
}

#Here, make sure we are reading the right file. Doesn't care what
#luigi is really pointing to. Two name options currently, this 
#works the same on both. DETwFeatures.csv.gz & DETwFEwAL.csv.gz
data<-read.csv(dataPath)




#use then when building in functionality to read in metadata
#FGdata<-read.csv(FGpath)

#strip the FG data down to the metadata, combine with features 

featuresVecwL<-colnames(data)[!colnames(data) %in% c("StartTime","EndTime","LowFreq","HighFreq","StartFile","EndFile","probs")]

if(stage=="train"|stage=="CV"){


#reweight the dataset based on params


#put this in params in a future version:
#reweight to site aggresion. Value between 1-6, 1 is low change, 6 is reweight all to lowest site items
#reweight_to_site=n
#rwts_aggression=1
#reweight to year aggresion. Value between 1-6, 1 is low change, 6 is reweight all to lowest year items
#reweight_to_year=n
#rwty_aggression=1
#should come up with an algorithm to do this: something like total up s/y/m 'rarity' scores, preferentially keep the more 'rare' detections

#also in future version: 
#using 
#meta_for_model=SiteID,Season,Instrument #(all refer to a column value in FG data). 

#could also make the data manipulation steps its own process, and fit in before the model training. 

#split the dataset 

#stage 1: make a model. 

dataMod<-data[,featuresVecwL]

dataMod$Selection<-1:nrow(data)

if(stage=="CV"){
  probstab<-data.frame(dataMod$Selection)
  colnames(probstab)<-"Selection"
  
  totCors<-detectCores()
  if(crs>totCors){
    crs=totCors
  }
}else{
  crs<-1
  cv_it<-1
}

startLocalPar(crs,"splitdf","stage","dataMod","cv_it","cv_split","balance_tpfp","mtry","ntree")

probsAll<-foreach(i=1:cv_it,.packages = c("randomForest","doParallel")) %dopar% {
  
  if(stage=="CV"){
    dataTrain<-splitdf(dataMod,weight = cv_split)
    dataTest<-dataTrain[[2]]
    dataTrain<-dataTrain[[1]]
    
  }else{
    dataTrain<-dataMod
  }
  
  #do other weightings here 
  if(balance_tpfp=="y"){
    pos<-sum(dataTrain$label=="TP")
    neg<-sum(dataTrain$label=="FP")
    
    if(pos>neg){
      stop("ERROR: more positives than negatives. Did you mean to do that?")
    }
    
    ratio<-pos/neg
    neg<-splitdf(dataTrain[which(dataTrain$label=="FP"),],weight=ratio)[[1]]
    dataTrain<-rbind(dataTrain[which(dataTrain$label=="TP"),],neg)
  }
  
  dataTrain$label<-as.factor(dataTrain$label)
  
  #here, remove any possible unwanted columns in training data 
  possiblefields<-c('SignalCode','Analyst',"Comments","FGID","Selection") #some possible column reserved keywords
  if(any(possiblefields %in% colnames(dataTrain))){
    possiblefields <- possiblefields[which(possiblefields %in% colnames(dataTrain))]
    dataTrain<-dataTrain[,-which(names(dataTrain) %in% possiblefields)]
    if(stage=="CV"){
    dtSel<-dataTest$Selection
    dataTest<-dataTest[,-which(names(dataTest) %in% possiblefields)]
    #dataTest$label<-as.factor(dataTest$label) need this? 
    }
  }
  
  data.rf<-randomForest(formula=label ~ .,data=dataTrain,mtry=mtry,ntree=ntree) 
  
  #reduce size of model
  data.rf$finalModel$predicted <- NULL 
  data.rf$finalModel$oob.times <- NULL 
  data.rf$finalModel$y <- NULL
  data.rf$finalModel$votes <- NULL
  data.rf$control$indexOut <- NULL
  data.rf$control$index    <- NULL
  data.rf$trainingData <- NULL
  
  if(stage=="train"){
    saveRDS(data.rf,paste(resultPath,"/RFmodel.rds",sep=""))
  }else if(stage=="CV"){
    #apply to validation set. 
    probs<-predict(data.rf,dataTest,type="prob")
    probs<-data.frame(dtSel,probs[,2])
    colnames(probs)<-c("Selection","probs")
    
    return(probs)
    
  }
}

stopCluster(cluz)

if(stage=="CV"){
  
probsAll<-do.call("rbind",probsAll)

probsAgg<-aggregate(probsAll$probs,list(probsAll$Selection),mean)
#probssd<-aggregate(probsAll$probs,list(probsAll$Selection),sd)

probs=probsAgg[,2]
#probssd=probssd[,2]

if(length(probs)<nrow(data)){
  stop("one or more items not validated: increase CV iterations")
}

}

}
  
#stage 2: 
if(stage=="apply"){
  print("MADE IT")
  dataIn<-data[,featuresVecwL]
  
  possiblefields<-c('SignalCode','Analyst',"Comments","FGID","Selection") #some possible column reserved keywords
  if(any(possiblefields) %in% colnames(dataIn)){
    possiblefields <- possiblefields[which(possiblefields %in% colnames(dataIn))]
    dataIn<-dataIn[,-which(names(dataIn) %in% possiblefields)]
  }

  data.rf<-readRDS(model_path)
  
  probs<-predict(data.rf,dataIn,type="prob")[,2]
  
}

if(stage!="train"){

  
#save data with probs, no features needed. 
dataOut<-data[colnames(data) %in% c("StartTime","EndTime","LowFreq","HighFreq","StartFile","EndFile","label","FGID")] #includes label if it exists

dataOut$probs<-probs
  
outName="DETx.csv.gz"
  
write.csv(dataOut,gzfile(paste(resultPath,outName,sep="/")),row.names = FALSE)
  
}
