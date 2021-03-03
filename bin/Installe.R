options(timeout=1800)

Packages<-c("Rtools","doParallel","tuneR","signal","foreach","imager","oce","randomForest","seewave","plotrix","autoimage","pracma","PRROC","flux","stringi")

for(n in Packages){
  install.packages(n, repos = "http://cran.us.r-project.org")
}
