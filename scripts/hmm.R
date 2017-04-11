library(Rmpfr)

XHMM_RUN_DIR <- "RUN"

D <- 70000
p <- 1e-8
q <- 1/6
M <- 3

read.xhmm <- function(filename) {
  # Convenience function to read XHMM table outputs
  d <- as.matrix(read.table(filename, header=T, row.names=1))
  colnames(d) <- as.character(read.table(filename, header=F, row.names=1, nrow=1, stringsAsFactors=F))
  d
}

targets2data.frame <- function(targets) {
  # Convert string regions, chr:beg-end to data.frame with chr, beg and end columns
  library(stringr)
  
  split <- unlist(strsplit(targets,"[:-]"))
  data.frame(
    chr=split[seq.int(1L, length(split), 3L)],
    beg=as.integer(split[seq.int(2L, length(split), 3L)]),
    end=as.integer(split[seq.int(3L, length(split), 3L)])
  )
}

trans.matrix <- function(f) {
  # matrix(c(
  #    f*(1-q)+ (1-f)*p, f*q + (1-f)*(1-2*p), (1-f)*p, p, 1-2*p, p, (1-f)*p, f*q+(1-f)*(1-2*p), f*(1-q)+ (1-f)*p    
  #  ), ncol=3, byrow=T)
  mpfrArray(c(
    f*(1-q)+(1-f)*p, p, (1-f)*p, f*q + (1-f)*(1-2*p), 1-2*p, f*q+(1-f)*(1-2*p), (1-f)*p, p, f*(1-q)+(1-f)*p    
  ), prec=80, dim=c(3,3))
}

Z <- read.xhmm(file.path(XHMM_RUN_DIR,"DATA.PCA_normalized.filtered.sample_zscores.RD.txt"))

d <- rep(0, ncol(Z))
d[2:length(d)] <- {
  # Compute the distance between adjacent targets
  targets <- targets2data.frame(colnames(Z))
  (targets$beg[-1] - targets$end[1:length(targets$end)-1]) - 1
}


path <- matrix(nrow=nrow(Z), ncol=ncol(Z))
#for (i in 1:nrow(Z)) {
i <- 19 #19 #15
  emit <- mpfrArray(NA,prec=80,dim = c(3,ncol(Z))) #matrix(nrow=3, ncol=ncol(Z))
  emit[1,] <- dnorm(Z[i,], mean=-M, sd=1)
  emit[2,] <- dnorm(Z[i,], mean=0, sd=1)
  emit[3,] <- dnorm(Z[i,], mean=M, sd=1)
  
  
  score <- mpfrArray(NA, prec = 80, dim = c(3,ncol(Z))) #matrix(nrow=3, ncol=ncol(Z))
  backp <- matrix(nrow=3, ncol=ncol(Z))
  fwd   <- mpfrArray(NA, prec = 80, dim = c(3,ncol(Z))) #matrix(nrow=3, ncol=ncol(Z))
  bwd   <- mpfrArray(NA, prec = 80, dim = c(3,ncol(Z))) #matrix(nrow=3, ncol=ncol(Z))
  
  score[,1] <- c(p, 1- 2*p, p) * emit[,1]
  fwd[,1] <- score[,1]
  for (j in 2:ncol(Z)) {
    trans <- trans.matrix(exp(-d[j]/D))
    for (s in 1:3) {
      # Basic Viterbi (not in log space)
      edges <- score[,j-1] * trans[,s] * emit[s,j]
      backp[s,j] <- which.max(edges)
      score[s,j] <- edges[backp[s,j]] 
      
      # Basic forward without normalization
      fwd[s,j] <- sum(fwd[,j-1] * trans[,s] * emit[s,j])
    }
  }
  
  bwd[,ncol(Z)] <- 1
  for (j in (ncol(Z)-1):1) {
    trans <- trans.matrix(exp(-d[j+1]/D))
    for (s in 1:3) {
      edges <- bwd[,j+1] * emit[,j+1] * trans[s,] 
      bwd[s,j] <- sum(edges)
    }
  }
  
  path[i,ncol(Z)] <- which.max(score[,ncol(Z)])
  for (j in ncol(Z):2) {
    path[i,j-1] <- backp[path[i,j],j]
  }
  
  
#}

# Test on the deletion in HG00121 from targets 104 to 117
if (i == 15) {
  exact <- fwd[1,104]
  for (j in 105:117) {
    trans <- trans.matrix(exp(-d[j]/D))
    exact <- exact * trans[1,1] * emit[1,j]
  }
  exact <- exact * bwd[1,117] / sum(fwd[,ncol(Z)])
  
  diploid <- fwd[2,104]
  for (j in 105:117) {
    trans <- trans.matrix(exp(-d[j]/D))
    diploid <- diploid * trans[2,2] * emit[2,j]
  }
  diploid <- diploid * bwd[2,117] / sum(fwd[,ncol(Z)]) 
  
  local_fwd <- mpfrArray(0.0, prec = 80, dim = c(3,14))
  local_fwd[1:2,1] <- fwd[1:2,104]
  for (j in 105:117) {
    trans <- trans.matrix(exp(-d[j]/D))
    for (s in 1:2) {
      local_fwd[s,j-103] <- sum(local_fwd[,j-104] * trans[,s] * emit[s,j])
    } 
  }
  some <- (sum(local_fwd[,14] * bwd[,117])/ sum(fwd[,ncol(Z)])) - diploid 
}
 
# Test on the duplication in HG00113 from targets 4 to 11 
if (i == 19) {
  exact <- fwd[3,4]
  for (j in 5:11) {
    trans <- trans.matrix(exp(-d[j]/D))
    exact <- exact * trans[3,3] * emit[3,j]
  }
  exact <- exact * bwd[3,11] / sum(fwd[,ncol(Z)])
  
  diploid <- fwd[2,4]
  for (j in 5:11) {
    trans <- trans.matrix(exp(-d[j]/D))
    diploid <- diploid * trans[2,2] * emit[2,j]
  }
  diploid <- diploid * bwd[2,11] / sum(fwd[,ncol(Z)])
  
  local_fwd <- mpfrArray(0.0, prec = 80, dim = c(3,8))
  local_fwd[2:3,1] <- fwd[2:3,4]
  for (j in 5:11) {
    trans <- trans.matrix(exp(-d[j]/D))
    for (s in 2:3) {
      local_fwd[s,j-3] <- sum(local_fwd[,j-4] * trans[,s] * emit[s,j])
    } 
  }
  some <- (sum(local_fwd[,8] * bwd[,11])/ sum(fwd[,ncol(Z)])) - diploid 
}
  