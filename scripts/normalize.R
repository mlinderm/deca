 # R test implementation for exploring XHMM algorithm

read.xhmm <- function(filename) {
  # Convenience function to read XHMM table outputs
  d <- as.matrix(read.table(filename, header=T, row.names=1))
  colnames(d) <- as.character(read.table(filename, header=F, row.names=1, nrow=1, stringsAsFactors=F))
  d
}

PVE_mean_factor <- 0.7

# TODO: Filter I: Filter extreme targets and samples, then mean center the data

#--minTargetSize 10 --maxTargetSize 10000 \
#--minMeanTargetRD 10 --maxMeanTargetRD 500 \
#--minMeanSampleRD 25 --maxMeanSampleRD 200 \
#--maxSdSampleRD 150

Rd <- read.xhmm("RUN/DATA.filtered_centered.RD.txt")

# Perform SVD decomposition (xhmm --PCA)
decomp <- La.svd(Rd)  # To compute V_t

# vt is _PCA.PC_SD.txt
# d is _PCA.PC.txt
# t(u) is _PCA.PC_LOADINGS.txt

# Regenerate the original 
# decomp$u %*% (diag(decomp$d) %*% decomp$vt)

# Normalize mean-centered data using PCA data
# TODO: Compute total variance without computing all SVD components?
pc_var <- decomp$d**2
scaled_mean_var <- (sum(pc_var) / length(pc_var)) * PVE_mean_factor
to_remove <- pc_var >= scaled_mean_var

# Should remove first 3 components
C <- decomp$vt[to_remove,]

Rd_star <- Rd
for (r in 1:nrow(Rd)) {
  # Row centric implementation
  for (pc in 1:nrow(C)) {
    loading <- C[pc,] %*% Rd[r,]   # dot-product
    Rd_star[r,] = Rd_star[r,] - (loading * C[pc,])
  }
}

# Rd_star is .PCA_normalized.txt

# TODO: Filter II: Filter extremely variable targets
# --maxSdTargetRD 30

# Z-score center (by sample) the PCA-normalized data

# Execute scale (implementing z-score = (x-μ)/σ) on each row (need to transpose final result)
# due to how R prepares apply result (by columns)
Z <- t(apply(Rd_star, 1, scale))
colnames(Z) <- colnames(Rd_star)

# Z is .PCA_normalized.filtered.sample_zscores.RD.txt