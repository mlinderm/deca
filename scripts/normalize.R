# R test implementation for exploring normalization components of XHMM algorithm
# Implements steps in http://atgu.mgh.harvard.edu/xhmm/tutorial.shtml (using demo data)

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

PVE_mean_factor <- 0.7

Rd <- read.xhmm("RUN/DATA.RD.txt")
targets <- targets2data.frame(colnames(Rd))

# Filter I: Filter extreme targets and samples, then mean center the data
# XHMM first filters by targets, then by samples
#--minTargetSize 10 --maxTargetSize 10000 \
#--minMeanTargetRD 10 --maxMeanTargetRD 500 \
#--minMeanSampleRD 25 --maxMeanSampleRD 200 \
#--maxSdSampleRD 150

low_complexity <- read.table("RUN/low_complexity_targets.txt", header=F, stringsAsFactors=F, col.names="target")
extreme_gc     <- read.table("RUN/extreme_gc_targets.txt", header=F, stringsAsFactors=F, col.names="target")

# XHMM filterTargetProperties function
targets_to_keep <- !(colnames(Rd) %in% union(low_complexity$target, extreme_gc$target))
targets_to_keep <- targets_to_keep & {
  target_means <- colMeans(Rd)
  target_lengths <- targets$end - targets$beg + 1
  target_means >= 10 & target_means <= 500 & target_lengths >= 10 & target_lengths <= 10000
}
Rd <- Rd[,targets_to_keep]

# XHMM filterSampleProperties function
samples_to_keep <- {
  sample_means <- rowMeans(Rd)
  sample_sds <- apply(Rd, 1, sd)  # Apply sd to all row vectors
  sample_means >= 25 & sample_means <= 200 & sample_sds <= 150
}
Rd <- Rd[samples_to_keep,]

# Rd now the equivalent of .filtered_centered.RD.txt
Rd <- scale(Rd, center=T, scale=F)  # Mean center columns

# Perform SVD decomposition (xhmm --PCA)
decomp <- svd(Rd, nu=0)  # To compute V_t

# vt is _PCA.PC.txt 
# d is _PCA.PC_SD.txt
# t(u) is _PCA.PC_LOADINGS.txt

# Regenerate the original 
# decomp$u %*% (diag(decomp$d) %*% decomp$vt)

# Normalize mean-centered data using PCA data
# TODO: Compute total variance without computing all SVD components?
pc_var <- decomp$d**2
scaled_mean_var <- mean(pc_var) * PVE_mean_factor
to_remove <- pc_var >= scaled_mean_var

# Should remove first 3 components
C <- decomp$v[,to_remove,drop=F]

Rd_star <- Rd
for (r in 1:nrow(Rd)) {
  # Row centric implementation
  for (pc in 1:ncol(C)) {
    loading <- C[,pc] %*% Rd[r,]   # dot-product
    Rd_star[r,] = Rd_star[r,] - (loading * C[,pc])
  }
}

# Rd_star is .PCA_normalized.txt

# Filter II: Filter extremely variable targets
# --maxSdTargetRD 30

# XHMM filterTargetProperties function
norm_targets_to_keep <- {
  target_sds <- apply(Rd_star, 2, sd)
  target_sds <= 30
}
Rd_star <- Rd_star[,norm_targets_to_keep]

# Z-score center (by sample) the PCA-normalized data

# Execute scale (implementing z-score = (x-μ)/σ) on each row (need to transpose final result)
# due to how R prepares apply result (by columns)
Z <- t(apply(Rd_star, 1, scale))
colnames(Z) <- colnames(Rd_star)

# Z is .PCA_normalized.filtered.sample_zscores.RD.txt

# Experiments with bounding mean variance
bounded_mean_pc_var <- rep_len(0, length(pc_var))
bounded_to_remove <- rep_len(0, length(pc_var))
for (i in 1:length(pc_var)) {
  bounded_mean_pc_var[i] <- mean(c(pc_var[1:i], rep_len(pc_var[i], length(pc_var)-i)))
  bounded_to_remove[i] <- sum(pc_var >= PVE_mean_factor * bounded_mean_pc_var[i])
}

