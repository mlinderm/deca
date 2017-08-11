suppressPackageStartupMessages(library(tidyr))
suppressPackageStartupMessages(library(dplyr))
suppressPackageStartupMessages(library(tibble))
suppressPackageStartupMessages(library(argparse))

parser <- ArgumentParser()
parser$add_argument("XHMM.RD.txt", nargs=1, help="XHMM read matrix")
parser$add_argument("DECA.RD.txt", nargs=1, help="DECA read matrix")
args <- parser$parse_args()

gatk <- read.table(args$XHMM.RD.txt, header=T, row.names=1)
deca <- read.table(args$DECA.RD.txt, header=T, row.names=1)

samp <- sort(intersect(rownames(gatk), rownames(deca)))

gatk <- gatk[samp,]
deca <- deca[samp,]

diff <- tbl_df(gatk-deca)
diff %<>% rownames_to_column("SAMPLE") %>% gather(TARGET, DELTA, -SAMPLE)
diff %>% arrange(desc(DELTA))
diff %>% arrange(DELTA)
summary(diff)
