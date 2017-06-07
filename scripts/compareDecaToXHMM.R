# Compute concordance of CNV calls between XHMM and DECA

suppressPackageStartupMessages(library(dplyr))
suppressPackageStartupMessages(library(tidyr))
suppressPackageStartupMessages(library(argparse))

parser <- ArgumentParser()
parser$add_argument("XHMM.xcnv", nargs=1, help="XHMM xcnv output")
parser$add_argument("DECA.gff3", nargs=1, help="DECA gff3 output")
args <- parser$parse_args()

deca <- 
  read.table(args$DECA.gff3, col.names=c("SEQID","SOURCE","TYPE","START","END","SCORE","STRAND","PHASE","ATTRIBUTES"), stringsAsFactors = F) %>%
  mutate(TYPE=factor(TYPE,c("DEL","DUP")))
  
xhmm <- read.table(args$XHMM.xcnv, header=T, stringsAsFactors = F) %>%
  separate(INTERVAL, c("SEQID","START","END"), remove=F, convert=T) %>%
  mutate(CNV=factor(CNV, c("DEL","DUP")))

join_columns <- c("SAMPLE" = "SOURCE", "CNV" = "TYPE", "SEQID" = "SEQID", "START"="START", "END" = "END")
xhmm_only <- anti_join(xhmm, deca, by=join_columns)

# Recover partial overlaps
potential_overlap <- left_join(xhmm_only, deca, by=c("SAMPLE" = "SOURCE", "CNV" = "TYPE", "SEQID" = "SEQID"), suffix=c(".xhmm", ".deca")) 
some_overlap <- potential_overlap %>%
  filter((START.xhmm >= START.deca & START.xhmm <= END.deca) | (END.xhmm >= START.deca & START.xhmm <= END.deca))

# Remove partially overalapping CNVs
no_overlap <- anti_join(xhmm_only, some_overlap, by=c("SAMPLE","CNV","INTERVAL"))

cat("XHMM variants: ", nrow(xhmm), "\n", file=stdout(), sep="")
cat("XHMM variants - DECA variants (exact): ", nrow(xhmm_only), "\n", file=stdout(), sep="")
cat("XHMM variants - DECA variants (overlap): ", nrow(no_overlap), "\n", file=stdout(), sep="")