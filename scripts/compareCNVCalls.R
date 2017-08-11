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
  mutate(kv=strsplit(ATTRIBUTES, ";")) %>% unnest(kv) %>% separate(kv, c("key","val"), sep="=") %>% spread(key, val) %>%
  mutate(TYPE=factor(TYPE,c("DEL","DUP")))
  
xhmm <- read.table(args$XHMM.xcnv, header=T, stringsAsFactors = F) %>%
  separate(INTERVAL, c("SEQID","START","END"), remove=F, convert=T) %>%
  separate(TARGETS, c("START_TARGET","END_TARGET"), sep="\\.\\.", remove=T, conver=T) %>%
  mutate(CNV=factor(CNV, c("DEL","DUP")))

join_columns <- c("SAMPLE" = "SOURCE", "CNV" = "TYPE", "SEQID" = "SEQID", "START"="START", "END" = "END")
xhmm_only <- anti_join(xhmm, deca, by=join_columns)

# Recover partial overlaps
potential_overlap <- left_join(xhmm_only, deca, by=c("SAMPLE" = "SOURCE", "CNV" = "TYPE", "SEQID" = "SEQID"), suffix=c(".xhmm", ".deca")) 
some_overlap <- potential_overlap %>%
  filter((START.xhmm >= START.deca & START.xhmm <= END.deca) | (END.xhmm >= START.deca & START.xhmm <= END.deca)) %>%
  mutate(
    START_DELTA=as.integer(START_TARGET.xhmm)-as.integer(START_TARGET.deca),
    END_DELTA=as.integer(END_TARGET.xhmm)-as.integer(END_TARGET.deca),
    FRAC_OVERLAP=pmax(pmin(END.deca, END.xhmm) - pmax(START.deca, START.xhmm), 0) / (END.xhmm - START.xhmm + 1)
  ) %>%
  group_by(SAMPLE, CNV, INTERVAL) %>%
  top_n(1, FRAC_OVERLAP)

# Remove partially overalapping CNVs
xhmm_only_no_overlap <- anti_join(xhmm_only, some_overlap, by=c("SAMPLE","CNV","INTERVAL"))

deca_only <- anti_join(deca, xhmm, by=c("SOURCE"="SAMPLE", "TYPE"="CNV", "SEQID" = "SEQID", "START"="START", "END" = "END"))
deca_only_no_overlap <- anti_join(
  deca_only,
  left_join(deca_only, xhmm, by=c("SOURCE"="SAMPLE", "TYPE"="CNV", "SEQID"="SEQID"), suffix=c(".deca", ".xhmm")) %>%
    filter((START.xhmm >= START.deca & START.xhmm <= END.deca) | (END.xhmm >= START.deca & START.xhmm <= END.deca)),
  by=c("SOURCE","TYPE","SEQID","START"="START.deca","END"="END.deca")
)

cat("XHMM variants: ", nrow(xhmm), "\n", file=stdout(), sep="")
cat("DECA variants: ", nrow(deca), "\n", file=stdout(), sep="")
cat("XHMM variants - DECA variants (exact match): ", nrow(xhmm_only), "\n", file=stdout(), sep="")
cat("XHMM variants - DECA variants (overlap): ", nrow(xhmm_only_no_overlap), "\n", file=stdout(), sep="")
cat("DECA variants - XHMM variants (overlap): ", nrow(deca_only_no_overlap), "\n", file=stdout(), sep="")

cat("Summary of fraction overlap of XHMM variants:\n", file=stdout())
print(summary(some_overlap$FRAC_OVERLAP))

cat("XHMM only (no overlap):\n", file=stdout())
print(xhmm_only_no_overlap)

cat("DECA only (no overlap):\n", file=stdout())
print(deca_only_no_overlap)