#!/bin/bash

TIME_FILE="xhmm.time.txt"
TIME_FORMAT="%C\t%E\t%U\t%M\t%K"
THREADS=1
EXCLUDE_TARGETS=()
EXCLUDE_SAMPLES=

usage()
{
	cat << EOF
usage: `basename $0` [options] RDMATRIX OUTPUT_PREFIX

Options:
  -p [int]                   Number of threads for OPENBLAS, default: $THREADS
  -x [path]                  Path to files with excluded targets, default empty
  -s [path]                  Path to file with excluded samples
  -t [path]                  File to write timing data, default: $TIME_FIILE
  -h                         Pring this usage message and exit
EOF
}

while getopts "x:s:t:p:h" Option
do
    case $Option in
        x)
            EXCLUDE_TARGETS+=("$OPTARG")
            ;;
        s)
            EXCLUDE_SAMPLES="$OPTARG"
            ;;
        t)
            TIME_FILE=$OPTARG
            ;;
        p)
            THREADS=$OPTARG
            ;;
        h)
            usage
            exit 0
            ;;
        ?)
            usage
            exit 85
            ;;
    esac
done

shift $(($OPTIND - 1))

OUT_PREFIX="${2}"

# Set threading
export OPENBLAS_NUM_THREADS=$THREADS

# Filters samples and targets and then mean-centers the targets
/usr/bin/time -f ${TIME_FORMAT} --append -o "${TIME_FILE}" \
xhmm --matrix -r "${1}" -o "${OUT_PREFIX}.filtered_centered.RD.txt" \
	--centerData --centerType target \
	--outputExcludedTargets "${OUT_PREFIX}.filtered_centered.RD.txt.filtered_targets.txt" \
	--outputExcludedSamples "${OUT_PREFIX}.filtered_centered.RD.txt.filtered_samples.txt"  \
	--minTargetSize 10 \
	--maxTargetSize 10000 \
	--minMeanTargetRD 10 \
	--maxMeanTargetRD 500 \
	--minMeanSampleRD 25 \
	--maxMeanSampleRD 200 \
	--maxSdSampleRD 150 \
	$(for ex in "${EXCLUDE_TARGETS[@]}"; do echo -n "--excludeTargets $ex "; done) $( if [[ -n $EXCLUDE_SAMPLES ]]; then echo "--excludeSamples $EXCLUDE_SAMPLES"; fi)

# Runs PCA on mean-centered data
/usr/bin/time -f ${TIME_FORMAT}  --append -o "${TIME_FILE}" \
xhmm --PCA -r "${OUT_PREFIX}.filtered_centered.RD.txt" --PCAfiles "${OUT_PREFIX}.RD_PCA"

# Normalizes mean-centered data using PCA information
/usr/bin/time -f ${TIME_FORMAT} --append -o "${TIME_FILE}" \
xhmm --normalize -r "${OUT_PREFIX}.filtered_centered.RD.txt" \
	--PCAfiles "${OUT_PREFIX}.RD_PCA" \
	--normalizeOutput "${OUT_PREFIX}.PCA_normalized.txt" \
	--PCnormalizeMethod PVE_mean --PVE_mean_factor 0.7

# Filters and z-score centers (by sample) the PCA-normalized data
/usr/bin/time -f ${TIME_FORMAT} --append -o "${TIME_FILE}" \
xhmm --matrix -r "${OUT_PREFIX}.PCA_normalized.txt" -o "${OUT_PREFIX}.PCA_normalized.filtered.sample_zscores.RD.txt" \
	 --centerData --centerType sample --zScoreData \
	--outputExcludedTargets "${OUT_PREFIX}.PCA_normalized.filtered.sample_zscores.RD.txt.filtered_targets.txt" \
	--outputExcludedSamples "${OUT_PREFIX}.PCA_normalized.filtered.sample_zscores.RD.txt.filtered_samples.txt" \
	--maxSdTargetRD 30
