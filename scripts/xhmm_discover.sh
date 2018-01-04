#!/bin/bash

PARAMS=params.txt
TIME_FILE="xhmm.time.txt"
TIME_FORMAT="%C\t%E\t%U\t%M\t%K"

usage()
{
        cat << EOF
usage: `basename $0` [options] ZMATRIX OUTPUT_PREFIX

Options:
  -p [path]                  Parameters file, default: $PARAMS
  -t [path]                  File to write timing data, default: $TIME_FIILE
  -h                         Pring this usage message and exit
EOF
}

while getopts "p:t:h" Option
do
    case $Option in
        p)
            PARAMS=$OPTARG
            ;;
        t)
            TIME_FILE=$OPTARG
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

# Discovers CNVs in normalized data
/usr/bin/time -f ${TIME_FORMAT} --append -o "${TIME_FILE}" \
xhmm --discover -p "${PARAMS}" \
	-r "${1}" \
	-c "${OUT_PREFIX}.xcnv" -s "${OUT_PREFIX}"
