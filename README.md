DECA: Distributed Exome CNV Analyzer
====

# Introduction

DECA is a distributed re-implementation of the [XHMM](https://atgu.mgh.harvard.edu/xhmm/) exome CNV caller using ADAM and Apache Spark.

# Getting Started

## Installation

**Note**: These instructions are shared with other tools that build on [ADAM](https://github.com/bigdatagenomics/adam).

### Building from Source

You will need to have [Maven](http://maven.apache.org/) installed in order to build DECA.

> **Note:** The default configuration is for Hadoop 2.7.3. If building against a different
> version of Hadoop, please edit the build configuration in the `<properties>` section of
> the `pom.xml` file.

```dtd
$ git clone https://github.com/.../deca.git
$ cd deca
$ export MAVEN_OPTS="-Xmx512m"
$ mvn clean package
```

### Installing Spark

You'll need to have a Spark release on your system and the `$SPARK_HOME` environment variable pointing at it; prebuilt binaries can be downloaded from the
[Spark website](http://spark.apache.org/downloads.html). DECA has been developed and tested with
[Spark 2.1.0 built against Hadoop 2.7 with Scala 2.11](http://d3kbcqa49mib13.cloudfront.net/spark-2.1.0-bin-hadoop2.7.tgz), but any more recent Spark distribution should likely work.

## Helpful Scripts

The `bin/deca-submit` script wraps the `spark-submit` commands to set up and launch DECA.

## Commands

```dtd
$ deca-submit

Usage: deca-submit [<spark-args> --] <deca-args> [-version]

Choose one of the following commands:

             normalize : Normalize XHMM read-depth matrix
              coverage : Generate XHMM read depth matrix from read data
              discover : Call CNVs from normalized read matrix
normalize_and_discover : Normalize XHMM read-depth matrix and discover CNVs
                   cnv : Discover CNVs from raw read data

```
You can learn more about a command, by calling it without arguments or with `--help`, e.g.

```dtd
$ deca-submit normalize_and_discover --help
 -I VAL                   : The XHMM read depth matrix
 -cnv_rate N              : CNV rate (p). Defaults to 1e-8.
 -exclude_targets STRING  : Path to file of targets (chr:start-end) to be excluded from analysis
 -fixed_pc_toremove INT   : Fixed number of principal components to remove if defined. Defaults to undefined.
 -h (-help, --help, -?)   : Print help
 -initial_k_fraction N    : Set initial k to fraction of max components. Defaults to 0.10.
 -max_sample_mean_RD N    : Maximum sample mean read depth prior to normalization. Defaults to 200.
 -max_sample_sd_RD N      : Maximum sample standard deviation of the read depth prior to normalization. Defaults to 150.
 -max_target_length N     : Maximum target length. Defaults to 10000.
 -max_target_mean_RD N    : Maximum target mean read depth prior to normalization. Defaults to 500.
 -max_target_sd_RD_star N : Maximum target standard deviation of the read depth after normalization. Defaults to 30.
 -mean_target_distance N  : Mean within-CNV target distance (D). Defaults to 70000.
 -mean_targets_cnv N      : Mean targets per CNV (T). Defaults to 6.
 -min_partitions INT      : Desired minimum number of partitions to be created when reading in XHMM matrix
 -min_sample_mean_RD N    : Minimum sample mean read depth prior to normalization. Defaults to 25.
 -min_some_quality N      : Min Q_SOME to discover a CNV. Defaults to 30.0.
 -min_target_length N     : Minimum target length. Defaults to 10.
 -min_target_mean_RD N    : Minimum target mean read depth prior to normalization. Defaults to 10.
 -o VAL                   : Path to write discovered CNVs as GFF3 file
 -print_metrics           : Print metrics to the log on completion
 -save_zscores STRING     : Path to write XHMM normalized, filtered, Z score matrix
 -zscore_threshold N      : Depth Z score threshold (M). Defaults to 3.
```

## Using native library algebra libraries

Apache Spark includes the [Netlib-Java](https://github.com/fommil/netlib-java) library for high-performance linear algebra. 
Netlib-Java can invoke optimized BLAS and Lapack system libraries if available, however, many Spark distributions are built 
without Netlib-Java system library support. You may be able to use system libraries by including the DECA jar on 
the Spark driver classpath, e.g.

```dtd
deca-submit --driver-class-path $DECA_JAR ...
```
or you may need to rebuild Spark as described in the [Spark MLlib guide](http://spark.apache.org/docs/2.1.0/ml-guide.html). 

If you see the following warning messages in the log file, you have not successfully invoked the system libraries:

```dtd
WARN  BLAS:61 - Failed to load implementation from: com.github.fommil.netlib.NativeSystemBLAS
WARN  BLAS:61 - Failed to load implementation from: com.github.fommil.neltlib.NativeRefARPACK
```

# Example usage with XHMM tutorial data

A small dataset (30 samples by 300 targets) is distributed as part of the [XHMM tutorial](http://atgu.mgh.harvard.edu/xhmm/tutorial.shtml). 
Using [pre-computed read-depth matrix and related files](http://atgu.mgh.harvard.edu/xhmm/RUN.zip) you could call CNVs 
(on a 16-core workstation with 128 GB RAM) with the following command:

```dtd
deca-submit \
--master local[16] \
--driver-class-path $DECA_JAR \
--conf spark.local.dir=/data/scratch/$USER \
--conf spark.driver.maxResultSize=0 
--executor-memory 96G --driver-memory 16G \
-- normalize_and_discover \
-min_some_quality 29.5 \
-exclude_targets exclude_targets.txt \
-I DATA.RD.txt \
-o DECA.gff3
```

The resulting [GFF3](https://github.com/The-Sequence-Ontology/Specifications/blob/master/gff3.md) file should contain

```dtd
22      HG00121 DEL     18898402        18913235        9.167771318038923       .       .       END_TARGET=117;START_TARGET=104;Q_SOME=90;Q_START=8;Q_STOP=4;Q_EXACT=9;Q_NON_DIPLOID=90
22      HG00113 DUP     17071768        17073440        25.32122306047942       .       .       END_TARGET=11;START_TARGET=4;Q_SOME=99;Q_START=53;Q_STOP=25;Q_EXACT=25;Q_NON_DIPLOID=99
```

The `exlude_targets.txt` file is the unique combination of the `extreme_gc_targets.txt` and `low_complexity_targets.txt` 
files provided in the tutorial data. The `min_some_quality` parameter is set to 29.5 to mimic XHMM behavior which uses a 
minimum SOME quality of 30 after rounding (while DECA applies the filter prior to rounding). Depending on your particular
computing environment, you may need to modify the [spark-submit](https://spark.apache.org/docs/latest/submitting-applications.html) 
[configuration parameters](https://spark.apache.org/docs/latest/configuration.html). `spark.driver.maxResultSize` is set to 0 (unlimited) 
to address errors collecting larger amounts of data to the driver.

The corresponding xcnv output from XHMM is:

```dtd
SAMPLE  CNV     INTERVAL        KB      CHR     MID_BP  TARGETS NUM_TARG        Q_EXACT Q_SOME  Q_NON_DIPLOID   Q_START Q_STOP  MEAN_RD MEAN_ORIG_RD
HG00121 DEL     22:18898402-18913235    14.83   22      18905818        104..117        14      9       90      90      8       4       -2.51   37.99
HG00113 DUP     22:17071768-17073440    1.67    22      17072604        4..11   8       25      99      99      53      25      4.00    197.73
```

To call CNVs from the original [BAM files](http://atgu.mgh.harvard.edu/xhmm/EXAMPLE_BAMS.zip):

```dtd
deca-submit \
--master local[16] \
--driver-class-path $DECA_JAR \
--conf spark.local.dir=/data/scratch/$USER \
--conf spark.driver.maxResultSize=0 \
--executor-memory 96G --driver-memory 16G \
-- coverage \
-L EXOME.interval_list \
-I *.bam 
-o DECA.RD.txt
```

followed by the command above (with `DECA.RD.txt` as the input).

# License

DECA is released under an [Apache 2.0 license](LICENSE.txt).