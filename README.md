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
$ deca-submit cnv --help

 -I STRING[]              : One or more BAM, Parquet or other alignment files
 -L VAL                   : Targets for XHMM analysis as interval_list, BED or other feature file
 -cnv_rate N              : CNV rate (p). Defaults to 1e-8.
 -exclude_targets STRING  : Path to file of targets (chr:start-end) to be excluded from analysis
 -fixed_pc_toremove INT   : Fixed number of principal components to remove if defined. Defaults to undefined.
 -h (-help, --help, -?)   : Print help
 -initial_k_fraction N    : Set initial k to fraction of max components. Defaults to 0.10.
 -l                       : Input file is a list of paths
 -max_sample_mean_RD N    : Maximum sample mean read depth prior to normalization. Defaults to 200.
 -max_sample_sd_RD N      : Maximum sample standard deviation of the read depth prior to normalization. Defaults to 150.
 -max_target_length N     : Maximum target length. Defaults to 10000.
 -max_target_mean_RD N    : Maximum target mean read depth prior to normalization. Defaults to 500.
 -max_target_sd_RD_star N : Maximum target standard deviation of the read depth after normalization. Defaults to 30.
 -mean_target_distance N  : Mean within-CNV target distance (D). Defaults to 70000.
 -mean_targets_cnv N      : Mean targets per CNV (T). Defaults to 6.
 -min_mapping_quality N   : Minimum mapping quality for read to count towards coverage. Defaults to 20.
 -min_sample_mean_RD N    : Minimum sample mean read depth prior to normalization. Defaults to 25.
 -min_some_quality N      : Min Q_SOME to discover a CNV. Defaults to 30.0.
 -min_target_length N     : Minimum target length. Defaults to 10.
 -min_target_mean_RD N    : Minimum target mean read depth prior to normalization. Defaults to 10.
 -o VAL                   : Path to write discovered CNVs as GFF3 file
 -print_metrics           : Print metrics to the log on completion
 -save_rd STRING          : Path to write XHMM read depth matrix
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

If see the following warning messages in the log file, you have not successfully invoked the system libraries:

```dtd
WARN  BLAS:61 - Failed to load implementation from: com.github.fommil.netlib.NativeSystemBLAS
WARN  BLAS:61 - Failed to load implementation from: com.github.fommil.neltlib.NativeRefARPACK
```

# License

ADAM is released under an [Apache 2.0 license](LICENSE.txt).