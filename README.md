DECA: Distributed Exome CNV Analyzer
====

# Introduction

DECA is a distributed re-implementation of the [XHMM](https://atgu.mgh.harvard.edu/xhmm/) exome CNV caller using ADAM and Apache Spark.

# Getting Started

## Installation

**Note**: These instructions are shared with other tools that build on [ADAM](https://github.com/bigdatagenomics/adam).

### Building from Source

You will need to have [Maven](http://maven.apache.org/) installed in order to build ADAM.

> **Note:** The default configuration is for Hadoop 2.6.0. If building against a different
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
[Spark website](http://spark.apache.org/downloads.html). Currently, ADAM and thus DECA defaults to
[Spark 1.6.3 built against Hadoop 2.6](http://d3kbcqa49mib13.cloudfront.net/spark-1.6.3-bin-hadoop2.6.tgz), but any more recent Spark distribution should also work.

### Helpful Scripts

The `bin/deca-submit` script wraps the `spark-submit` commands to set up DECA and launch DECA.

# License

ADAM is released under an [Apache 2.0 license](LICENSE.txt).