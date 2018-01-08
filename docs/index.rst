DECA User Guide
===============

Introduction
============

DECA is a copy number variant caller built on top of `Apache
Spark <https://spark.apache.org>`__ to allow rapid variant calling on
cluster/cloud computing environments. DECA is built on
`ADAM's <https://github.com/bigdatagenomics/adam>`__ APIs, and is a
reimplementation of the `XHMM <http://atgu.mgh.harvard.edu/xhmm/index.shtml>`__
copy number variant caller. DECA provides an order of magnitude performance
improvement over XHMM when running on a single machine. When running on
a 1,024 core cluster, DECA can call copy number variants from the 1,000 Genomes
exome reads in approximately 5 hours. DECA is highly concordant with XHMM, with
>93% exact breakpoint concordance, and <0.01% discordant CNV calls.

Running DECA
============

DECA is run through the `deca-submit` command line:

.. code:: bash

    ./bin/deca-submit
   
::
   
   Using SPARK_SUBMIT=/usr/local/bin/spark-2.2.1-bin-hadoop2.7/bin/spark-submit

   Usage: deca-submit [<spark-args> --] <deca-args> [-version]

   Choose one of the following commands:

   normalize : Normalize XHMM read-depth matrix
   coverage : Generate XHMM read depth matrix from read data
   discover : Call CNVs from normalized read matrix
   normalize_and_discover : Normalize XHMM read-depth matrix and discover CNVs
   cnv : Discover CNVs from raw read data

The `deca-submit` script follows the same conventions as the `adam-submit`
command line, whose documentation can be found
`here <http://adam.readthedocs.io/en/adam-parent_2.11-0.23.0/cli/overview/>`__.
As a result, just like ADAM, DECA can be deployed on a local machine, on
`AWS <http://adam.readthedocs.io/en/adam-parent_2.11-0.23.0/deploying/cgcloud/#running-adam-on-aws-ec2-using-cgcloud>`__,
an in-house cluster running `YARN <http://adam.readthedocs.io/en/adam-parent_2.11-0.23.0/deploying/yarn/>`__
or `SLURM <http://adam.readthedocs.io/en/adam-parent_2.11-0.23.0/deploying/slurm/>`__,
or using `Toil <http://adam.readthedocs.io/en/adam-parent_2.11-0.23.0/deploying/toil/>`__.
We provide a Toil workflow for running DECA as part of the `bdgenomics.workflows
package <http://bdg-workflows.readthedocs.io/en/latest/running/deca.html>`__.
``bdgenomics.workflows`` can be `installed with
pip <http://bdg-workflows.readthedocs.io/en/latest/gettingStarted/install.html#basic-installation>`__.

.. toctree::
   :caption: Installation
   :maxdepth: 2

   installation/source

.. toctree::
   :caption: Running
   :maxdepth: 2

   running/calling-cnvs

* :ref:`genindex`
* :ref:`search`
