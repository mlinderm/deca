Building DECA from Source
=========================

You will need to have `Apache Maven <http://maven.apache.org/>`__
version 3.1.1 or later installed in order to build DECA.

    **Note:** The default configuration is for Hadoop 2.7.3. If building
    against a different version of Hadoop, please pass
    ``-Dhadoop.version=<HADOOP_VERSION>`` to the Maven command.

.. code:: bash

    git clone https://github.com/bigdatagenomics/deca.git
    cd avocado
    export MAVEN_OPTS="-Xmx512m -XX:MaxPermSize=128m"
    mvn clean package -DskipTests

Outputs

::

    ...
    [INFO] ------------------------------------------------------------------------
    [INFO] BUILD SUCCESS
    [INFO] ------------------------------------------------------------------------
    [INFO] Total time: 9.647s
    [INFO] Finished at: Thu May 23 15:50:42 PDT 2013
    [INFO] Final Memory: 19M/81M
    [INFO] ------------------------------------------------------------------------

You might want to take a peek at the ``scripts/jenkins-test`` script and
give it a run. We use this script to test that DECA is
working correctly.

Running DECA
------------

DECA is packaged as an
`überjar <https://maven.apache.org/plugins/maven-shade-plugin/>`__ and
includes all necessary dependencies, except for Apache Hadoop and Apache
Spark.

You might want to add the following to your ``.bashrc`` to make running
DECA easier:

.. code:: bash

    alias deca-submit="${DECA_HOME}/bin/deca-submit"

``$DECA_HOME`` should be the path to where you have checked DECA out on
your local filesystem. The alias calls a script that wraps
the ``spark-submit`` command to set up DECA. You
will need to have the Spark binaries on your system; prebuilt binaries
can be downloaded from the `Spark
website <http://spark.apache.org/downloads.html>`__. Our `continuous
integration setup <https://amplab.cs.berkeley.edu/jenkins/job/DECA/>`__
builds DECA against Spark 2.0.0, Scala 2.11, and Hadoop versions 2.3.0 and 2.6.0.

Once this alias is in place, you can run DECA by simply typing
``deca-submit`` at the command line.

.. code:: bash

    deca-submit

Native Linear Algebra Libraries
-------------------------------

For best performance, DECA should be linked against native linear algebra
libraries, like `ATLAS <http://math-atlas.sourceforge.net/>`__, or Intel's
`Math Kernel Library <https://software.intel.com/en-us/mkl>`__. If we are
running on an Apache Spark cluster, we will need to ensure that our Apache
Spark distribution `bundles the interfaces to the native linear algebra
libraries <http://www.spark.tc/blas-libraries-in-mllib/>`__. If we are running
locally, we should include these libraries in the DECA JAR. We can do this by
running with the `native-lgpl` Maven profile:

.. code:: bash

    mvn -P native-lgpl package

The bindings to the native libraries are licensed under the `LGPL <https://www.gnu.org/licenses/lgpl-3.0.en.html>`.
As an Apache licensed project, we cannot bundle LGPL'ed libraries in our binary
distributions.
