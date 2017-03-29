package org.bdgenomics.deca.cli

import org.apache.spark.SparkContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.deca.{ Coverage, Deca }
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.spi.StringArrayOptionHandler
import org.kohsuke.args4j.{ Option => Args4jOption }

/**
 * Created by mlinderman on 3/27/17.
 */
object Coverager extends BDGCommandCompanion {
  val commandName = "coverage"
  val commandDescription = "Generate XHMM read depth matrix from read data"

  def apply(cmdLine: Array[String]) = {
    new Coverager(Args4j[CoveragerArgs](cmdLine))
  }
}

class CoveragerArgs extends Args4jBase {
  @Args4jOption(required = true,
    name = "-I",
    usage = "BAM, Parquet or other alignment files",
    handler = classOf[StringArrayOptionHandler])
  var readsPaths: Object = null

  @Args4jOption(required = true,
    name = "-L",
    usage = "Targets for XHMM analysis")
  var targetsPath: String = null

  @Args4jOption(required = true,
    name = "-o",
    usage = "The XHMM read depth matrix")
  var outputPath: String = null
}

class Coverager(protected val args: CoveragerArgs) extends BDGSparkCommand[CoveragerArgs] {

  val companion = Coverager

  def run(sc: SparkContext): Unit = {

    val readsRdds = args.readsPaths.asInstanceOf[Array[String]].map(path => {
      // TODO: Utilize projection and push down filters
      sc.loadAlignments(path)
    })

    // TODO: Utilize projection
    val targetsAsFeatures = sc.loadFeatures(args.targetsPath)

    var (rdMatrix, samples, targets) = Coverage.coverageMatrix(readsRdds, targetsAsFeatures, minMapQ = 20)

    Deca.writeXHMMMatrix(rdMatrix, samples, targets, args.outputPath, label = "DECA._mean_cvg")

  }
}
