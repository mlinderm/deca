package org.bdgenomics.deca.cli

import org.apache.spark.SparkContext
import org.bdgenomics.deca.{ Deca, Normalization }
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }

/**
 * Created by mlinderman on 2/22/17.
 */

object Normalizer extends BDGCommandCompanion {
  val commandName = "normalize"
  val commandDescription = "Normalize XHMM read-depth matrix"

  def apply(cmdLine: Array[String]) = {
    new Normalizer(Args4j[NormalizerArgs](cmdLine))
  }
}

class NormalizerArgs extends Args4jBase {
  @Argument(required = true,
    metaVar = "INPUT",
    usage = "The XHMM read depth matrix",
    index = 0)
  var inputPath: String = null

  @Args4jOption(required = false,
    name = "-min_target_mean_RD",
    usage = "Minimum target mean read depth prior to normalization. Defaults to 10.")
  var minTargetMeanRD: Int = 10
}

class Normalizer(protected val args: NormalizerArgs) extends BDGSparkCommand[NormalizerArgs] {

  val companion = Normalizer

  def run(sc: SparkContext): Unit = {

    // TODO: Read in excluded targets
    // TODO: Add in various command line arguments
    var (rdMatrix, samples, targets) = Deca.readXHMMMatrix(
      args.inputPath, minTargetLength = 10L, maxTargetLength = 10000L)
    val (zMatrix, zTargets) = Normalization.normalizeReadDepth(rdMatrix, targets)
    // TODO: Write out XHMM compatible table of zScores

  }
}