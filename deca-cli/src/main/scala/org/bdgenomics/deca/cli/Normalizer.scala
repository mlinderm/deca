package org.bdgenomics.deca.cli

import org.apache.spark.SparkContext
import org.bdgenomics.deca.coverage.ReadDepthMatrix
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

trait NormalizeArgs {
  @Args4jOption(required = false,
    name = "-min_target_mean_RD",
    usage = "Minimum target mean read depth prior to normalization. Defaults to 10.")
  var minTargetMeanRD: Int = 10

  @Args4jOption(required = false,
    name = "-max_target_mean_RD",
    usage = "Maximum target mean read depth prior to normalization. Defaults to 500.")
  var maxTargetMeanRD: Int = 500

  @Args4jOption(required = false,
    name = "-min_sample_mean_RD",
    usage = "Minimum sample mean read depth prior to normalization. Defaults to 25.")
  var minSampleMeanRD: Int = 25

  @Args4jOption(required = false,
    name = "-max_sample_mean_RD",
    usage = "Maximum sample mean read depth prior to normalization. Defaults to 200.")
  var maxSampleMeanRD: Int = 200

  @Args4jOption(required = false,
    name = "-max_sample_sd_RD",
    usage = "Maximum sample standard deviation of the read depth prior to normalization. Defaults to 150.")
  var maxSampleSDRD: Int = 150

  @Args4jOption(required = false,
    name = "-max_target_sd_RD_star",
    usage = "Maximum target standard deviation of the read depth after normalization. Defaults to 30.")
  var maxTargetSDRDStar: Int = 30
}

class NormalizerArgs extends Args4jBase with NormalizeArgs {
  @Args4jOption(required = true,
    name = "-I",
    usage = "The XHMM read depth matrix")
  var inputPath: String = null

  @Args4jOption(required = true,
    name = "-o",
    usage = "The XHMM normalized and z-score centered matrix")
  var outputPath: String = null
}

class Normalizer(protected val args: NormalizerArgs) extends BDGSparkCommand[NormalizerArgs] {

  val companion = Normalizer

  def run(sc: SparkContext): Unit = {

    // TODO: Read in excluded targets
    // TODO: Add in full complement of command line arguments
    var matrix = Deca.readXHMMMatrix(args.inputPath, minTargetLength = 10L, maxTargetLength = 10000L)

    val (zMatrix, zTargets) = Normalization.normalizeReadDepth(
      matrix.depth, matrix.targets,
      minTargetMeanRD = args.minTargetMeanRD,
      maxTargetMeanRD = args.maxTargetMeanRD,
      minSampleMeanRD = args.minSampleMeanRD,
      maxSampleMeanRD = args.maxSampleMeanRD,
      maxSampleSDRD = args.maxSampleSDRD,
      maxTargetSDRDStar = args.maxTargetSDRDStar)

    Deca.writeXHMMMatrix(ReadDepthMatrix(zMatrix, matrix.samples, zTargets), args.outputPath, label = "Matrix")

  }
}