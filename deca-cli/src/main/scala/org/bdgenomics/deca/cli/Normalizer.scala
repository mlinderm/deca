package org.bdgenomics.deca.cli

import org.apache.spark.SparkContext
import org.bdgenomics.deca.cli.util.{ IntOptionHandler => IntOptionArg, StringOptionHandler => StringOptionArg }
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
    name = "-exclude_targets",
    usage = "Path to file of targets (chr:start-end) to be excluded from analysis",
    handler = classOf[StringOptionArg])
  var excludeTargetsPath: Option[String] = None

  @Args4jOption(required = false,
    name = "-min_target_length",
    usage = "Minimum target length. Defaults to 10.")
  var minTargetLength: Long = 10L

  @Args4jOption(required = false,
    name = "-max_target_length",
    usage = "Maximum target length. Defaults to 10000.")
  var maxTargetLength: Long = 10000L

  @Args4jOption(required = false,
    name = "-min_target_mean_RD",
    usage = "Minimum target mean read depth prior to normalization. Defaults to 10.")
  var minTargetMeanRD: Double = 10

  @Args4jOption(required = false,
    name = "-max_target_mean_RD",
    usage = "Maximum target mean read depth prior to normalization. Defaults to 500.")
  var maxTargetMeanRD: Double = 500

  @Args4jOption(required = false,
    name = "-min_sample_mean_RD",
    usage = "Minimum sample mean read depth prior to normalization. Defaults to 25.")
  var minSampleMeanRD: Double = 25

  @Args4jOption(required = false,
    name = "-max_sample_mean_RD",
    usage = "Maximum sample mean read depth prior to normalization. Defaults to 200.")
  var maxSampleMeanRD: Double = 200

  @Args4jOption(required = false,
    name = "-max_sample_sd_RD",
    usage = "Maximum sample standard deviation of the read depth prior to normalization. Defaults to 150.")
  var maxSampleSDRD: Double = 150

  @Args4jOption(required = false,
    name = "-max_target_sd_RD_star",
    usage = "Maximum target standard deviation of the read depth after normalization. Defaults to 30.")
  var maxTargetSDRDStar: Double = 30

  @Args4jOption(required = false,
    name = "-fixed_pc_toremove",
    usage = "Fixed number of principal components to remove if defined. Defaults to undefined",
    handler = classOf[IntOptionArg])
  var fixedPCToRemove: Option[Int] = None
}

class NormalizerArgs extends Args4jBase with NormalizeArgs {
  @Args4jOption(required = true,
    name = "-I",
    usage = "The XHMM read depth matrix")
  var inputPath: String = null

  @Args4jOption(required = true,
    name = "-o",
    usage = "Path to write XHMM normalized, filtered, Z score matrix")
  var outputPath: String = null
}

class Normalizer(protected val args: NormalizerArgs) extends BDGSparkCommand[NormalizerArgs] {

  val companion = Normalizer

  def run(sc: SparkContext): Unit = {
    val matrix = Deca.readXHMMMatrix(args.inputPath,
      targetsToExclude = args.excludeTargetsPath,
      minTargetLength = args.minTargetLength,
      maxTargetLength = args.maxTargetLength)

    val (zRowMatrix, zTargets) = Normalization.normalizeReadDepth(
      matrix,
      minTargetMeanRD = args.minTargetMeanRD,
      maxTargetMeanRD = args.maxTargetMeanRD,
      minSampleMeanRD = args.minSampleMeanRD,
      maxSampleMeanRD = args.maxSampleMeanRD,
      maxSampleSDRD = args.maxSampleSDRD,
      maxTargetSDRDStar = args.maxTargetSDRDStar,
      fixedToRemove = args.fixedPCToRemove)
    val zMatrix = ReadDepthMatrix(zRowMatrix, matrix.samples, zTargets)

    Deca.writeXHMMMatrix(zMatrix, args.outputPath, label = "Matrix")

  }
}