package org.bdgenomics.deca.cli

import org.apache.spark.SparkContext
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.deca.cli.util.IntOptionHandler
import org.bdgenomics.deca.coverage.ReadDepthMatrix
import org.bdgenomics.deca.util.Target
import org.bdgenomics.deca.{ Deca, HMM, Normalization }
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }

/**
 * Created by mlinderman on 6/2/17.
 */

object NormalizingDiscoverer extends BDGCommandCompanion {
  val commandName = "normalize_and_discover"
  val commandDescription = "Normalize XHMM read-depth matrix and discover CNVs"

  def apply(cmdLine: Array[String]) = {
    new NormalizingDiscoverer(Args4j[NormalizingDiscovererArgs](cmdLine))
  }
}

class NormalizingDiscovererArgs extends Args4jBase with NormalizeArgs with DiscoveryArgs {
  @Args4jOption(required = true,
    name = "-I",
    usage = "The XHMM read depth matrix")
  var inputPath: String = null

  @Args4jOption(required = true,
    name = "-o",
    usage = "Path to write discovered CNVs as GFF3 file")
  var outputPath: String = null
}

class NormalizingDiscoverer(protected val args: NormalizingDiscovererArgs) extends BDGSparkCommand[NormalizingDiscovererArgs] {
  val companion = NormalizingDiscoverer

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

    var features = HMM.discoverCNVs(zMatrix, M = args.M, T = args.T, p = args.p, D = args.D, minSomeQuality = args.minSomeQuality)
    features.saveAsGff3(args.outputPath, asSingleFile = true)
  }
}
