package org.bdgenomics.deca.cli

import htsjdk.samtools.ValidationStringency
import org.apache.spark.SparkContext
import org.bdgenomics.adam.projections.{ Projection, AlignmentRecordField => ARF, FeatureField => FF }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.deca.coverage.ReadDepthMatrix
import org.bdgenomics.deca.{ Coverage, Deca, HMM, Normalization }
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.spi.StringArrayOptionHandler
import org.kohsuke.args4j.{ Option => Args4jOption }

/**
 * Created by mlinderman on 4/12/17.
 */

object CNVer extends BDGCommandCompanion {
  val commandName = "cnv"
  val commandDescription = "Discover CNVs from raw read data"

  def apply(cmdLine: Array[String]) = {
    new CNVer(Args4j[CNVerArgs](cmdLine))
  }
}

class CNVerArgs extends Args4jBase with CoverageArgs with NormalizeArgs with DiscoveryArgs {
  @Args4jOption(required = true,
    name = "-I",
    usage = "One or more BAM, Parquet or other alignment files",
    handler = classOf[StringArrayOptionHandler])
  var readsPaths: Array[String] = null

  @Args4jOption(required = true,
    name = "-L",
    usage = "Targets for XHMM analysis as interval_list, BED or other feature file")
  var targetsPath: String = null

  @Args4jOption(required = true,
    name = "-o",
    usage = "Path to write discovered CNVs as GFF3 file")
  var outputPath: String = null

  @Args4jOption(required = false,
    name = "-save_rd",
    usage = "Path to write XHMM read depth matrix")
  var rdPath: String = null

  @Args4jOption(required = false,
    name = "-save_zscores",
    usage = "Path to write XHMM normalized, filtered, Z score matrix")
  var zScorePath: String = null
}

class CNVer(protected val args: CNVerArgs) extends BDGSparkCommand[CNVerArgs] {
  val companion = CNVer

  def run(sc: SparkContext): Unit = {
    // 1. Read alignment files
    val readProj = {
      // TODO: Add mate fields when coverage incorporates fragment features
      var readFields = Seq(ARF.readMapped, ARF.mapq, ARF.contigName, ARF.start, ARF.end, ARF.cigar)
      Projection(readFields)
    }

    val readsRdds = args.readsPaths.map(path => {
      // TODO: Add push down filters
      log.info("Loading {}", path)
      sc.loadAlignments(path, projection = Some(readProj), stringency = ValidationStringency.SILENT)
    })

    val targetsAsFeatures = {
      val targetProj = Projection(FF.contigName, FF.start, FF.end)
      sc.loadFeatures(args.targetsPath, projection = Some(targetProj))
    }

    // 2. Compute coverage
    val rdMatrix = Coverage.coverageMatrix(readsRdds, targetsAsFeatures, minMapQ = args.minMappingQuality)
    if (args.rdPath != null) {
      Deca.writeXHMMMatrix(rdMatrix, args.rdPath, label = "DECA._mean_cvg")
    }

    // 3. Normalize read depth
    val (zRowMatrix, zTargets) = Normalization.normalizeReadDepth(
      rdMatrix,
      minTargetMeanRD = args.minTargetMeanRD,
      maxTargetMeanRD = args.maxTargetMeanRD,
      minSampleMeanRD = args.minSampleMeanRD,
      maxSampleMeanRD = args.maxSampleMeanRD,
      maxSampleSDRD = args.maxSampleSDRD,
      maxTargetSDRDStar = args.maxTargetSDRDStar)
    val zMatrix = ReadDepthMatrix(zRowMatrix, rdMatrix.samples, zTargets)
    if (args.zScorePath != null) {
      Deca.writeXHMMMatrix(zMatrix, args.zScorePath, label = "Matrix")
    }

    // 4. Discover CNVs
    var features = HMM.discoverCNVs(zMatrix, M = args.M, T = args.T, p = args.p, D = args.D, minSomeQuality = args.minSomeQuality)
    features.saveAsGff3(args.outputPath, asSingleFile = true)
  }
}
