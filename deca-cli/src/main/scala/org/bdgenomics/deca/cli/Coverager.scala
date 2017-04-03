package org.bdgenomics.deca.cli

import org.apache.spark.SparkContext
import org.bdgenomics.adam.projections.{ AlignmentRecordField => ARF, FeatureField => FF }
import org.bdgenomics.adam.projections.Projection
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

  @Args4jOption(required = false,
    name = "-min_mapping_quality",
    usage = "Minimum mapping quality for read to count towards coverage")
  var minMappingQuality: Int = 20

  @Args4jOption(required = false,
    name = "-min_base_quality",
    usage = "Minimum base quality for base to count towards coverage")
  var minBaseQuality: Int = 0
}

class Coverager(protected val args: CoveragerArgs) extends BDGSparkCommand[CoveragerArgs] {

  val companion = Coverager

  def run(sc: SparkContext): Unit = {

    val readProj = {
      // TODO: Add mate fields when coverage incorporates fragment features
      var readFields = Seq(ARF.readMapped, ARF.mapq, ARF.contigName, ARF.start, ARF.end, ARF.cigar)
      if (args.minBaseQuality > 0) {
        readFields :+ ARF.qual
      }
      Projection(readFields)
    }

    val readsRdds = args.readsPaths.asInstanceOf[Array[String]].map(path => {
      // TODO: Add push down filters
      println("Loading " + path)
      sc.loadAlignments(path, projection = Some(readProj))
    })

    val targetProj = Projection(FF.contigName, FF.start, FF.end)
    val targetsAsFeatures = sc.loadFeatures(args.targetsPath, projection = Some(targetProj))

    var (rdMatrix, samples, targets) = Coverage.coverageMatrix(
      readsRdds, targetsAsFeatures,
      minMapQ = args.minMappingQuality, minBaseQ = args.minBaseQuality)

    Deca.writeXHMMMatrix(rdMatrix, samples, targets, args.outputPath, label = "DECA._mean_cvg")

  }
}
