package org.bdgenomics.deca.cli

import htsjdk.samtools.ValidationStringency
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

trait CoverageArgs {
  @Args4jOption(required = false,
    name = "-min_mapping_quality",
    usage = "Minimum mapping quality for read to count towards coverage. Defaults to 20.")
  var minMappingQuality: Int = 20

}

class CoveragerArgs extends Args4jBase with CoverageArgs {
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
    usage = "Path to write XHMM read depth matrix")
  var outputPath: String = null
}

class Coverager(protected val args: CoveragerArgs) extends BDGSparkCommand[CoveragerArgs] {

  val companion = Coverager

  def run(sc: SparkContext): Unit = {

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

    val targetProj = Projection(FF.contigName, FF.start, FF.end)
    val targetsAsFeatures = sc.loadFeatures(args.targetsPath, projection = Some(targetProj))

    var matrix = Coverage.coverageMatrix(readsRdds, targetsAsFeatures, minMapQ = args.minMappingQuality)

    Deca.writeXHMMMatrix(matrix, args.outputPath, label = "DECA._mean_cvg", format = "%.2f")

  }
}
