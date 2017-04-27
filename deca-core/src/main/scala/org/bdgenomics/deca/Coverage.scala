package org.bdgenomics.deca

import breeze.linalg.{ DenseVector, convert, min }
import breeze.stats.mean
import htsjdk.samtools.{ CigarElement, CigarOperator }
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.distributed.{ IndexedRow, IndexedRowMatrix }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.feature.FeatureRDD
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD
import org.bdgenomics.adam.rich.RichAlignmentRecord
import org.bdgenomics.deca.coverage.{ ReadDepthMatrix, TargetRDD }
import org.bdgenomics.deca.util.MLibUtils
import org.bdgenomics.deca.Timers._
import org.bdgenomics.utils.misc.Logging

import scala.collection.JavaConversions._

case class CoverageByTarget(targetId: Long, coverage: Double) {
}

/**
 * Created by mlinderman on 3/8/17.
 */
object Coverage extends Serializable with Logging {

  def targetCoverage(targets: TargetRDD,
                     reads: AlignmentRecordRDD,
                     minMapQ: Int = 0): RDD[(Long, Double)] = PerSampleTargetCoverage.time {
    val samples = reads.recordGroups.toSamples
    if (samples.length > 1) {
      throw new IllegalArgumentException("reads RDD must be a single sample")
    }

    // Basic read filtering
    val filteredReads = reads.transform(rdd => rdd.filter(read => {
      !read.getDuplicateRead && read.getReadMapped && (minMapQ == 0 || read.getMapq >= minMapQ)
    }))

    // Compute left outer join of targets with reads through combination of inner join and co-group
    val coverageRdd = targets.broadcastRegionJoin(filteredReads).rdd.map(kv => {
      val (target, read) = kv
      CoverageByTarget(target.index,
        ReferenceRegion.unstranded(read).intersection(target.refRegion).width.toDouble / target.refRegion.width.toDouble)
    })

    val sqlContext = SQLContext.getOrCreate(coverageRdd.context)
    import sqlContext.implicits._
    val coverageDs = sqlContext.createDataset(coverageRdd)

    val coverageByTargetDs = coverageDs.groupBy(coverageDs("targetId"))
      .sum("coverage")

    coverageByTargetDs.rdd.map(row => {
      (row.getLong(0), row.getDouble(1))
    })
  }

  def coverageMatrixFromCoordinates(coverageCoordinates: RDD[(Long, (Long, Double))], numSamples: Long, numTargets: Long): IndexedRowMatrix = CoverageCoordinatesToMatrix.time {
    // TODO: Are there additional partitioning (or reduction) optimizations that should be applied here?
    val indexedRows = coverageCoordinates.groupByKey(numSamples.toInt).map {
      case (sampleIdx, targetCovg) =>
        var perTargetCoverage = DenseVector.zeros[Double](numTargets.toInt)
        targetCovg.foreach {
          case (targetIdx, covg) => perTargetCoverage(targetIdx.toInt) = covg
        }
        IndexedRow(sampleIdx, MLibUtils.breezeVectorToMLlib(perTargetCoverage))
    }
    new IndexedRowMatrix(indexedRows, numSamples, numTargets.toInt)
  }

  def coverageMatrix(readRdds: Seq[AlignmentRecordRDD], targets: FeatureRDD, minMapQ: Int = 0): ReadDepthMatrix = ComputeReadDepths.time {
    // Sequence dictionary parsing is broken in current ADAM release:
    //    https://github.com/bigdatagenomics/adam/issues/1409
    // which breaks the required sorting in the creation of the TargetRDD
    // Upgrading to a newer version of ADAM did not fix the issues as the necessary indices are not being set when
    // the header of the interval_list is being parsed
    val orderedTargets = TargetRDD.fromRdd(targets.rdd.zipWithIndex(), targets.sequences)
    orderedTargets.rdd.cache()

    val numSamples = readRdds.length
    val numTargets = orderedTargets.rdd.count

    val coverageCoordinates = TargetCoverage.time {
      val coverageCoordinatesPerSample = readRdds.zipWithIndex.map {
        case (readsRdd, sampleIdx) =>
          // Label coverage with sample ID to create (sampleId, (targetId, coverage)) RDD
          targetCoverage(orderedTargets, readsRdd, minMapQ = minMapQ).map((sampleIdx.toLong, _))
      }

      val sc = SparkContext.getOrCreate()
      sc.union(coverageCoordinatesPerSample)
    }

    val rdMatrix = coverageMatrixFromCoordinates(coverageCoordinates, numSamples, numTargets)
    val samplesDriver = readRdds.map(readsRdd => readsRdd.recordGroups.toSamples.head.getSampleId).toArray
    val targetsDriver = orderedTargets.rdd.map(_.refRegion).collect

    ReadDepthMatrix(rdMatrix, samplesDriver, targetsDriver)
  }

}
