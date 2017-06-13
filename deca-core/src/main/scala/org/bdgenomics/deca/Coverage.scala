package org.bdgenomics.deca

import breeze.linalg.DenseVector
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.distributed.{ IndexedRow, IndexedRowMatrix }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.lit
import org.bdgenomics.adam.models.{ ReferencePosition, ReferenceRegion }
import org.bdgenomics.adam.rdd.feature.FeatureRDD
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD
import org.bdgenomics.adam.rich.RichAlignmentRecord
import org.bdgenomics.deca.coverage.ReadDepthMatrix
import org.bdgenomics.deca.util.MLibUtils
import org.bdgenomics.deca.Timers._
import org.bdgenomics.formats.avro.AlignmentRecord
import org.bdgenomics.utils.misc.Logging

import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.collection.mutable.{ ArrayBuffer, HashMap, MultiMap, Set }
import scala.math.{ max, min }
import scala.util.control.Breaks._

/**
 * Created by mlinderman on 3/8/17.
 */

case class CoverageByTarget(targetId: Int, coverage: Double) {
}

object CoverageByTarget {
  def apply(target: (ReferenceRegion, Int), coverage: Long): CoverageByTarget = {
    new CoverageByTarget(targetId = target._2, coverage = coverage.toDouble / target._1.length)
  }
}

object Coverage extends Serializable with Logging {

  def addReadToPileup(region: ReferenceRegion, read: RichAlignmentRecord): Long = {
    var pileup: Long = 0
    var readStart = read.getStart
    read.samtoolsCigar.foreach(cigar => {
      val op = cigar.getOperator
      if (op.consumesReadBases && op.consumesReferenceBases) {
        val overlap = min(region.end, readStart + cigar.getLength) - max(region.start, readStart)
        if (overlap > 0) {
          pileup += overlap
        }
        readStart += cigar.getLength
      } else if (op.consumesReferenceBases) {
        readStart += cigar.getLength; // Need to distinguish between soft clip and insertion
      }
    })
    pileup
  }

  private def queryGreaterThanOther(query: ReferenceRegion, other: ReferenceRegion): Boolean = {
    if (query.referenceName != other.referenceName)
      query.referenceName > other.referenceName
    else
      query.start >= other.end
  }

  /**
   * Compute maximum potential overlapping region
   *
   * @param read
   * @return
   */
  private def couldOverlap(read: AlignmentRecord): Option[ReferenceRegion] = {
    if (read.getMateMapped && read.getMateContigName == read.getContigName &&
      (read.getMateAlignmentStart >= read.getStart && read.getMateAlignmentStart < read.getEnd)) {
      Some(ReferenceRegion(read.getContigName, read.getMateAlignmentStart, read.getEnd))
    } else
      None
  }

  def findFirstTarget(targets: Array[(ReferenceRegion, Int)], query: ReferenceRegion): Option[Int] = {
    var first: Int = 0
    var last: Int = targets.length
    while (first < last) {
      val mid: Int = (first + last) / 2
      var midRegion = targets(mid)._1
      if (queryGreaterThanOther(query, targets(mid)._1))
        first = mid + 1
      else
        last = mid
    }

    if (first < targets.length)
      Some(first)
    else
      None
  }

  def sortedCoverageCalculation(targets: Broadcast[Array[(ReferenceRegion, Int)]], reads: AlignmentRecordRDD) = {
    val targetsArray = targets.value
    val coverageRdd = reads.rdd.mapPartitions(readIter => {

      val perTargetTotalCoverage = ArrayBuffer[Long]()
      val overlapSubtraction = new HashMap[String, Set[(Int, Long)]] with MultiMap[String, (Int, Long)]

      // Scan forward to identify any overlapping targets to compute depth. Return new first target for future searches
      // as we advance through the reads
      @tailrec def scanForward(read: AlignmentRecord, readRegion: ReferenceRegion, firstIndex: Int, targetIndex: Int, localIndex: Int, mateOverlap: Option[ReferenceRegion]): Int = {
        if (targetIndex >= targetsArray.length)
          return firstIndex
        else {
          val targetRegion = targetsArray(targetIndex)._1
          if (readRegion.covers(targetRegion)) {
            if (localIndex >= perTargetTotalCoverage.length)
              perTargetTotalCoverage ++= Seq.fill[Long](localIndex - perTargetTotalCoverage.length + 1)(0)

            val richRead = new RichAlignmentRecord(read)
            perTargetTotalCoverage(localIndex) += addReadToPileup(targetRegion, richRead)

            // Keep track of potentially overlapping paired reads to more precisely calculate their coverage
            mateOverlap.filter(targetRegion.overlaps(_)).foreach(potentialOverlap => {
              val overlapAndTarget = targetRegion.intersection(potentialOverlap)
              overlapSubtraction.addBinding(read.getReadName, (localIndex, addReadToPileup(overlapAndTarget, richRead)))
            })

            return scanForward(read, readRegion, firstIndex, targetIndex + 1, localIndex + 1, mateOverlap)
          } else if (readRegion.referenceName == targetRegion.referenceName && readRegion.start >= targetRegion.end)
            return scanForward(read, readRegion, targetIndex + 1, targetIndex + 1, localIndex + 1, mateOverlap)
          else
            return firstIndex
        }
      }

      // CoverageByTarget for return
      val coverageByTarget = ArrayBuffer[CoverageByTarget]()

      // Find starting target of sorted reads once per partition
      var firstRead = readIter.next()
      var firstTarget = findFirstTarget(targetsArray, ReferenceRegion.unstranded(firstRead))
      while (firstTarget.isDefined) {
        val firstTargetIndex = firstTarget.get
        var movingFirstTargetIndex = scanForward(
          firstRead, ReferenceRegion.unstranded(firstRead),
          firstTargetIndex, firstTargetIndex, localIndex = 0,
          couldOverlap(firstRead))

        breakable {
          readIter.foreach(read => {
            val readRegion = ReferenceRegion.unstranded(read)

            // Crossing contig boundary? Reset firstRead and firstTarget for coverage analysis of next contig
            if (read.getContigName != firstRead.getContigName) {
              firstRead = read
              firstTarget = findFirstTarget(targetsArray, readRegion)
              break // Should happen infrequently
            }

            // Does this read potentially overlap its earlier mate? If so, subtract previously computed overlap region.
            // We could (should) check if that possible overlap region would have actually fully overlapped, like we
            // assumed. Not doing so could produce pessimistic coverage. If we implement that check, however, the
            // maximum error decreases from 1 to 0.2, but the mean error goes up.
            overlapSubtraction.remove(read.getReadName).foreach(overlaps => {
              overlaps.foreach {
                case (localIndex, coverage) => {
                  perTargetTotalCoverage(localIndex) -= coverage //Math.min(coverage, addReadToPileup(overlapAndTarget, read))
                }
              }
            })

            movingFirstTargetIndex = scanForward(
              read, readRegion,
              movingFirstTargetIndex, movingFirstTargetIndex, localIndex = (movingFirstTargetIndex - firstTargetIndex),
              couldOverlap(read))
          })

          firstTarget = None // Terminate while loop since we have exhausted all reads
        }

        // Translate local indices to global target indices
        coverageByTarget ++= perTargetTotalCoverage.zipWithIndex.map {
          case (totalCoverage, index) => CoverageByTarget(targetsArray(firstTargetIndex + index), totalCoverage)
        }

        // Reset local coverage counts
        perTargetTotalCoverage.clear()
        overlapSubtraction.clear()
      }

      coverageByTarget.toIterator
    })

    val sqlContext = SQLContext.getOrCreate(coverageRdd.context)
    import sqlContext.implicits._
    val coverageDs = sqlContext.createDataset(coverageRdd)

    coverageDs.groupBy(coverageDs("targetId")).sum("coverage")
  }

  def coverageMatrixFromCoordinates(coverageCoordinates: RDD[(Int, (Int, Double))], numSamples: Long, numTargets: Long): IndexedRowMatrix = CoverageCoordinatesToMatrix.time {
    // TODO: Are there additional partitioning (or reduction) optimizations that should be applied here?
    val indexedRows = coverageCoordinates.groupByKey(numSamples.toInt).map {
      case (sampleIdx, targetCovg) =>
        var perTargetCoverage = DenseVector.zeros[Double](numTargets.toInt)
        targetCovg.foreach { case (targetIdx, covg) => perTargetCoverage(targetIdx) = covg; }
        IndexedRow(sampleIdx.toLong, MLibUtils.breezeVectorToMLlib(perTargetCoverage))
    }
    new IndexedRowMatrix(indexedRows, numSamples, numTargets.toInt)
  }

  def coverageMatrix(readRdds: Seq[AlignmentRecordRDD], targets: FeatureRDD, minMapQ: Int = 0): ReadDepthMatrix = ComputeReadDepths.time {
    // Sequence dictionary parsing is broken in current ADAM release:
    //    https://github.com/bigdatagenomics/adam/issues/1409
    // which breaks the desired sorting of the targets. Upgrading to a newer version of ADAM did not fix the issues as
    // the necessary indices are not being set when the header of the interval_list is being parsed. Instead we assume
    // the intervals are supplied in the desired order.

    val targetsDriver = targets.rdd
      .map(ReferenceRegion.unstranded(_))
      .collect()

    val broadcastTargetArray = {
      // After assigning target indices we sort with natural ordering of the ReferenceRegions (lexicographic contigs)
      // to eliminate need for sequence dictionary in subsequent analyses
      val sc = SparkContext.getOrCreate()
      sc.broadcast(targetsDriver.zipWithIndex.sortBy(_._1))
    }

    // TODO: Check inputs are in sorted order

    val samplesDriver = readRdds.map(readsRdd => {
      val samples = readsRdd.recordGroups.toSamples
      if (samples.length > 1)
        throw new IllegalArgumentException("reads RDD must be a single sample")
      samples.head.getSampleId
    }).toArray

    val numSamples = readRdds.length
    val numTargets = targetsDriver.length

    val coverageCoordinates = TargetCoverage.time {
      val coverageCoordinatesPerSample = readRdds.zipWithIndex.map {
        case (readsRdd, sampleIdx) =>
          // Basic read filtering
          val filteredReadsRdd = readsRdd.transform(rdd => rdd.filter(read => {
            !read.getDuplicateRead &&
              !read.getFailedVendorQualityChecks &&
              read.getPrimaryAlignment &&
              read.getReadMapped &&
              (minMapQ == 0 || read.getMapq >= minMapQ)
          }))

          // Label coverage with sample ID to create (sampleId, (targetId, coverage)) RDD
          // Union as RDDs instead of datasets to minimize query planning
          // https://stackoverflow.com/questions/37612622/spark-unionall-multiple-dataframes
          sortedCoverageCalculation(broadcastTargetArray, filteredReadsRdd)
            .withColumn("sample", lit(sampleIdx))
            .rdd.map(row => { (row.getInt(2), (row.getInt(0), row.getDouble(1))) })
      }

      val sc = SparkContext.getOrCreate()
      sc.union(coverageCoordinatesPerSample)
    }

    val rdMatrix = coverageMatrixFromCoordinates(coverageCoordinates, numSamples, numTargets)

    ReadDepthMatrix(rdMatrix, samplesDriver, targetsDriver)
  }

}
