package org.bdgenomics.deca

import breeze.linalg.DenseVector
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.distributed.{ IndexedRow, IndexedRowMatrix }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.feature.FeatureRDD
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD
import org.bdgenomics.adam.rich.RichAlignmentRecord
import org.bdgenomics.deca.coverage.{ ReadDepthMatrix }
import org.bdgenomics.deca.util.MLibUtils
import org.bdgenomics.deca.Timers._
import org.bdgenomics.formats.avro.AlignmentRecord
import org.bdgenomics.utils.misc.Logging

import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.math.{ max, min }
import scala.util.control.Breaks._

/**
 * Created by mlinderman on 3/8/17.
 */

case class CoverageByTarget(targetId: Long, coverage: Double) {
}

object Coverage extends Serializable with Logging {

  def addReadToPileup(region: ReferenceRegion, read: RichAlignmentRecord): Long = {
    var pileup: Long = 0
    var readStart = read.getStart
    read.samtoolsCigar.foreach(cigar => {
      val op = cigar.getOperator
      (op.consumesReadBases, op.consumesReferenceBases) match {
        case (true, true) => {
          val overlap = min(region.end, readStart + cigar.getLength) - max(region.start, readStart)
          if (overlap > 0) {
            pileup += overlap
          }
          readStart += cigar.getLength
        }
        case (true, false) if (op.isIndel) => readStart += cigar.getLength; // Need to distinguish between soft clip and insertion
        case (_, _)                        => ;
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

      val coverageByTarget = ArrayBuffer[CoverageByTarget]()
      val perTargetTotalCoverage = ArrayBuffer[Long]()

      // Scan forward to identify any overlapping targets to compute depth. Return new first target for future searches
      // as we advance through the reads
      @tailrec def scanForward(read: AlignmentRecord, readRegion: ReferenceRegion, firstIndex: Int, targetIndex: Int, localIndex: Int): Int = {
        if (targetIndex >= targetsArray.length)
          return firstIndex
        else {
          val targetRegion = targetsArray(targetIndex)._1
          if (readRegion.covers(targetRegion)) {
            if (localIndex >= perTargetTotalCoverage.length)
              perTargetTotalCoverage ++= Seq.fill[Long](localIndex - perTargetTotalCoverage.length + 1)(0)
            perTargetTotalCoverage(localIndex) += addReadToPileup(targetRegion, new RichAlignmentRecord(read))
            return scanForward(read, readRegion, firstIndex, targetIndex + 1, localIndex + 1)
          } else if (readRegion.referenceName == targetRegion.referenceName && readRegion.start >= targetRegion.end)
            return scanForward(read, readRegion, targetIndex + 1, targetIndex + 1, localIndex + 1)
          else if (readRegion.referenceName == targetRegion.referenceName && targetRegion.start < readRegion.end)
            return scanForward(read, readRegion, firstIndex, targetIndex + 1, localIndex + 1)
          else
            return firstIndex
        }
      }

      // Find starting target of sorted reads once per partition
      var firstRead = readIter.next()
      var firstTarget = findFirstTarget(targetsArray, ReferenceRegion.unstranded(firstRead))
      while (firstTarget.isDefined) {
        val firstTargetIndex = firstTarget.get
        var movingFirstTargetIndex = scanForward(firstRead, ReferenceRegion.unstranded(firstRead), firstTargetIndex, firstTargetIndex, 0)

        breakable {
          readIter.foreach(read => {
            val readRegion = ReferenceRegion.unstranded(read)
            if (read.getContigName != firstRead.getContigName) {
              // Switched contigs. Reset firstRead and firstTarget for coverage analysis of next contig
              firstRead = read
              firstTarget = findFirstTarget(targetsArray, readRegion)
              break // Should happen infrequently
            }
            movingFirstTargetIndex = scanForward(read, readRegion, movingFirstTargetIndex, movingFirstTargetIndex, movingFirstTargetIndex - firstTargetIndex)
          })
          firstTarget = None // Terminate while loop
        }

        coverageByTarget ++= perTargetTotalCoverage.zipWithIndex.map {
          case (totalCoverage, index) => {
            val target = targetsArray(firstTargetIndex + index)
            CoverageByTarget(target._2, totalCoverage.toDouble / target._1.length)
          }
        }
        perTargetTotalCoverage.clear()
      }

      coverageByTarget.toIterator
    })

    // TODO: Return table with target and sample ID to enable different matrices to be generated
    val sqlContext = SQLContext.getOrCreate(coverageRdd.context)
    import sqlContext.implicits._
    val coverageDs = sqlContext.createDataset(coverageRdd)

    val coverageByTargetDs = coverageDs.groupBy(coverageDs("targetId")).sum("coverage")

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
            !read.getDuplicateRead && read.getReadMapped && (minMapQ == 0 || read.getMapq >= minMapQ)
          }))

          // Label coverage with sample ID to create (sampleId, (targetId, coverage)) RDD
          sortedCoverageCalculation(broadcastTargetArray, filteredReadsRdd).map((sampleIdx.toLong, _))
      }

      val sc = SparkContext.getOrCreate()
      sc.union(coverageCoordinatesPerSample)
    }

    val rdMatrix = coverageMatrixFromCoordinates(coverageCoordinates, numSamples, numTargets)

    ReadDepthMatrix(rdMatrix, samplesDriver, targetsDriver)
  }

}
