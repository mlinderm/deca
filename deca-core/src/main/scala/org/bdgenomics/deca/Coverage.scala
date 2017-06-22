package org.bdgenomics.deca

import breeze.linalg.DenseVector
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.distributed.{ IndexedRow, IndexedRowMatrix }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.lit
import org.bdgenomics.adam.models.{ ReferenceRegion }
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
import scala.collection.mutable
import scala.collection.mutable.{ HashMap, MultiMap, Set }
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

private[deca] class PotentialMateOverlap(names: HashMap[String, AlignmentRecord], targets: MultiMap[String, Int]) {
  def this() = this(new mutable.HashMap[String, AlignmentRecord], new mutable.HashMap[String, Set[Int]] with MultiMap[String, Int])

  def addBinding(read: AlignmentRecord, targetIndex: Int) = {
    names.putIfAbsent(read.getReadName, read)
    targets.addBinding(read.getReadName, targetIndex)
  }

  def remove(read: AlignmentRecord): Option[(AlignmentRecord, mutable.Set[Int])] = {
    val myName = read.getReadName
    targets.remove(myName).map(targetSet => {
      val leftRead = names.remove(myName).get // If targets is defined, so should names
      (leftRead, targetSet)
    })
  }

  def clear() = {
    names.clear()
    targets.clear()
  }
}

object Coverage extends Serializable with Logging {

  def totalCoverageOfRegion(region: ReferenceRegion, read: RichAlignmentRecord): Long = {
    // "Clip" bases when the insert size is shorter than the read length
    val insert = read.getInferredInsertSize
    val regionStart = if (insert != null && insert < 0) max(region.start, read.getEnd + insert - 1) else region.start
    val regionEnd = if (insert != null && insert > 0) min(region.end, read.getStart + insert + 1) else region.end

    var pileup: Long = 0
    var cigarSegmentStart = read.getStart
    read.samtoolsCigar.foreach(cigar => {
      val op = cigar.getOperator
      if (op.consumesReadBases && op.consumesReferenceBases) {
        val overlap = min(regionEnd, cigarSegmentStart + cigar.getLength) - max(regionStart, cigarSegmentStart)
        if (overlap > 0) {
          pileup += overlap
        }
        cigarSegmentStart += cigar.getLength
      } else if (op.consumesReferenceBases) {
        cigarSegmentStart += cigar.getLength; // Need to distinguish between soft clip and insertion
      }
    })

    pileup
  }

  def fragmentOverlap(region: ReferenceRegion, read1: RichAlignmentRecord, read2: RichAlignmentRecord): Long = {
    // TODO: Account for deletions
    val overlap = min(min(region.end, read1.getEnd), read2.getEnd) - max(max(region.start, read1.getStart), read2.getStart)
    if (overlap > 0) overlap else 0
  }

  private def queryGreaterThanOther(query: ReferenceRegion, other: ReferenceRegion): Boolean = {
    if (query.referenceName != other.referenceName)
      query.referenceName > other.referenceName
    else
      query.start >= other.end
  }

  private def readOverlapsTarget(read: AlignmentRecord, target: ReferenceRegion): Boolean = {
    read.getContigName == target.referenceName && read.getEnd() > target.start && read.getStart < target.end
  }

  private def couldOverlapMate(read: AlignmentRecord): Boolean = {
    read.getMateMapped &&
      read.getMateContigName == read.getContigName &&
      read.getMateAlignmentStart >= read.getStart && read.getMateAlignmentStart < read.getEnd
  }

  /**
   * Find smallest target index for which all targets overlap or greater than the query
   *
   * @param targets Array of target regions and associated target indices into read matrix
   * @param query Query region
   * @return Optional target index
   */
  def findFirstTarget(targets: Array[(ReferenceRegion, Int)], query: ReferenceRegion): Option[Int] = {
    var first: Int = 0
    var last: Int = targets.length
    while (first < last) {
      val mid: Int = (first + last) / 2
      if (queryGreaterThanOther(query, targets(mid)._1))
        first = mid + 1
      else
        last = mid
    }

    if (first < targets.length) Some(first) else None
  }

  def sortedCoverageCalculation(targets: Broadcast[Array[(ReferenceRegion, Int)]], reads: AlignmentRecordRDD): RDD[CoverageByTarget] = {
    val targetsArray = targets.value
    reads.rdd.mapPartitions(readIter => {

      val coverageByTarget = new mutable.HashMap[Int, Long].withDefaultValue(0)
      val potentialMateOverlap = new PotentialMateOverlap()

      // Scan forward to identify any overlapping targets to compute depth. Return new first target for future searches
      // as we advance through the reads
      @tailrec def scanForward(read: RichAlignmentRecord, firstIndex: Int, targetIndex: Int, mateOverlap: Boolean): Int = {
        if (targetIndex >= targetsArray.length)
          return firstIndex
        else {
          val targetRegion = targetsArray(targetIndex)._1
          if (readOverlapsTarget(read, targetRegion)) { //ReferenceRegion.unstranded(read).covers(targetRegion)) {

            coverageByTarget(targetIndex) += totalCoverageOfRegion(targetRegion, read)
            if (mateOverlap) {
              // Slow path: Track potentially overlapping paired reads to more precisely calculate fragment coverage
              potentialMateOverlap.addBinding(read, targetIndex)
            }

            return scanForward(read, firstIndex, targetIndex + 1, mateOverlap)
          } else if (read.getContigName == targetRegion.referenceName && read.getStart >= targetRegion.end)
            return scanForward(read, targetIndex + 1, targetIndex + 1, mateOverlap)
          else
            return firstIndex
        }
      }

      // Find starting target of sorted reads once per partition
      var firstRead = readIter.next()
      var firstTarget = findFirstTarget(targetsArray, ReferenceRegion.unstranded(firstRead))
      while (firstTarget.isDefined) {
        var movingFirstTargetIndex = scanForward(
          new RichAlignmentRecord(firstRead), firstTarget.get, firstTarget.get, couldOverlapMate(firstRead))

        breakable {
          readIter.foreach(read => {
            // Crossed a contig boundary? Reset firstRead and firstTarget for coverage analysis of next contig
            if (read.getContigName != firstRead.getContigName) {
              firstRead = read
              firstTarget = findFirstTarget(targetsArray, ReferenceRegion.unstranded(read))
              break // Should happen infrequently
            }

            potentialMateOverlap.remove(read).foreach {
              case (mate, targets) => targets.foreach(targetIndex => {
                coverageByTarget(targetIndex) -= fragmentOverlap(targetsArray(targetIndex)._1, mate, read)
              })
            }

            movingFirstTargetIndex = scanForward(
              new RichAlignmentRecord(read), movingFirstTargetIndex, movingFirstTargetIndex, couldOverlapMate(read))
          })

          firstTarget = None // Terminate while loop since we have exhausted all reads
        }

        potentialMateOverlap.clear()
      }

      coverageByTarget.map {
        case (targetIndex, totalCoverage) => CoverageByTarget(targetsArray(targetIndex), totalCoverage)
      }.toIterator
    })
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

    // TODO: Check inputs, e.g. BAM files, are in sorted order

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
        case (readsRdd, sampleIdx) => {
          // Read filtering
          val filteredReadsRdd = readsRdd.transform(rdd => rdd.filter(read => {
            !read.getDuplicateRead &&
              !read.getFailedVendorQualityChecks &&
              read.getPrimaryAlignment &&
              read.getReadMapped &&
              (minMapQ == 0 || read.getMapq >= minMapQ)
          }))

          val coverageRdd = sortedCoverageCalculation(broadcastTargetArray, filteredReadsRdd)

          val sqlContext = SQLContext.getOrCreate(coverageRdd.context)
          import sqlContext.implicits._
          val coverageDs = sqlContext.createDataset(coverageRdd)

          // Label coverage with sample ID to create (sampleId, (targetId, coverage)) RDD
          // Union as RDDs instead of datasets to minimize query planning
          // https://stackoverflow.com/questions/37612622/spark-unionall-multiple-dataframes
          coverageDs.groupBy(coverageDs("targetId")).sum("coverage")
            .withColumn("sample", lit(sampleIdx))
            .rdd.map(row => {
              (row.getInt(2), (row.getInt(0), row.getDouble(1)))
            })
        }
      }

      val sc = SparkContext.getOrCreate()
      sc.union(coverageCoordinatesPerSample)
    }

    val rdMatrix = coverageMatrixFromCoordinates(coverageCoordinates, numSamples, numTargets)
    ReadDepthMatrix(rdMatrix, samplesDriver, targetsDriver)
  }

}
