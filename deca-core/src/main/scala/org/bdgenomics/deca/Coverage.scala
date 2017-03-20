package org.bdgenomics.deca

import breeze.linalg.{ DenseVector, convert, min }
import breeze.stats.mean
import htsjdk.samtools.{ CigarElement, CigarOperator }
import org.apache.spark.mllib.linalg.{ Vector => SparkVector }
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.feature.FeatureRDD
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD
import org.bdgenomics.adam.rich.RichAlignmentRecord

import scala.collection.JavaConversions._

/**
 * Created by mlinderman on 3/8/17.
 */
object Coverage {

  def addReadToPileup(region: ReferenceRegion, read: RichAlignmentRecord, pileup: DenseVector[Int], minBaseQ: Int): DenseVector[Int] = {
    var targetIdx = (read.getStart - region.start).toInt
    var readIdx: Int = 0
    read.samtoolsCigar.foreach(cigar => {
      // TODO: can end if targetIdx >= pileup.length
      val op = cigar.getOperator
      (op.consumesReadBases, op.consumesReferenceBases) match {
        case (true, true) => {
          // TODO: can shift forward by a chunk if targetIdx < 0
          for (i <- 0 until cigar.getLength) {
            if (targetIdx >= 0 && targetIdx < pileup.length && (minBaseQ == 0 || read.qualityScores(readIdx) >= minBaseQ)) {
              pileup(targetIdx) = pileup(targetIdx) + 1
            }
            targetIdx += 1
            readIdx += 1
          }
        }
        case (false, true)  => targetIdx += cigar.getLength
        case (true, false)  => readIdx += cigar.getLength // May need distinguish between soft clip and insertion
        case (false, false) => ;
      }
    })
    pileup
  }

  def targetCoverage(targets: FeatureRDD, reads: AlignmentRecordRDD, minMapQ: Int = 0, minBaseQ: Int = 0): RDD[(ReferenceRegion, Double)] = {
    // Basic read filtering
    val filteredReads = reads.transform(rdd => rdd.filter(read => {
      !read.getDuplicateRead && read.getReadMapped && read.getMapq >= minMapQ
    }))

    // Compute left outer join of targets with reads through combination of inner join and co-group
    val coveredIntervals = targets.shuffleRegionJoin(filteredReads)
    val coverage = targets.rdd.keyBy(feature => feature).cogroup(coveredIntervals.rdd).map {
      case (feature, (features, reads)) => {
        val region = ReferenceRegion.unstranded(feature)

        if (reads.isEmpty)
          (region, 0.0)
        else {
          // Compute the coverage over this interval accounting for CIGAR string and any fragments
          var pileup = DenseVector.zeros[Int](region.length.toInt)
          reads.foreach(read => {
            // TODO: Count by fragment not by read
            pileup = addReadToPileup(region, new RichAlignmentRecord(read), pileup, minBaseQ)
          })
          (region, mean(convert(pileup, Double)))
        }
      }
    }

    coverage
  }

}
