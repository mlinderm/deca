package org.bdgenomics.deca

import breeze.linalg.sum
import breeze.stats.mean
import breeze.numerics.sqrt
import breeze.stats.meanAndVariance
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.distributed.{ IndexedRow, IndexedRowMatrix }
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.deca.Timers._
import org.bdgenomics.deca.coverage.ReadDepthMatrix
import org.bdgenomics.deca.util.MLibUtils
import org.bdgenomics.utils.misc.Logging

import scala.util.control.Breaks._

object Normalization extends Serializable with Logging {

  def filterColumns(matrix: IndexedRowMatrix, targets: Array[ReferenceRegion],
                    minMean: Double = Double.MinValue,
                    maxMean: Double = Double.MaxValue,
                    minSD: Double = 0,
                    maxSD: Double = Double.MaxValue): (IndexedRowMatrix, Array[ReferenceRegion]) = {
    matrix.rows.cache()
    val colStats = matrix.toRowMatrix.computeColumnSummaryStatistics
    val colMeans = MLibUtils.mllibVectorToDenseBreeze(colStats.mean)
    val colSDs = sqrt(MLibUtils.mllibVectorToDenseBreeze(colStats.variance))

    val toKeep = (colMeans :>= minMean) :& (colMeans :<= maxMean) :& (colSDs :>= minSD) :& (colSDs :<= maxSD)
    val newTargets = targets.zipWithIndex.collect { case (target, index) if toKeep(index) => target }

    val toKeepBroadcast = SparkContext.getOrCreate().broadcast(toKeep)
    val filteredMatrix = new IndexedRowMatrix(matrix.rows.map(row => {
      val vector = MLibUtils.mllibVectorToDenseBreeze(row.vector)
      IndexedRow(row.index, MLibUtils.breezeVectorToMLlib(vector(toKeepBroadcast.value)))
    }))

    (filteredMatrix, newTargets)
  }

  def meanCenterColumns(matrix: IndexedRowMatrix): IndexedRowMatrix = {
    matrix.rows.cache()
    val colStats = matrix.toRowMatrix.computeColumnSummaryStatistics
    new IndexedRowMatrix(matrix.rows.map(row => {
      IndexedRow(
        row.index,
        MLibUtils.breezeVectorToMLlib(
          MLibUtils.mllibVectorToDenseBreeze(row.vector) - MLibUtils.mllibVectorToDenseBreeze(colStats.mean)))
    }))
  }

  def filterRows(matrix: IndexedRowMatrix, samples: Array[String],
                 minMean: Double = Double.MinValue,
                 maxMean: Double = Double.MaxValue,
                 minSD: Double = 0,
                 maxSD: Double = Double.MaxValue): IndexedRowMatrix = {
    val broadcastSamples = SparkContext.getOrCreate().broadcast(samples)
    new IndexedRowMatrix(matrix.rows.filter(row => {
      val (mean, sd) = {
        val stats = meanAndVariance(MLibUtils.mllibVectorToDenseBreeze(row.vector))
        (stats.mean, Math.sqrt(stats.variance))
      }
      val toKeep = mean >= minMean && mean <= maxMean && sd >= minSD && sd <= maxSD
      if (!toKeep)
        // https://stackoverflow.com/questions/11940614/scala-and-slf4j-pass-multiple-parameters
        log.info("Excluding sample {} with mean={} and SD={}",
          Array[AnyRef](broadcastSamples.value(row.index.toInt), new java.lang.Double(mean), new java.lang.Double(sd)): _*)
      toKeep
    }))
  }

  def zscoreRows(matrix: IndexedRowMatrix): IndexedRowMatrix = ComputeZScores.time {
    new IndexedRowMatrix(matrix.rows.map(row => {
      val vector = MLibUtils.mllibVectorToDenseBreeze(row.vector)
      val stats = meanAndVariance(vector)
      IndexedRow(row.index, MLibUtils.breezeVectorToMLlib((vector - stats.mean) / Math.sqrt(stats.variance)))
    }))
  }

  def predictKFromComponents(maxComponents: Int): Int = {
    // TODO: Refine prediction function
    Math.min(Math.ceil(0.05 * maxComponents + 10).toInt, maxComponents)
  }

  def pcaNormalization(readMatrix: IndexedRowMatrix,
                       pveMeanFactor: Double = 0.7,
                       fixedToRemove: Option[Int] = None): IndexedRowMatrix = PCANormalization.time {
    readMatrix.rows.cache()

    // Determine k for SVD
    val maxComponents = Math.min(readMatrix.numRows, readMatrix.numCols).toInt
    val k = fixedToRemove.getOrElse(predictKFromComponents(maxComponents))
    log.info("Computing SVD with k={}", k)

    // Compute SVD
    val svd = ComputeSVD.time {
      // May need to convert to RowMatrix and cache for performance reasons
      readMatrix.computeSVD(k, computeU = false)
    }

    // Determine components to remove
    var toRemove = k
    if (fixedToRemove.isEmpty) {
      breakable {
        val S = MLibUtils.mllibVectorToDenseBreeze(svd.s)
        val componentVar = S :* S

        // Compute cutoff by extending last value of S to entire length if max components had been calculated
        val cutoff: Double = pveMeanFactor * ((sum(componentVar) + (maxComponents - componentVar.length) * componentVar(-1)) / maxComponents)
        for (c <- 0 until componentVar.length) {
          if (componentVar(c) < cutoff) {
            toRemove = c
            break
          }
        }
      }

      if (toRemove > k) {
        throw new RuntimeException("Insufficient terms in SVD calculated")
      }
    }
    log.info("Removing top {} components in PCA normalization", toRemove)

    val V = MLibUtils.mllibMatrixToDenseBreeze(svd.V)

    // Broadcast subset of V matrix
    val sc = SparkContext.getOrCreate()
    val C = sc.broadcast(V(::, 0 until toRemove)) // Exclusive to toRemove

    // Remove components with row centric approach
    new IndexedRowMatrix(readMatrix.rows.map(row => {
      val rd = MLibUtils.mllibVectorToDenseBreeze(row.vector)
      var rdStar = rd.copy
      for (col <- 0 until C.value.cols) {
        val component = C.value(::, col)
        val loading = component dot rd
        rdStar :-= component * loading
      }
      IndexedRow(row.index, MLibUtils.breezeVectorToMLlib(rdStar))
    }))

  }

  def normalizeReadDepth(readMatrix: ReadDepthMatrix,
                         minTargetMeanRD: Double = 10.0,
                         maxTargetMeanRD: Double = 500.0,
                         minSampleMeanRD: Double = 25.0,
                         maxSampleMeanRD: Double = 200.0,
                         minSampleSDRD: Double = 0.0,
                         maxSampleSDRD: Double = 150.0,
                         pveMeanFactor: Double = 0.7,
                         fixedToRemove: Option[Int] = None,
                         maxTargetSDRDStar: Double = 30.0): (IndexedRowMatrix, Array[ReferenceRegion]) = NormalizeReadDepths.time {

    // Filter I: Filter extreme targets and samples, then mean center the data
    val (centeredRdMatrix, targFilteredRdTargets) = ReadDepthFilterI.time {
      val (targFilteredrdMatrix, targFilteredRdTargets) = filterColumns(
        readMatrix.depth,
        readMatrix.targets,
        minMean = minTargetMeanRD,
        maxMean = maxTargetMeanRD)

      val sampFilteredRdMatrix = filterRows(
        targFilteredrdMatrix,
        readMatrix.samples,
        minMean = minSampleMeanRD,
        maxMean = maxSampleMeanRD,
        minSD = minSampleSDRD,
        maxSD = maxSampleSDRD)

      (meanCenterColumns(sampFilteredRdMatrix), targFilteredRdTargets)
    }

    // Can't use numRows to get row count as it is based on row indices
    centeredRdMatrix.rows.cache()
    log.info("After pre-normalization filter: {} samples by {} targets", centeredRdMatrix.rows.count, targFilteredRdTargets.length)

    // PCA normalization
    val rdStarMatrix = pcaNormalization(centeredRdMatrix, pveMeanFactor = pveMeanFactor, fixedToRemove = fixedToRemove)
    centeredRdMatrix.rows.unpersist() // Let Spark know centered data no longer needed

    // Filter II: Filter extremely variable targets
    val (targFilteredRdStarMatrix, targFilteredRdStarTargets) = ReadDepthFilterII.time {
      filterColumns(rdStarMatrix, targFilteredRdTargets, maxSD = maxTargetSDRDStar)
    }
    rdStarMatrix.rows.unpersist() // Let Spark know normalized data no longer needed

    // Z-score by sample
    val zMatrix = Normalization.zscoreRows(targFilteredRdStarMatrix)

    zMatrix.rows.cache()
    log.info("After post-normalization filter: {} samples by {} targets", zMatrix.rows.count, targFilteredRdStarTargets.length)
    (zMatrix, targFilteredRdStarTargets)
  }

}
