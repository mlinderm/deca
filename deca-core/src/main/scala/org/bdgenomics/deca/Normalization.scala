package org.bdgenomics.deca

import breeze.linalg.sum
import breeze.stats.meanAndVariance
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.distributed.{ IndexedRow, IndexedRowMatrix }
import org.bdgenomics.utils.misc.Logging
import util.control.Breaks._

object Normalization extends Serializable with Logging {

  def meanCenterColumns(matrix: IndexedRowMatrix): IndexedRowMatrix = {
    val colStats = matrix.toRowMatrix.computeColumnSummaryStatistics
    new IndexedRowMatrix(matrix.rows.map(row => {
      IndexedRow(row.index,
        MLibUtils.breezeVectorToMLlib(MLibUtils.mllibVectorToDenseBreeze(row.vector) - MLibUtils.mllibVectorToDenseBreeze(colStats.mean)))
    }))
  }

  def zscoreRows(matrix: IndexedRowMatrix): IndexedRowMatrix = {
    new IndexedRowMatrix(matrix.rows.map(row => {
      val vector = MLibUtils.mllibVectorToDenseBreeze(row.vector)
      val stats = meanAndVariance(vector)
      IndexedRow(row.index, MLibUtils.breezeVectorToMLlib((vector - stats.mean) / Math.sqrt(stats.variance)))
    }))
  }

  def pcaNormalization(readMatrix: IndexedRowMatrix, pveMeanFactor: Double = 0.7): IndexedRowMatrix = {
    val svd = readMatrix.computeSVD(Math.min(readMatrix.numRows, readMatrix.numCols).toInt, computeU = false)

    // Determine components to remove
    var toRemove = svd.s.size
    breakable {
      val S = MLibUtils.mllibVectorToDenseBreeze(svd.s)
      val componentVar = S :* S
      val cutoff: Double = (sum(componentVar) / componentVar.length) * pveMeanFactor
      for (c <- 0 until componentVar.length) {
        if (componentVar(c) < cutoff) {
          toRemove = c
          break
        }
      }
    }
    log.info("Removing top %d components in PCA normalization", toRemove)

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
}
