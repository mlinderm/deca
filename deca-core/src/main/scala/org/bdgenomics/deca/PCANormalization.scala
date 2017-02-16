package org.bdgenomics.deca

import breeze.stats.meanAndVariance
import org.apache.spark.mllib.linalg.distributed.{ IndexedRow, IndexedRowMatrix, RowMatrix }

object PCANormalization extends Serializable {

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
}
