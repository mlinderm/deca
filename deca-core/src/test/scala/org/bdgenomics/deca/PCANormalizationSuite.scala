/**
 * Created by mlinderman on 2/14/17.
 */
package org.bdgenomics.deca

import breeze.linalg.{ DenseMatrix, DenseVector }
import breeze.numerics.abs
import org.apache.spark.mllib.linalg.distributed.{ IndexedRow, IndexedRowMatrix }

class PCANormalizationSuite extends DecaFunSuite {
  /**
   * Margin to use for comparing numerical values.
   */
  val thresh = 1e-8

  val denseData = Array(
    IndexedRow(0, MLibUtils.breezeVectorToMLlib(DenseVector(0.5, 1.0, 1.5))),
    IndexedRow(1, MLibUtils.breezeVectorToMLlib(DenseVector(1.0, 2.0, 3.0))))

  def aboutEq(a: DenseMatrix[Double], b: DenseMatrix[Double]): Boolean = {
    require(a.rows == b.rows && a.cols == b.cols, "Matrices must be the same size.")
    abs(a - b).toArray.forall(_ < thresh)
  }

  sparkTest("mean centers data by columns") {
    val matrix = new IndexedRowMatrix(sc.parallelize(denseData))
    val result = new DenseMatrix[Double](2, 3, Array(-0.25, 0.25, -0.5, 0.5, -0.75, 0.75))

    val centered = PCANormalization.meanCenterColumns(matrix)
    assert(aboutEq(result, MLibUtils.mllibMatrixToDenseBreeze(centered)))
  }

  sparkTest("zScores data by rows") {
    val matrix = new IndexedRowMatrix(sc.parallelize(denseData))
    val result = new DenseMatrix[Double](2, 3, Array(-1.0, -1.0, 0.0, 0.0, 1.0, 1.0))

    val centered = PCANormalization.zscoreRows(matrix)
    assert(aboutEq(result, MLibUtils.mllibMatrixToDenseBreeze(centered)))
  }

  sparkTest("filter targets by mean and SD") {
    val rdPath = resourceUrl("DATA.RD.txt")
    val lines = sc.textFile(rdPath.toString)
    assert(lines.count() === 31)
  }
}
