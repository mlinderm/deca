/**
 * Created by mlinderman on 2/14/17.
 */
package org.bdgenomics.deca

import breeze.linalg.{ DenseMatrix, DenseVector }
import breeze.numerics.abs
import org.apache.spark.mllib.linalg.distributed.{ IndexedRow, IndexedRowMatrix }

class NormalizationSuite extends DecaFunSuite {
  /**
   * Margin to use for comparing numerical values.
   */
  val thresh = 1e-8

  val denseData = Array(
    IndexedRow(0, MLibUtils.breezeVectorToMLlib(DenseVector(0.5, 1.0, 1.5))),
    IndexedRow(1, MLibUtils.breezeVectorToMLlib(DenseVector(1.0, 2.0, 3.0))))

  def aboutEq(a: DenseMatrix[Double], b: DenseMatrix[Double], thresh: Double): Boolean = {
    require(a.rows == b.rows && a.cols == b.cols, "Matrices must be the same size.")
    abs(a - b).toArray.forall(_ < thresh)
  }

  def aboutEq(a: DenseMatrix[Double], b: DenseMatrix[Double]): Boolean = {
    require(a.rows == b.rows && a.cols == b.cols, "Matrices must be the same size.")
    abs(a - b).toArray.forall(_ < thresh)
  }

  sparkTest("filter targets by mean and SD") {
    val rdPath = resourceUrl("DATA.RD.txt")
    val lines = sc.textFile(rdPath.toString)
    assert(lines.count() === 31)
  }

  sparkTest("mean centers data by columns") {
    val matrix = new IndexedRowMatrix(sc.parallelize(denseData))
    val result = new DenseMatrix[Double](2, 3, Array(-0.25, 0.25, -0.5, 0.5, -0.75, 0.75))

    val centered = Normalization.meanCenterColumns(matrix)
    assert(aboutEq(result, MLibUtils.mllibMatrixToDenseBreeze(centered)))
  }

  sparkTest("zScores data by rows") {
    val matrix = new IndexedRowMatrix(sc.parallelize(denseData))
    val result = new DenseMatrix[Double](2, 3, Array(-1.0, -1.0, 0.0, 0.0, 1.0, 1.0))

    val centered = Normalization.zscoreRows(matrix)
    assert(aboutEq(result, MLibUtils.mllibMatrixToDenseBreeze(centered)))
  }

  sparkTest("removes top k components of data") {
    val matrix = new IndexedRowMatrix(sc.parallelize(denseData))
    // Computed via R
    val result = new DenseMatrix[Double](2, 3, Array(-1.665335e-16, 1.665335e-16, -1.665335e-16, 1.665335e-16, -2.220446e-16, 2.220446e-16))

    val centered = Normalization.meanCenterColumns(matrix)
    val matrixStar = Normalization.pcaNormalization(centered)
    assert(aboutEq(result, MLibUtils.mllibMatrixToDenseBreeze(matrixStar), 5e-16))
  }
}
