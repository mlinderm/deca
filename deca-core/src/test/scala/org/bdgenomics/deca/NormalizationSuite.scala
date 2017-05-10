/**
 * Created by mlinderman on 2/14/17.
 */
package org.bdgenomics.deca

import breeze.linalg.{ DenseMatrix, DenseVector, max }
import breeze.numerics.abs
import org.apache.spark.mllib.linalg.distributed.{ IndexedRow, IndexedRowMatrix }
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.deca.util.MLibUtils

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

  ignore("removes top k components of data") {
    val matrix = new IndexedRowMatrix(sc.parallelize(denseData))
    // Computed via R
    val result = new DenseMatrix[Double](2, 3, Array(-1.665335e-16, 1.665335e-16, -1.665335e-16, 1.665335e-16, -2.220446e-16, 2.220446e-16))

    val centered = Normalization.meanCenterColumns(matrix)
    val matrixStar = Normalization.pcaNormalization(centered)
    assert(aboutEq(result, MLibUtils.mllibMatrixToDenseBreeze(matrixStar), thresh = 5e-16))
  }

  sparkTest("filters and centers SVD results") {
    val matrix = Deca.readXHMMMatrix(resourceUrl("DATA.PCA_normalized.txt").toString)
    val resultMatrix = Deca.readXHMMMatrix(resourceUrl("DATA.PCA_normalized.filtered.sample_zscores.RD.txt").toString)

    val (targFilteredRdStar, targFilteredRdStarTargets) = Normalization.filterColumns(matrix.depth, matrix.targets, maxSD = 30.0)
    val zMatrix = Normalization.zscoreRows(targFilteredRdStar)
    assert(aboutEq(MLibUtils.mllibMatrixToDenseBreeze(resultMatrix.depth), MLibUtils.mllibMatrixToDenseBreeze(zMatrix)))
    assert(targFilteredRdStarTargets.sameElements(resultMatrix.targets))
  }

  sparkTest("filters and normalizes read depth data") {
    // To match XHMM need to filter out low complexity and extreme GC targets. For the example data, this is just
    // 22:19770437-19770545
    // And also filter out targets with length < 10 and > 10000
    val matrix = Deca.readXHMMMatrix(resourceUrl("DATA.RD.txt").toString,
      Array(ReferenceRegion("22", 19770436L, 19770545L)),
      minTargetLength = 10L, maxTargetLength = 10000L)

    val resultMatrix = Deca.readXHMMMatrix(resourceUrl("DATA.PCA_normalized.filtered.sample_zscores.RD.txt").toString)

    val (zMatrix, zTargets) = Normalization.normalizeReadDepth(matrix.depth, matrix.targets)

    // Max observed difference was 1.02e-4 between XHMM results and this implementation
    assert(aboutEq(
      MLibUtils.mllibMatrixToDenseBreeze(resultMatrix.depth), MLibUtils.mllibMatrixToDenseBreeze(zMatrix), thresh = 2e-4))
    assert(zTargets.sameElements(resultMatrix.targets))
  }
}
