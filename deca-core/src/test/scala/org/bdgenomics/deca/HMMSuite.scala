package org.bdgenomics.deca

import breeze.linalg.DenseMatrix
import breeze.numerics.abs
import org.bdgenomics.deca.coverage.ReadDepthMatrix
import org.bdgenomics.deca.hmm.{ FixedMatrix, TransitionProbabilities }

/**
 * Created by mlinderman on 4/5/17.
 */
class HMMSuite extends DecaFunSuite {

  /**
   * Margin to use for comparing numerical values.
   */
  val thresh = 1e-8

  def aboutEq(a: FixedMatrix, b: Array[Double], thresh: Double = thresh): Boolean = {
    require(a.rows * a.cols == b.length, "Matrices must be the same size.")
    a.toArray.zip(b).forall(pair => (pair._1 - pair._2).abs < thresh)

  }

  sparkTest("Generates transition matrix for array of distances") {
    val matrix = Deca.readXHMMMatrix(resourceUrl("DATA.PCA_normalized.filtered.sample_zscores.RD.txt").toString)

    val transProb = TransitionProbabilities(matrix.targets, D = 70000, p = 1.0e-8, q = 1.0 / 6.0)

    {
      val expTrans = Array[Double](
        0.8333214286, 1.0e-8, 1.4285612245e-13, 0.1666785713, 0.99999998000, 0.1666785713, 1.4285612245e-13, 1.0e-8, 0.8333214286)
      assert(aboutEq(transProb.matrix(1), expTrans))
    }

  }

  sparkTest("Discovers CNVs") {
    val matrix = Deca.readXHMMMatrix(resourceUrl("DATA.PCA_normalized.filtered.sample_zscores.RD.txt").toString)

    val cnvs = HMM.discoverCNVs(matrix)
    assert(cnvs.rdd.count === 2)
    cnvs.rdd.collect.map(println(_))
  }
}
