package org.bdgenomics.deca

import breeze.numerics.abs
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.deca.util.MLibUtils

/**
 * Created by mlinderman on 3/9/17.
 */
class CoverageSuite extends DecaFunSuite {

  sparkTest("computes coverage for targets") {
    val inputBam = resourceUrl("HG00107_target1.bam")
    val reads = sc.loadAlignments(inputBam.toString)

    val inputTargets = resourceUrl("EXOME.interval_list")
    val features = sc.loadFeatures(inputTargets.toString)

    val (rdMatrix, samples, targets) = Coverage.coverageMatrix(Seq(reads), features, minMapQ = 20, minBaseQ = 0)
    assert(rdMatrix.numRows() === 1 && rdMatrix.numCols() === 300)

    val matrix = MLibUtils.mllibMatrixToDenseBreeze(rdMatrix)
    println(matrix)
    assert(abs(matrix(0, 0) - 18.31) < .05) // Should be target 22:16448824-16449023
    assert(matrix(0, 1) > 0)
    assert(matrix(0, 2) === 0.0)
  }
}
