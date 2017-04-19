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
    println(features.sequences.records)

    val depths = Coverage.coverageMatrix(Seq(reads), features, minMapQ = 20, minBaseQ = 0)
    assert(depths.numSamples() === 1 && depths.numTargets() === 300)

    val matrix = MLibUtils.mllibMatrixToDenseBreeze(depths.depth)

    assert(abs(matrix(0, 0) - 18.31) < .05) // Should be target 22:16448824-16449023
    assert(matrix(0, 1) > 0) // There are reads overlapping next target
    assert(matrix(0, 2) === 0.0)
  }
}
