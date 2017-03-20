package org.bdgenomics.deca

import breeze.numerics.abs
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.ADAMContext._

/**
 * Created by mlinderman on 3/9/17.
 */
class CoverageSuite extends DecaFunSuite {

  sparkTest("computes coverage for targets") {
    val inputBam = resourceUrl("HG00107_target1.bam")
    val reads = sc.loadAlignments(inputBam.toString)

    val inputTargets = resourceUrl("EXOME.interval_list")
    val targets = sc.loadFeatures(inputTargets.toString)

    val coverage = Coverage.targetCoverage(targets, reads, 20, 0)
    coverage.cache()

    assert(targets.rdd.count === coverage.count)

    val result = coverage.lookup(ReferenceRegion("22", 16448824, 16449023))
    assert(abs(result.head - 18.31) < .05) // Error limit determined by rounding in XHMM tutorial data
  }
}
