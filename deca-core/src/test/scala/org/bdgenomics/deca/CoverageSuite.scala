package org.bdgenomics.deca

import breeze.numerics.abs
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.deca.util.MLibUtils

/**
 * Created by mlinderman on 3/9/17.
 */
class CoverageSuite extends DecaFunSuite {

  sparkTest("Finds correct first target") {
    val inputTargets = resourceUrl("EXOME.interval_list")
    val features = sc.loadFeatures(inputTargets.toString)

    val targetsDriver = features.rdd
      .map(ReferenceRegion.unstranded(_))
      .collect
      .zipWithIndex
      .sortBy(_._1)

    assert(Coverage.findFirstTarget(targetsDriver, ReferenceRegion("22", 16448862, 16448952)) === Some(0))
    assert(Coverage.findFirstTarget(targetsDriver, ReferenceRegion("22", 16448800, 16448810)) === Some(0))
    assert(Coverage.findFirstTarget(targetsDriver, ReferenceRegion("22", 20074000, 20074010)).isEmpty)
    assert(Coverage.findFirstTarget(targetsDriver, ReferenceRegion("22", 17071000, 17071010)) === Some(4))
    assert(Coverage.findFirstTarget(targetsDriver, ReferenceRegion("22", 16449200, 16449300)) === Some(1))
    assert(Coverage.findFirstTarget(targetsDriver, ReferenceRegion("22", 16448824, 16449024)) === Some(0))
    assert(Coverage.findFirstTarget(targetsDriver, ReferenceRegion("22", 16448834, 16449024)) === Some(0))
    assert(Coverage.findFirstTarget(targetsDriver, ReferenceRegion("22", 16448804, 16449000)) === Some(0))
    assert(Coverage.findFirstTarget(targetsDriver, ReferenceRegion("21", 16448804, 16449000)) === Some(0))
    assert(Coverage.findFirstTarget(targetsDriver, ReferenceRegion("22", 19127400, 19127500)) === Some(156))
  }

  sparkTest("computes coverage for targets") {
    val inputBam = resourceUrl("HG00107_target1.bam")
    val reads = sc.loadAlignments(inputBam.toString)

    val inputTargets = resourceUrl("EXOME.interval_list")
    val features = sc.loadFeatures(inputTargets.toString)

    val depths = Coverage.coverageMatrix(Seq(reads), features, minMapQ = 20)
    assert(depths.numSamples() === 1 && depths.numTargets() === 300)

    val matrix = MLibUtils.mllibMatrixToDenseBreeze(depths.depth)

    assert(abs(matrix(0, 0) - 18.31) < 0.005) // Should be target 22:16448824-16449023
    assert(abs(matrix(0, 1) - 58.86) < 0.005) // There are reads overlapping next target
    assert(matrix(0, 2) === 0.0)
  }
}
