package org.bdgenomics.deca

import org.bdgenomics.adam.models.ReferenceRegion

/**
 * Created by mlinderman on 2/21/17.
 */
class DecaSuite extends DecaFunSuite {

  sparkTest("read XHMM matrix") {
    val rdPath = resourceUrl("DATA.RD.txt")
    val (samples, targets, rdMatrix) = Deca.readXHMMMatrix(rdPath.toString)

    assert(samples.length === 30)
    assert(samples(1) === "HG00096")

    assert(targets.length === 300)
    assert(targets(3) === ReferenceRegion("22", 16449424, 16449804))

    assert(rdMatrix.numRows() === 30 && rdMatrix.numCols() === 300)
    val matrix = MLibUtils.mllibMatrixToDenseBreeze(rdMatrix)
    assert(matrix(2, 3) === 35.16)
    assert(matrix(29, 299) === 72.96)
  }

}
