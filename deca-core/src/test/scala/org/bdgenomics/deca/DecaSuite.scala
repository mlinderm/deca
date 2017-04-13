package org.bdgenomics.deca

import java.io.File

import com.google.common.io.Files
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.deca.util.MLibUtils

/**
 * Created by mlinderman on 2/21/17.
 */
class DecaSuite extends DecaFunSuite {

  val tempDir = Files.createTempDir()

  sparkTest("read XHMM matrix") {
    val rdPath = resourceUrl("DATA.RD.txt")
    val matrix = Deca.readXHMMMatrix(rdPath.toString)

    assert(matrix.numSamples() === 30)
    assert(matrix.samples(1) === "HG00096")

    assert(matrix.numTargets() === 300)
    assert(matrix.targets(3) === ReferenceRegion("22", 16449424, 16449804))

    val depth = MLibUtils.mllibMatrixToDenseBreeze(matrix.depth)
    assert(depth(2, 3) === 35.16)
    assert(depth(29, 299) === 72.96)
  }

  sparkTest("write XHMM matrix") {
    val rdPath = resourceUrl("DATA.RD.txt")
    val matrix = Deca.readXHMMMatrix(rdPath.toString)

    val outPath = new File(tempDir, "test.txt")

    Deca.writeXHMMMatrix(matrix, outPath.getAbsolutePath)
    assert(outPath.exists)

    val readMatrix = Deca.readXHMMMatrix(outPath.getAbsolutePath)
    assert(readMatrix.targets.sameElements(matrix.targets))
    // There is no guarantee that the rows are in the same order
    assert(readMatrix.numSamples() == matrix.numSamples() && readMatrix.numTargets() == matrix.numTargets())
  }

}
