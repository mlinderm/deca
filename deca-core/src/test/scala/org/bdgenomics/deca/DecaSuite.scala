package org.bdgenomics.deca

import java.io.File

import com.google.common.io.Files
import org.bdgenomics.adam.models.ReferenceRegion

import scala.io.Source

/**
 * Created by mlinderman on 2/21/17.
 */
class DecaSuite extends DecaFunSuite {

  val tempDir = Files.createTempDir()

  sparkTest("read XHMM matrix") {
    val rdPath = resourceUrl("DATA.RD.txt")
    val (rdMatrix, samples, targets) = Deca.readXHMMMatrix(rdPath.toString)

    assert(samples.length === 30)
    assert(samples(1) === "HG00096")

    assert(targets.length === 300)
    assert(targets(3) === ReferenceRegion("22", 16449424, 16449804))

    assert(rdMatrix.numRows() === 30 && rdMatrix.numCols() === 300)
    val matrix = MLibUtils.mllibMatrixToDenseBreeze(rdMatrix)
    assert(matrix(2, 3) === 35.16)
    assert(matrix(29, 299) === 72.96)
  }

  sparkTest("write XHMM matrix") {
    val rdPath = resourceUrl("DATA.RD.txt")
    val (rdMatrix, samples, targets) = Deca.readXHMMMatrix(rdPath.toString)

    val outPath = new File(tempDir, "test.txt")

    Deca.writeXHMMMatrix(rdMatrix, samples, targets, outPath.getAbsolutePath)
    assert(outPath.exists)

    val (readRdMatrix, readSamples, readTargets) = Deca.readXHMMMatrix(outPath.getAbsolutePath)
    assert(readTargets.sameElements(targets))
    // There is no guarantee that the rows are in the same order
    assert(readRdMatrix.numRows == rdMatrix.numRows && readRdMatrix.numCols == rdMatrix.numCols)
  }

}
