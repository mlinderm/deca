/**
 * Created by mlinderman on 2/14/17.
 */
package org.bdgenomics.deca

class PCANormalizationSuite extends DecaFunSuite {

  sparkTest("filter targets by mean and SD") {
    val rdPath = resourceUrl("DATA.RD.txt")
    val lines = sc.textFile(rdPath.toString)
    assert(lines.count() === 31)
  }
}
