package org.bdgenomics.deca.coverage

import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix
import org.bdgenomics.adam.models.ReferenceRegion

/**
 * Created by mlinderman on 4/4/17.
 */

case class ReadDepthMatrix(depth: IndexedRowMatrix, samples: Array[String], targets: Array[ReferenceRegion]) {
  assert(
    depth.numRows() == samples.length && depth.numCols() == targets.length,
    "Size of depth matrix not consistent with samples and targets")

  def numSamples() = samples.length

  def numTargets() = targets.length
}
