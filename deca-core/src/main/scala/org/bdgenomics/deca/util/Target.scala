package org.bdgenomics.deca.util

import org.bdgenomics.adam.models.ReferenceRegion

/**
 * Created by mlinderman on 6/5/17.
 */
object Target {

  /**
   * Generates ReferenceRegion from compact, inclusive region description
   *
   * @param region String of "contig:start-end"
   * @return Equivalent ReferenceRegion
   */
  def regionToReferenceRegion(region: String): ReferenceRegion = {
    val fields = region.split(Array(':', '-'))
    new ReferenceRegion(fields(0), fields(1).toLong - 1, fields(2).toLong)
  }

}
