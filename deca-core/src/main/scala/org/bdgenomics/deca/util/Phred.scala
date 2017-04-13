package org.bdgenomics.deca.util

/**
 * Created by mlinderman on 4/11/17.
 */
object Phred {
  def phred(prob: BigDecimal, max: Double = 99.0): Double = {
    val actual = (-10.0 * math.log10(1.0 - prob.doubleValue()))
    math.min(actual, max)
  }
}
