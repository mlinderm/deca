package org.bdgenomics.deca.util

/**
 * Created by mlinderman on 4/11/17.
 */
object Phred {
  def phred(prob: BigDecimal, max: Int = 99): Int = {
    val actual = (-10.0 * math.log10(1.0 - prob.doubleValue())).toInt
    math.min(actual, max)
  }
}
