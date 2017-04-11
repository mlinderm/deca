package org.bdgenomics.deca.hmm

/**
 * Created by mlinderman on 4/7/17.
 */
class FixedVector(val v0: BigDecimal, val v1: BigDecimal, val v2: BigDecimal) {
  def this(data: Array[BigDecimal]) = this(data(0), data(1), data(2))
  def this(data: Array[Double]) = this(data(0), data(1), data(2))

  def apply(i: Int): BigDecimal = {
    if (i == 0) return v0
    else if (i == 1) return v1
    else if (i == 2) return v2
    else
      throw new IndexOutOfBoundsException(i + " not in 0-2")
  }

  def :*(that: FixedVector): FixedVector = {
    new FixedVector(v0 * that.v0, v1 * that.v1, v2 * that.v2)
  }

  def *(that: FixedMatrix): FixedVector = {
    new FixedVector(
      v0 * that.v0_0 + v1 * that.v1_0 + v2 * that.v2_0,
      v0 * that.v0_1 + v1 * that.v1_1 + v2 * that.v2_1,
      v0 * that.v0_2 + v1 * that.v1_2 + v2 * that.v2_2)
  }

  def sum(): BigDecimal = v0 + v1 + v2

  def argmax(): Int = {
    if (v0 >= v1) {
      if (v0 >= v2) 0 else 2
    } else {
      if (v1 >= v2) 1 else 2
    }
  }
}

object FixedVector {
  def apply(v0: BigDecimal, v1: BigDecimal, v2: BigDecimal): FixedVector = {
    new FixedVector(v0, v1, v2)
  }
  def apply(v0: Double, v1: Double, v2: Double): FixedVector = {
    new FixedVector(v0, v1, v2)
  }
}
