package org.bdgenomics.deca.hmm

import com.esotericsoftware.kryo.io.{ Input, Output }
import com.esotericsoftware.kryo.{ Kryo, Serializer }
import org.bdgenomics.adam.models.ReferenceRegion

/**
 * Created by mlinderman on 4/4/17.
 */
class TransitionProbabilities(val f: Array[Double], val p: Double, val q: Double) {
  // Methods for row and columns and individual entry
  def matrix(t: Int): FixedMatrix = {
    if (t == 0) {
      new FixedMatrix(0, p, 0, 0, 1 - 2 * p, 0, 0, p, 0)
    } else {
      val ft = f(t)
      // Matrices are initialized in column order
      new FixedMatrix(
        ft * (1 - q) + (1 - ft) * p, p, (1 - ft) * p,
        ft * q + (1 - ft) * (1 - 2 * p), 1 - 2 * p, ft * q + (1 - ft) * (1 - 2 * p),
        (1 - ft) * p, p, ft * (1 - q) + (1 - ft) * p)
    }
  }

  def edge(t: Int, xt_1: Int, xt: Int): Double = {
    val ft = f(t)
    (xt_1, xt) match {
      case (0, 0) => ft * (1 - q) + (1 - ft) * p
      case (0, 1) => ft * q + (1 - ft) * (1 - 2 * p)
      case (0, 2) => (1 - ft) * p
      case (1, 0) => p
      case (1, 1) => 1 - 2 * p
      case (1, 2) => p
      case (2, 0) => (1 - ft) * p
      case (2, 1) => ft * q + (1 - ft) * (1 - 2 * p)
      case (2, 2) => ft * (1 - q) + (1 - ft) * p
      case (_, _) =>
        throw new IndexOutOfBoundsException(xt_1 + " or " + xt + " not in 0-2")
    }
  }
}

object TransitionProbabilities {
  def apply(targets: Array[ReferenceRegion], D: Double, p: Double, q: Double): TransitionProbabilities = {
    val f = new Array[Double](targets.length)
    (1 until f.length).foreach(i => {
      // TODO: Special handling of contig boundaries?
      f(i) = math.exp(-(targets(i).start - targets(i - 1).end) / D)
    })
    new TransitionProbabilities(f, p, q)
  }
}

private[deca] class TransitionProbabilitiesSerializer extends Serializer[TransitionProbabilities] {
  def write(kryo: Kryo, output: Output, obj: TransitionProbabilities) {
    output.writeInt(obj.f.length)
    obj.f.foreach(output.writeDouble(_))
    output.writeDouble(obj.p)
    output.writeDouble(obj.q)
  }

  def read(kryo: Kryo, input: Input, klazz: Class[TransitionProbabilities]): TransitionProbabilities = {
    val fSize = input.readInt()
    val f = new Array[Double](fSize)
    (0 until fSize).foreach(i => {
      f(i) = input.readDouble()
    })
    val p = input.readDouble()
    val q = input.readDouble()
    new TransitionProbabilities(f, p, q)
  }
}