package org.bdgenomics.deca.hmm
import java.util

import breeze.linalg.{ argmax, sum, DenseMatrix => BDM, DenseVector => BDV }
import org.apache.commons.math3.distribution.NormalDistribution
import org.apache.spark.mllib.linalg.{ DenseVector => SDV, Vector => SV }
import org.bdgenomics.deca.util.{ MLibUtils, Phred }
import org.bdgenomics.formats.avro.{ Feature, OntologyTerm }

import scala.collection.mutable.HashMap

/**
 * Created by mlinderman on 4/5/17.
 */

class SampleModel(obs: BDV[Double], transProb: TransitionProbabilities, M: Double, p: Double) {

  private lazy val delDist = new NormalDistribution(-M, 1)
  private lazy val dipDist = new NormalDistribution(0, 1)
  private lazy val dupDist = new NormalDistribution(M, 1)

  private def emitDist(t: Int): FixedVector = {
    val my_obs = obs(t)
    FixedVector(delDist.density(my_obs), dipDist.density(my_obs), dupDist.density(my_obs))
  }

  private def emitDist(t: Int, kind: Int): Double = {
    val my_obs = obs(t)
    kind match {
      case 0 => delDist.density(my_obs)
      case 1 => dipDist.density(my_obs)
      case 2 => dupDist.density(my_obs)
      case _ => throw new IndexOutOfBoundsException(kind + " not in 0-2")
    }
  }

  private def kind2Type(kind: Int) = {
    kind match {
      case 0 => "DEL"
      case 1 => "DIP"
      case 2 => "DUP"
      case _ => throw new IndexOutOfBoundsException(kind + " not in 0-2")
    }
  }

  lazy val fwdCache = {
    val fwd = Array.ofDim[FixedVector](obs.length)

    fwd(0) = FixedVector(p, 1 - 2 * p, p) :* emitDist(0)
    for (t <- 1 until obs.length) {
      val trans = transProb.matrix(t)
      fwd(t) = (fwd(t - 1) * trans) :* emitDist(t)
    }

    fwd
  }

  lazy val totalLikelihood = fwdCache(obs.length - 1).sum()

  def exact_probability(t1: Int, t2: Int, kind: Int, bwd: BigDecimal): BigDecimal = {
    var prob = fwdCache(t1)(kind) * bwd
    for (t <- t1 + 1 to t2) {
      prob *= transProb.edge(t, kind, kind) * emitDist(t, kind)
    }
    prob / totalLikelihood
  }

  def discoverCNVs(): Seq[Feature] = {
    val features = scala.collection.mutable.ArrayBuffer.empty[Feature]

    // Compute backward probabilities without backward caching while looking for CNVs
    var prevBwd = FixedVector(1.0, 1.0, 1.0)
    var currentCNV: (Int, Int, FixedVector) = null

    for (t <- (0 until obs.length - 1).reverse) {
      val trans = transProb.matrix(t + 1)

      val bwd = trans * (emitDist(t + 1) :* prevBwd)
      val gamma = fwdCache(t) :* bwd

      val kind = gamma.argmax()
      if (kind != 1 && currentCNV == null) {
        // Start of a new CNV
        currentCNV = (t, kind, bwd)
      } else if (currentCNV != null && kind != currentCNV._2) {
        // End of a CNV and possibly the start of another
        val cnvStart = t + 1
        val (cnvEnd, cnvKind, cnvBwd) = currentCNV

        // Compute the various probabilities for the CNV
        val exact = Phred.phred(exact_probability(cnvStart, cnvEnd, cnvKind, cnvBwd(cnvKind)))

        val cnvAttr = new util.HashMap[String, String]()
        cnvAttr.put("START_TARGET", cnvStart.toString)
        cnvAttr.put("END_TARGET", cnvEnd.toString)
        cnvAttr.put("Q_EXACT", exact.toInt.toString)

        val builder = Feature.newBuilder()
        builder.setFeatureType(kind2Type(cnvKind))
        builder.setScore(exact.toDouble)
        builder.setAttributes(cnvAttr)
        features += builder.build()

        // Are we back to diploid, or did we start another different CNV
        currentCNV = if (kind != 1) (t, kind, bwd) else null
      }

      prevBwd = bwd
    }

    features
  }
}

object SampleModel {
  def apply(obs: SV, transProb: TransitionProbabilities, M: Double, p: Double): SampleModel = {
    new SampleModel(MLibUtils.mllibVectorToDenseBreeze(obs), transProb, M, p)
  }
}
