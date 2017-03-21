package org.bdgenomics.deca

import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import org.apache.spark.rdd.RDD
import org.apache.commons.math3.distribution.NormalDistribution
import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable

/**
 * Created by davinchia on 3/20/17.
 */
object DistXHMM {
  def runXHMM(mat: IndexedRowMatrix, transitions: Broadcast[Array[Array[Array[BigDecimal]]]]): Unit = {
    val numCols = mat.numCols()

    var samples: RDD[Sample] = mat.rows.map( row => {
      new Sample (row.vector.toArray, numCols)
    })

    samples.cache()
    samples.foreach( sample => {
      println(sample.toString)
    })

    samples = samples.map( e => {
      e.emissions   = e.observations map ( a => calc_emission(a) )
      e.forward     = calc_forward(e.observations.length, e.emissions, transitions.value)
      e.backward    = calc_backward(e.observations.length, e.emissions, transitions.value)
      e.viterbiPath = calc_viterbi(e.fwd, e.bck)
      e
    })
    return samples
  }

  private def calc_emission(ob: Double): Array[BigDecimal] = {
    val normDistDip : NormalDistribution = new NormalDistribution(0.0, 1.0)
    val normDistDel : NormalDistribution = new NormalDistribution(-3, 1.0)
    val normDistDup : NormalDistribution = new NormalDistribution(3, 1.0)
    Array(BigDecimal(normDistDel.density(ob)), BigDecimal(normDistDip.density(ob)), BigDecimal(normDistDup.density(ob)))
  }

  private def calc_forward(len: Int, emissions: Array[Array[BigDecimal]], transitions: Array[Array[Array[BigDecimal]]]): Array[Array[BigDecimal]] = {
    var fwdCache = Array.ofDim[BigDecimal](3, len)
    val states = (0 to 2)

    fwdCache(0)(0) = 0.01 * emissions(0)(0)
    fwdCache(1)(0) = 0.98 * emissions(0)(1)
    fwdCache(2)(0) = 0.01 * emissions(0)(2)

    for (i <- 1 until len) {
      for (s <- states) {
        fwdCache(s)(i) = (states map { e =>
          fwdCache(e)(i-1) * transitions(i)(e)(s)
        } sum) * emissions(i)(s)
      }
    }

    fwdCache
  }

  private def calc_backward(len: Int, emissions: Array[Array[BigDecimal]], transitions: Array[Array[Array[BigDecimal]]]): Array[Array[BigDecimal]] = {
    var bckCache = Array.ofDim[BigDecimal](3, len+1)
    val states = (0 to 2)

    bckCache(0)(len) = 1
    bckCache(1)(len) = 1
    bckCache(2)(len) = 1

    for (i <- (1 to len-1).reverse) {
      for (s <- states) {
        bckCache(s)(i) = states map { e =>
          transitions(i)(s)(e) * emissions(i)(e) * bckCache(e)(i+1)
        } sum
      }
    }

    bckCache
  }

  private def calc_viterbi(fwdCache: Array[Array[BigDecimal]], bckCache: Array[Array[BigDecimal]]): Array[Int] = {
    var path : mutable.ListBuffer[Int] = new mutable.ListBuffer[Int]
    val states = (0 to 2)
    for (t <- 0 until fwdCache(0).length) {
      path += (states map { (s) => (fwdCache(s)(t) * bckCache(s)(t+1), s)
      } maxBy (_._1))._2
    }
    path.toArray[Int]
  }
}
