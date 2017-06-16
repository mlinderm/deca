package org.bdgenomics.deca.serialization

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import org.bdgenomics.adam.serialization.ADAMKryoRegistrator

/**
 * Created by mlinderman on 6/16/17.
 */

class DECAKryoRegistrator extends KryoRegistrator {

  private val akr = new ADAMKryoRegistrator()

  override def registerClasses(kryo: Kryo) {

    // Register ADAM's requirements
    akr.registerClasses(kryo)

    // breeze.linalg
    kryo.register(classOf[breeze.linalg.DenseVector[Double]])
    kryo.register(classOf[breeze.linalg.DenseVector[Boolean]])
    kryo.register(classOf[breeze.linalg.DenseVector$mcD$sp])
    kryo.register(classOf[breeze.linalg.BitVector])
    kryo.register(classOf[breeze.linalg.DenseMatrix$mcD$sp])

    // org.apache.spark.mllib
    kryo.register(classOf[org.apache.spark.mllib.linalg.DenseMatrix])
    kryo.register(classOf[org.apache.spark.mllib.linalg.DenseVector])
    kryo.register(classOf[org.apache.spark.mllib.linalg.distributed.IndexedRow])
    kryo.register(classOf[scala.Array[org.apache.spark.mllib.linalg.distributed.IndexedRow]])
    kryo.register(classOf[org.apache.spark.mllib.stat.MultivariateOnlineSummarizer])

    // org.bdgenomics.deca
    kryo.register(classOf[org.bdgenomics.deca.hmm.TransitionProbabilities])

    // scala
    kryo.register(classOf[scala.Array[Boolean]])
    kryo.register(classOf[scala.Array[Double]])
  }
}
