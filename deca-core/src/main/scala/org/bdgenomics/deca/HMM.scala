package org.bdgenomics.deca

import org.apache.spark.SparkContext
import org.bdgenomics.adam.rdd.feature.FeatureRDD
import org.bdgenomics.deca.coverage.ReadDepthMatrix
import org.bdgenomics.deca.hmm.{ SampleModel, TransitionProbabilities }
import org.bdgenomics.formats.avro.Feature
import org.bdgenomics.utils.misc.Logging

/**
 * Created by mlinderman on 4/4/17.
 */
object HMM extends Serializable with Logging {

  def discoverCNVs(readMatrix: ReadDepthMatrix, M: Double = 3, T: Double = 6, p: Double = 1e-8, D: Double = 70000): FeatureRDD = {
    val sc = SparkContext.getOrCreate()

    // Generate transition probabilities from targets
    val transProb = sc.broadcast(TransitionProbabilities(readMatrix.targets, D = D, p = p, q = 1.0 / T))

    // Broadcast targets so that features can be fixed up
    val targets = sc.broadcast(readMatrix.targets)

    val cnvs = readMatrix.depth.rows.flatMap(obs => {
      // Create per-sample HMM model
      val model = SampleModel(obs.vector, transProb.value, M, p)

      // Discover CNVs (TODO: Add sample label)
      val per_sample_cnvs = model.discoverCNVs()
      per_sample_cnvs.map(raw_feature => {
        val attr = raw_feature.getAttributes

        val start_target = targets.value(attr.get("START_TARGET").toInt)
        val end_target = targets.value(attr.get("END_TARGET").toInt)

        // TODO: Filter out contig spanning CNVs

        val builder = Feature.newBuilder(raw_feature)
        builder.setContigName(start_target.referenceName)
        builder.setStart(start_target.start)
        builder.setEnd(end_target.end)
        builder.build()
      })
    })

    FeatureRDD(cnvs)
  }
}
