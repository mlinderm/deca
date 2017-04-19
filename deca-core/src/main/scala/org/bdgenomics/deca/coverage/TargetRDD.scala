package org.bdgenomics.deca.coverage

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.serializers.FieldSerializer
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{ ReferenceRegion, ReferenceRegionSerializer, SequenceDictionary }
import org.bdgenomics.adam.rdd.GenomicRDD
import org.bdgenomics.formats.avro.Feature
import org.bdgenomics.utils.interval.array.{ IntervalArray, IntervalArraySerializer }

import scala.reflect.ClassTag

/**
 * Created by mlinderman on 3/27/17.
 */

case class Target(refRegion: ReferenceRegion, index: Long) {

}

private[deca] case class TargetArray(array: Array[(ReferenceRegion, Target)], maxIntervalWidth: Long) extends IntervalArray[ReferenceRegion, Target] {

  def duplicate(): IntervalArray[ReferenceRegion, Target] = {
    copy()
  }

  protected def replace(arr: Array[(ReferenceRegion, Target)], maxWidth: Long): IntervalArray[ReferenceRegion, Target] = {
    TargetArray(arr, maxWidth)
  }
}

private[deca] class TargetArraySerializer(kryo: Kryo) extends IntervalArraySerializer[ReferenceRegion, Target, TargetArray] {

  protected val kSerializer = new ReferenceRegionSerializer
  protected val tSerializer = new FieldSerializer[Target](kryo, classOf[Target])

  protected def builder(arr: Array[(ReferenceRegion, Target)], maxIntervalWidth: Long): TargetArray = {
    TargetArray(arr, maxIntervalWidth)
  }
}

object TargetRDD {

  /**
   * Creates a TargetRDD where no record groups or sequence info are attached.
   *
   * @param rdd RDD of fragments.
   * @return Returns a FragmentRDD with an empty record group dictionary and sequence dictionary.
   */
  def fromRdd(rdd: RDD[(Feature, Long)], sequences: SequenceDictionary = SequenceDictionary.empty): TargetRDD = {
    TargetRDD(rdd.map {
      case (feature, index) => new Target(ReferenceRegion.unstranded(feature), index)
    }, sequences)
  }
}

case class TargetRDD(rdd: RDD[Target], sequences: SequenceDictionary) extends GenomicRDD[Target, TargetRDD] {
  protected def buildTree(rdd: RDD[(ReferenceRegion, Target)])(
    implicit tTag: ClassTag[Target]): IntervalArray[ReferenceRegion, Target] = {
    IntervalArray(rdd, TargetArray.apply(_, _))
  }

  /**
   * Gets sequence of ReferenceRegions from Target element.
   * Since a target maps directly to a single genomic region, this method will always
   * return a Seq of exactly one ReferenceRegion.
   *
   * @param elem The Coverage to get an underlying region for.
   * @return Sequence of ReferenceRegions extracted from Coverage.
   */
  protected def getReferenceRegions(elem: Target): Seq[ReferenceRegion] = {
    Seq(elem.refRegion)
  }

  /**
   * @param newRdd The RDD to replace the underlying RDD with.
   * @return Returns a new CoverageRDD with the underlying RDD replaced.
   */
  protected def replaceRdd(newRdd: RDD[Target]): TargetRDD = {
    copy(rdd = newRdd)
  }
}

