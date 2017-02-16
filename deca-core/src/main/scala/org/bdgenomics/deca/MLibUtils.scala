package org.bdgenomics.deca

import breeze.linalg.{ DenseVector, SparseVector, DenseMatrix }

/**
 * Adapted from https://github.com/amplab/keystone/blob/master/src/main/scala/utils/MLlibUtils.scala
 */
object MLibUtils {

  /** Convert an MLlib vector to a Breeze dense vector */
  def mllibVectorToDenseBreeze(vector: org.apache.spark.mllib.linalg.Vector): DenseVector[Double] = {
    vector match {
      case dense: org.apache.spark.mllib.linalg.DenseVector => new DenseVector[Double](dense.values)
      case _ => new DenseVector[Double](vector.toArray)
    }
  }

  /** Convert an MLlib matrix to a Breeze dense matrix */
  def mllibMatrixToDenseBreeze(matrix: org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix): DenseMatrix[Double] = {
    // Breeze is column ordered so create transposed matrix from rows
    val result = new DenseMatrix[Double](matrix.numCols.toInt, matrix.numRows.toInt)
    matrix.rows.collect.foreach(row => {
      result(::, row.index.toInt) := mllibVectorToDenseBreeze(row.vector)
    })
    result.t
  }

  /** Convert a Breeze vector to an MLlib vector, maintaining underlying data structure (sparse vs dense) */
  def breezeVectorToMLlib(breezeVector: breeze.linalg.Vector[Double]): org.apache.spark.mllib.linalg.Vector = {
    breezeVector match {
      case v: DenseVector[Double] =>
        if (v.offset == 0 && v.stride == 1 && v.length == v.data.length) {
          new org.apache.spark.mllib.linalg.DenseVector(v.data)
        } else {
          new org.apache.spark.mllib.linalg.DenseVector(v.toArray) // Can't use underlying array directly, so make a new one
        }
      case v: SparseVector[Double] =>
        if (v.index.length == v.used) {
          new org.apache.spark.mllib.linalg.SparseVector(v.length, v.index, v.data)
        } else {
          new org.apache.spark.mllib.linalg.SparseVector(v.length, v.index.slice(0, v.used), v.data.slice(0, v.used))
        }
      case v: breeze.linalg.Vector[_] =>
        sys.error("Unsupported Breeze vector type: " + v.getClass.getName)
    }
  }
}
