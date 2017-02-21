/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.deca

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{ DenseVector => SDV }
import org.apache.spark.mllib.linalg.distributed.{ IndexedRow, IndexedRowMatrix }
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.feature.FeatureRDD
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD
import org.bdgenomics.utils.misc.Logging

object Deca extends Serializable with Logging {

  def readXHMMMatrix(filePath: String): (Array[String], Array[ReferenceRegion], IndexedRowMatrix) = {
    val sc = SparkContext.getOrCreate()
    val lines = sc.textFile(filePath)
    lines.cache()

    // Read header line with the targets into ReferenceRegions
    val targets = lines.first().split('\t').drop(1).map(target => {
      val fields = target.split(Array(':', '-'))
      new ReferenceRegion(fields(0), fields(1).toLong - 1, fields(2).toLong)
    })

    // Return sample IDs as array
    val samples = lines.map(line => {
      line.substring(0, line.indexOf('\t'))
    }).collect().drop(1)

    // Read matrix body into IndexedRow RDD dropping first row and first column
    val matrix = new IndexedRowMatrix(lines.zipWithIndex.flatMap(lineWithIndex => {
      val (line, index) = lineWithIndex
      if (index == 0)
        None
      else
        Some(IndexedRow(index - 1, new SDV(line.split('\t').drop(1).map(_.toDouble))))
    }))

    lines.unpersist()

    (samples, targets, matrix)
  }

  def callCnvs(reads: AlignmentRecordRDD): FeatureRDD = {
    ???
  }
}
