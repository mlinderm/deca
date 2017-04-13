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

import breeze.linalg.DenseVector
import htsjdk.samtools.util.AsciiWriter
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{ DenseVector => SDV }
import org.apache.spark.mllib.linalg.distributed.{ IndexedRow, IndexedRowMatrix }
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.deca.util.FileMerger
import org.bdgenomics.deca.Timers._
import org.bdgenomics.deca.coverage.ReadDepthMatrix
import org.bdgenomics.utils.misc.Logging

object Deca extends Serializable with Logging {

  def readXHMMMatrix(filePath: String,
                     targetsToExclude: Array[ReferenceRegion] = Array(),
                     minTargetLength: Long = 0, maxTargetLength: Long = Long.MaxValue): ReadDepthMatrix = ReadXHMMMatrix.time {
    val sc = SparkContext.getOrCreate()
    val lines = sc.textFile(filePath)
    lines.cache()

    // Read header line with the targets into ReferenceRegions
    val targets = lines.first().split('\t').drop(1).map(target => {
      val fields = target.split(Array(':', '-'))
      new ReferenceRegion(fields(0), fields(1).toLong - 1, fields(2).toLong)
    })

    // Filter matrix based on target characteristics (including suppression list)
    val targetsToExcludeSet = targetsToExclude.toSet
    val toKeep = DenseVector.tabulate(targets.length) { (index) =>
      {
        val target = targets(index)
        val length = target.length
        (length >= minTargetLength) && (length <= maxTargetLength) && !targetsToExcludeSet.contains(target)
      }
    }

    // Return sample IDs as array
    val samples = lines.map(line => {
      line.substring(0, line.indexOf('\t'))
    }).collect().drop(1)

    // Read matrix body into IndexedRow RDD dropping first row and first column
    val toKeepBroadcast = SparkContext.getOrCreate().broadcast(toKeep)
    val matrix = new IndexedRowMatrix(lines.zipWithIndex.flatMap(lineWithIndex => {
      val (line, index) = lineWithIndex
      if (index == 0)
        None
      else {
        val myToKeep = toKeepBroadcast.value
        Some(IndexedRow(index - 1, new SDV(line.split('\t').drop(1).zipWithIndex.collect {
          case (depth, targetIndex) if myToKeep(targetIndex) => depth.toDouble
        })))
      }
    }))

    ReadDepthMatrix(matrix, samples, targets.zipWithIndex.collect { case (target, index) if toKeep(index) => target })
  }

  def writeXHMMMatrix(matrix: ReadDepthMatrix, filePath: String, label: String = "Matrix") = WriteXHMMMatrix.time {
    val sc = SparkContext.getOrCreate()

    val lines = matrix.depth.rows.map(row => {
      // TODO: Broadcast samples (or otherwise implement join)?
      val sample: String = matrix.samples(row.index.toInt)
      val valuesAsArray = row.vector match {
        case dense: org.apache.spark.mllib.linalg.DenseVector => dense.values
        case _ => row.vector.toArray
      }
      valuesAsArray.mkString(start = sample + "\t", sep = "\t", end = "")
    })

    val headPath = new Path("%s_head".format(filePath))
    val bodyPath = "%s_body".format(filePath)

    val conf = lines.context.hadoopConfiguration
    val fs = headPath.getFileSystem(conf)

    { // Write header file
      val headOutputStream = fs.create(headPath)
      val headerWriter = new AsciiWriter(headOutputStream)
      try {
        headerWriter.write(label)
        matrix.targets.foreach(target => {
          headerWriter.write('\t')
          headerWriter.write("%s:%d-%d".format(target.referenceName, target.start + 1, target.end))
        })
        headerWriter.write('\n')
      } finally {
        headerWriter.close()
        headOutputStream.close()
      }
    }

    // Write body files
    lines.saveAsTextFile(bodyPath.toString)

    // Merge files into file result (does not preserve ordering of sample lines)
    FileMerger.mergeFiles(conf, fs, new Path(filePath), new Path(bodyPath), Some(headPath))
  }

}
