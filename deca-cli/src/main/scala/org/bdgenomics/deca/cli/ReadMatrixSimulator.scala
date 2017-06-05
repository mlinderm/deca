package org.bdgenomics.deca.cli

import org.apache.commons.math3.distribution.NormalDistribution
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.distributed.{ IndexedRow, IndexedRowMatrix }
import org.bdgenomics.deca.coverage.ReadDepthMatrix
import org.bdgenomics.deca.{ Deca }
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }

/**
 * Created by mlinderman on 6/5/17.
 */

object ReadMatrixSimulator extends BDGCommandCompanion {
  val commandName = "rdsim"
  val commandDescription = "Generate synthetic read depth data for performance testing"

  def apply(cmdLine: Array[String]) = {
    new ReadMatrixSimulator(Args4j[ReadMatrixSimulatorArgs](cmdLine))
  }
}

class ReadMatrixSimulatorArgs extends Args4jBase {
  @Args4jOption(required = true,
    name = "-I",
    usage = "The XHMM read depth matrix")
  var inputPath: String = null

  @Args4jOption(required = true,
    name = "-samples",
    usage = "Total samples to produce")
  var totalSamples: Int = 0

  @Args4jOption(required = true,
    name = "-o",
    usage = "Path to write simulated read depth matrix")
  var outputPath: String = null
}

class ReadMatrixSimulator(protected val args: ReadMatrixSimulatorArgs) extends BDGSparkCommand[ReadMatrixSimulatorArgs] {
  val companion = ReadMatrixSimulator

  def run(sc: SparkContext): Unit = {
    val matrix = Deca.readXHMMMatrix(args.inputPath)
    if (args.totalSamples > matrix.numSamples()) {

      val targetStats = matrix.depth.toRowMatrix().computeColumnSummaryStatistics()
      val targetDists = (0 until matrix.numTargets()).map(idx => {
        new NormalDistribution(targetStats.mean(idx), math.max(math.sqrt(targetStats.variance(idx)), 1e-8))
      })
      val broadcastTargetDists = sc.broadcast(targetDists)

      val synRows = sc.parallelize(matrix.numSamples() until args.totalSamples).map(rowIndex => {
        // An awkward approximation, since read depth can't be less than zero, however should be sufficient for
        // performance testing
        val d = new Array[Double](broadcastTargetDists.value.length)
        (0 until d.length).foreach(i => d(i) = math.max(broadcastTargetDists.value(i).sample(), 0.0))
        new IndexedRow(rowIndex, new DenseVector(d))
      })

      val synSamples = matrix.samples ++ (matrix.numSamples() until args.totalSamples).map(i => s"sample${i}")
      val synMatrix = new IndexedRowMatrix(sc.union(matrix.depth.rows, synRows))

      Deca.writeXHMMMatrix(ReadDepthMatrix(synMatrix, synSamples, matrix.targets), args.outputPath, label = "SynDepth", format = "%.2f")
    } else {
      log.error("Desired number of samples smaller than input set")
    }
  }
}
