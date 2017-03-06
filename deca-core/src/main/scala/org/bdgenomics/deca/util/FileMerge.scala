package org.bdgenomics.deca.util

import java.io.{ InputStream, OutputStream }

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, FileUtil, Path }
import org.apache.hadoop.io.IOUtils
import org.bdgenomics.utils.misc.Logging

/**
 * Helper object to merge sharded files together adapted from ADAM
 */
object FileMerger extends Logging {

  /**
   * Merges together sharded files.
   *
   * @param fs The file system implementation to use.
   * @param outputPath The location to write the merged file at.
   * @param bodyPath The location where the sharded files have been written.
   * @param optHeaderPath Optionally, the location where a header file has
   *   been written.
   */
  private[deca] def mergeFiles(conf: Configuration,
                               fs: FileSystem,
                               outputPath: Path,
                               bodyPath: Path,
                               optHeaderPath: Option[Path] = None) {

    val os = fs.create(outputPath)

    def copy(inputPath: Path, los: OutputStream) {
      val lis = fs.open(inputPath)
      try {
        IOUtils.copyBytes(lis, los, conf, false)
      } finally {
        lis.close()
      }
    }

    // optionally copy the header
    optHeaderPath.foreach(headerPath => {
      log.info("Copying header file (%s)".format(headerPath))
      copy(headerPath, os)
    })

    // get a list of all of the files in the tail file
    val bodyFiles = fs.globStatus(new Path("%s/part-*".format(bodyPath)))
      .toSeq
      .map(_.getPath)
      .sortBy(_.getName)

    val numFiles = bodyFiles.length
    var filesCopied = 1
    bodyFiles.foreach(file => {
      log.info("Copying file %s, file %d of %d.".format(file.toString, filesCopied, numFiles))
      copy(file, os)
      filesCopied += 1
    })

    os.flush()
    os.close()

    // Cleanup temporary files
    optHeaderPath.foreach(headPath => fs.delete(headPath, true))
    fs.delete(bodyPath, true)
  }

}
