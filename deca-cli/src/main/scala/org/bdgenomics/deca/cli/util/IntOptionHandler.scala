package org.bdgenomics.deca.cli.util

import org.kohsuke.args4j.spi.Setter
import org.kohsuke.args4j.{ CmdLineParser, OptionDef }

/**
 * Created by mlinderman on 5/15/17.
 */

// Adapted from: https://github.com/hammerlab/args4s/blob/master/src/main/scala/org/hammerlab/args4s/IntOptionHandler.scala

class IntOptionHandler(parser: CmdLineParser,
                       option: OptionDef,
                       setter: Setter[Option[Int]])
    extends Handler[Option[Int]](
      parser,
      option,
      setter,
      "INT",
      s => Some(s.toInt))
