package org.bdgenomics.deca.cli.util

import org.kohsuke.args4j.{ CmdLineParser, OptionDef }
import org.kohsuke.args4j.spi.{ OptionHandler ⇒ Args4JOptionHandler, Parameters, Setter }

/**
 * Created by mlinderman on 5/15/17.
 */

// Adapted from: https://github.com/hammerlab/args4s/blob/master/src/main/scala/org/hammerlab/args4s/Handler.scala
class Handler[T](parser: CmdLineParser,
                 option: OptionDef,
                 setter: Setter[T],
                 defaultMetaVariable: String,
                 fn: String => T)
    extends Args4JOptionHandler[T](parser, option, setter) {
  override def getDefaultMetaVariable: String = defaultMetaVariable

  override def parseArguments(params: Parameters): Int = {
    setter.addValue(fn(params.getParameter(0)))
    1
  }
}
